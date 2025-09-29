(* a block is, physically, a revision count and series of commits *)
(* it can also have a hardtail, pointing at another blockpair where
 * the directory continues *)

module IdSet = Set.Make(Int)

type t = {
  revision_count : int32;
  commits : Commit.t list;
  hardtail : Entry.t option;
}

type write_result = [ `Ok | `Split | `Split_emergency | `Unwriteable ]

let commits t = t.commits
let revision_count t = t.revision_count

let entries t = List.(flatten @@ map Commit.entries t.commits)

let pp fmt t =
  Fmt.pf fmt "@[block:@ ";
  Fmt.pf fmt "@[revision count: %ld@]@ " t.revision_count;
  Fmt.pf fmt "@[hardtail: %a@]@ " Fmt.(option Entry.pp) t.hardtail;
  Fmt.pf fmt "@[commits: %a@]@ " Fmt.(list Commit.pp) t.commits;
  Fmt.pf fmt "@]"

let latest a b =
  (* if there is a gap of more than (MAXINT / 2), assume an overflow
   * has occurred, and the "smaller" number is actually more recent *)
  let max_diff = Int32.(shift_right max_int 1) in
  let a_minus_b = Int32.(sub a.revision_count b.revision_count) in
  let b_minus_a = Int32.(sub b.revision_count a.revision_count) in
  if a.revision_count < b.revision_count && b_minus_a < max_diff then b
  else if a.revision_count > b.revision_count && a_minus_b > max_diff then b
  else a (* we will arbitrarily prefer a when a.revision_count = b.revision_count *)

let crc_of_revision_count revision_count =
  let start_crc = Checkseum.Crc32.default in
  let cs = Cstruct.create 4 in
  let sizeof_crc = 4 in
  Cstruct.LE.set_uint32 cs 0 revision_count;
  Checkseum.Crc32.(digest_bigstring
                     (Cstruct.to_bigarray cs) 0 sizeof_crc start_crc)

let linked_blocks t =
  (* we call `compact` on the entry list because otherwise we'd incorrectly follow
   * deleted entries *)
  List.filter_map Entry.links @@ Entry.compact @@ entries t

let of_commits ~hardtail ~revision_count commits =
  (* we have to redo the crc for the first commit when the revision count changes :( *)
  (* we don't have to recalculate CRCs for any subsequent commits because the only data
   * dependency on a previous commit for later commits in a block
   * is on the last tag of the previous commit, which is the CRC tag.
   * The CRC tag's variable fields are its length and chunk value only,
   * and changes to the revision count are changes to a fixed-width field that
   * doesn't affect the chunk. *)
  match commits with
  | [] -> { commits; revision_count; hardtail}
  | commit :: l ->
    let crc = crc_of_revision_count revision_count in
    let new_commit = Commit.(of_entries_filter_crc (seed_tag commit) crc (entries commit)) in
    {commits = (new_commit :: l); revision_count; hardtail}

let of_entries ~revision_count entries =
  let crc = crc_of_revision_count revision_count in
  let commit = Commit.of_entries_filter_crc (Cstruct.of_string "\xff\xff\xff\xff") crc entries in
  of_commits ~hardtail:None ~revision_count (commit::[])

let compact t =
  let revision_count = Int32.(add t.revision_count one) in
  let entries = List.map Commit.entries t.commits |> List.flatten |> Entry.compact in
  of_entries ~revision_count entries

let add_commit {revision_count; commits; hardtail} entries =
  let revision_count = Int32.(add revision_count one) in
  match commits with
  | [] -> of_entries ~revision_count entries
  | l ->
    let last = List.(nth l ((length l) - 1)) in
    let commit = Commit.commit_after last entries in
    of_commits ~hardtail ~revision_count (l @ [commit])

let hardtail t =
  match t.hardtail with
  | None -> None
  | Some e -> Dir.hard_tail_links e

(* return variants `Split and `Ok are successful; `Split warns that the block
 * took up more than 1/2 of the block size and the metadata block
 * should be split into two pairs.
 *
 * `Split_emergency means the size of `block` exceeds the block size,
 * and the data *cannot* be written. A partial serialization remains in [cs] in
 * this case. *)
let into_cstruct ~program_block_size cs block =
  (* the hardtail is handled specially since its presence alters our next_commit_valid
   * and all entries need to be before it. *)
  let write_hardtail ~after ~starting_xor_tag ~starting_offset t cs =
    match t.hardtail with
    | None -> (0, starting_xor_tag)
    | Some entry ->
      let commit = Commit.commit_after after [entry] in
      Commit.into_cstruct ~filter_hardtail:false ~starting_offset ~program_block_size ~starting_xor_tag
        ~next_commit_valid:false cs commit
  in
  (* if there's nothing to write, just return *)
  match block.commits, block.hardtail with
  | [], None -> `Ok
  | commits, _ ->
    Cstruct.LE.set_uint32 cs 0 block.revision_count;
    try
      let after_last_crc, starting_xor_tag, starting_offset =
        List.fold_left
          (fun (pointer, prev_commit_last_tag, starting_offset) commit ->
             (* it may be a bit surprising that we don't use `last_tag` from `commit` as the previous tag here.
              * as serialized, the last tag in the commit is the tag for the CRC, so we need to XOR with that
              * rather than the last non-CRC tag, which is what's represented in `commit.last_tag`. *)
             let this_commit_region = Cstruct.shift cs pointer in
             let (bytes_written, raw_crc_tag) = Commit.into_cstruct ~filter_hardtail:true ~next_commit_valid:true
                 ~program_block_size ~starting_xor_tag:prev_commit_last_tag ~starting_offset
                 this_commit_region commit in
             (* only the first commit has nonzero offset; all subsequent ones have an offset of 0,
              * since each commit is padded to a multiple of the program block size. *)
             (pointer + bytes_written, raw_crc_tag, 0)
          ) (4, (Cstruct.of_string "\xff\xff\xff\xff"), 4) block.commits
      in
      let hardtail_region = Cstruct.shift cs after_last_crc in
      let hardtail_bytes, _raw_crc =
        write_hardtail ~after:List.(hd @@ rev commits) ~starting_xor_tag ~starting_offset block hardtail_region
      in
      if (after_last_crc + hardtail_bytes) > Cstruct.length cs then `Unwriteable
      else if (after_last_crc + hardtail_bytes) > ((Cstruct.length cs) / 2) then `Split
      else `Ok
    with
    | Invalid_argument _ -> `Unwriteable

let ids t =
  let id_of_entry e = (fst e).Tag.id in
  let commit_ids c = List.map id_of_entry (Commit.entries c) in
  let block_ids = List.(flatten @@ map commit_ids t.commits) in
  IdSet.of_list block_ids

let split block next_blockpair =
  let entry = Dir.hard_tail_at next_blockpair in
  {block with hardtail = Some entry},
  of_entries ~revision_count:1l []

let to_cstruct ~program_block_size ~block_size block =
  let cs = Cstruct.create block_size in
  let res = into_cstruct ~program_block_size cs block in
  cs, res

let of_cstruct ~program_block_size cs =
  if Cstruct.length cs <= Tag.size
  then Error (`Msg "block is too small to contain littlefs commits")
  else begin
    let revision_count = Cstruct.LE.get_uint32 cs 0 in
    let revision_count_crc = crc_of_revision_count revision_count in
    let commit_list = Cstruct.shift cs 4 in
    let starting_xor_tag = Cstruct.of_string "\xff\xff\xff\xff" in
    (* the first commit is at an offset of 4, because the revision count is hanging out at the front of the block *)
    let commits = Commit.of_cstructv ~preceding_crc:revision_count_crc
        ~starting_offset:4 ~starting_xor_tag ~program_block_size
        commit_list
    in
    let entries = List.(flatten @@ map Commit.entries commits) in
    let hardtail = List.find_opt (fun (t, _d) -> Tag.is_hardtail t) entries in
    Ok {revision_count; commits; hardtail}
  end
