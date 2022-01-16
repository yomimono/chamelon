(* a block is, physically, a revision count and series of commits *)

module IdSet = Set.Make(Int)

type t = {
  revision_count : int;
  commits : Commit.t list;
}

type write_result = [ `Ok | `Split | `Split_emergency ]

let commits t = t.commits
let revision_count t = t.revision_count

let entries t = List.(flatten @@ map Commit.entries t.commits)

let crc_of_revision_count revision_count =
  let start_crc = Checkseum.Crc32.default in
  let cs = Cstruct.create 4 in
  let sizeof_crc = 4 in
  Cstruct.LE.set_uint32 cs 0 (Int32.of_int revision_count);
  let revision_count_crc = Checkseum.Crc32.(digest_bigstring
                             (Cstruct.to_bigarray cs) 0 sizeof_crc start_crc)
  in
  (* hey hey, ho ho, we don't want no overflow *)
  Optint.((logand) revision_count_crc @@ of_unsigned_int32 0xffffffffl)

let linked_blocks t =
  (* we call `compact` on the entry list because otherwise we'd incorrectly follow
   * deleted entries *)
  List.filter_map Entry.links @@ Entry.compact @@ entries t

let of_commits ~revision_count commits =
  (* we have to redo the crc for the first commit when the revision count changes :( *)
  (* we don't have to recalculate CRCs for any subsequent commits because the only data
   * dependency on a previous commit for later commits in a block
   * is on the last tag of the previous commit, which is the CRC tag.
   * The CRC tag's variable fields are its length and chunk value only,
   * and changes to the revision count are changes to a fixed-width field that
   * doesn't affect the chunk. *)
  match commits with
  | [] -> { commits; revision_count; }
  | commit :: l ->
    let crc = crc_of_revision_count revision_count in
    let new_commit = Commit.(of_entries_filter_crc (seed_tag commit) crc (entries commit)) in
    {commits = (new_commit :: l); revision_count}

let of_entries ~revision_count entries =
  let crc = crc_of_revision_count revision_count in
  let commit = Commit.of_entries_filter_crc (Cstruct.of_string "\xff\xff\xff\xff") crc entries in
  of_commits ~revision_count (commit::[])

let compact t =
  let revision_count = t.revision_count + 1 in
  let entries = List.map Commit.entries t.commits |> List.flatten |> Entry.compact in
  of_entries ~revision_count entries

let add_commit {revision_count; commits} entries =
  let revision_count = revision_count + 1 in
  match commits with
  | [] -> of_entries ~revision_count entries
  | l ->
    let last = List.(nth l ((length l) - 1)) in
    let commit = Commit.commit_after last entries in
    of_commits ~revision_count (l @ [commit])

let hardtail t =
  match List.filter_map (fun (tag, data) ->
      match Tag.is_hardtail tag with
      | false -> None
      | true -> match Entry.links (tag, data) with
        | Some Entry.Metadata block_pair -> Some block_pair
        | _ -> None
    ) (entries t) with
  | [] -> None
  | block_pair::_ -> Some block_pair

(* return variants `Split and `Ok are successful; `Split warns that the block
 * took up more than 1/2 of the block size and the metadata block
 * should be split into two pairs.
 *
 * `Split_emergency means the size of `block` exceeds the block size,
 * and the data *cannot* be written. A partial serialization remains in [cs] in
 * this case. *)
let into_cstruct ~program_block_size cs block =
  match block.commits with
  | [] -> (* this is a somewhat degenerate case, but
             not pathological enough to throw an error IMO.
             Since there's nothing to write, write nothing *)
    `Ok
  | _ ->
    Cstruct.LE.set_uint32 cs 0 (Int32.of_int block.revision_count);
    try
      let after_last_crc, _last_tag, _ =
        List.fold_left
          (fun (pointer, prev_commit_last_tag, starting_offset) commit ->
             (* it may be a bit surprising that we don't use `last_tag` from `commit` as the previous tag here.
              * as serialized, the last tag in the commit is the tag for the CRC, so we need to XOR with that
              * rather than the last non-CRC tag, which is what's represented in `commit.last_tag`. *)
             let this_commit_region = Cstruct.shift cs pointer in
             let (bytes_written, raw_crc_tag) = Commit.into_cstruct ~next_commit_valid:true
                 ~program_block_size ~starting_xor_tag:prev_commit_last_tag ~starting_offset
                 this_commit_region commit in
             (* only the first commit has nonzero offset; all subsequent ones have an offset of 0,
              * since each commit is padded to a multiple of the program block size. *)
             (pointer + bytes_written, raw_crc_tag, 0)
          ) (4, (Cstruct.of_string "\xff\xff\xff\xff"), 4) block.commits
      in
      if after_last_crc > ((Cstruct.length cs) / 2) then `Split else `Ok
    with Invalid_argument _ -> `Split_emergency

let ids t =
  let id_of_entry e = (fst e).Tag.id in
  let commit_ids c = List.map id_of_entry (Commit.entries c) in
  let block_ids = List.(flatten @@ map commit_ids t.commits) in
  IdSet.of_list block_ids

let split block next_blockpair =
  (add_commit block [Dir.hard_tail_at next_blockpair], of_entries ~revision_count:1 [])

let to_cstruct ~program_block_size ~block_size block =
  let cs = Cstruct.create block_size in
  let res = into_cstruct ~program_block_size cs block in
  cs, res

let of_cstruct ~program_block_size cs =
  if Cstruct.length cs <= Tag.size
  then Error (`Msg "block is too small to contain littlefs commits")
  else begin
    let revision_count = Cstruct.LE.get_uint32 cs 0 |> Int32.to_int in
    let revision_count_crc = crc_of_revision_count revision_count in
    let commit_list = Cstruct.shift cs 4 in
    let starting_xor_tag = Cstruct.of_string "\xff\xff\xff\xff" in
    (* the first commit is at an offset of 4, because the revision count is hanging out at the front of the block *)
    let commits = Commit.of_cstructv ~preceding_crc:revision_count_crc
        ~starting_offset:4 ~starting_xor_tag ~program_block_size
        commit_list
    in
    Ok {revision_count; commits}
  end
