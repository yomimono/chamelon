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
  let revision_count = t.revision_count in
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
      if after_last_crc > (Cstruct.length cs / 2) then `Split else `Ok
    with Invalid_argument _ -> `Split_emergency

let ids t =
  let id_of_entry e = (fst e).Tag.id in
  let commit_ids c = List.map id_of_entry (Commit.entries c) in
  let block_ids = List.(flatten @@ map commit_ids t.commits) in
  IdSet.of_list block_ids

let split block next_blockpair =
  match block.commits with
  | [] -> (* trivial case - just add the hardtail to block,
             since we have no commits to move *)
    (* we'll overwhelmingly more often end up in this case, because
     * in most situations we'll compact before we split,
     * meaning that there will be only 1 commit on the block *)
    let block = add_commit block [Dir.hard_tail_at next_blockpair] in
    (block, of_entries ~revision_count:0 [])
  | _ ->
    let entries = entries block |> Entry.compact in
    let length = List.length entries in
    let half = (List.length entries) / 2 in
    let first_half = List.init half (fun i -> List.nth entries i) in
    let second_half = List.init (length - half) (fun i -> List.nth entries (half + i)) in
    let new_block = of_entries ~revision_count:0 second_half in
    let old_block = of_entries ~revision_count:(revision_count block) first_half in
    let old_block = add_commit old_block [Dir.hard_tail_at next_blockpair] in
    (old_block, new_block)

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
