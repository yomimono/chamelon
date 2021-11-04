(* a block is, physically, a revision count and series of commits *)

type t = {
  revision_count : int32;
  commits : Commit.t list; (* the structure specified is more complex than this, but `list` will do for now *)
}

let empty = {
  revision_count = 0l;
  commits = [];
}

let last l = List.(nth l @@ (length l) - 1)

let get_padding is_first program_block_size entries =
  let sizeof_revision_count = 4
  and sizeof_crc = 4 in

  let unpadded_size = 
    (if is_first then sizeof_revision_count else 0)
                                                + (Entry.lenv entries)
                                                + Tag.size
                                                + sizeof_crc
  in
  let overhang = Int32.(rem (of_int unpadded_size) program_block_size) in
  match overhang with
  | 0l -> 0
  | n -> Int32.(sub program_block_size n |> to_int)

let crc_of_revision_count revision_count =
  let start_crc = Checkseum.Crc32.default in
  let cs = Cstruct.create 4 in
  let sizeof_crc = 4 in
  Cstruct.LE.set_uint32 cs 0 revision_count;
  let revision_count_crc = Checkseum.Crc32.(digest_bigstring
                               (Cstruct.to_bigarray cs) 0 sizeof_crc start_crc)
  in
  (* hey hey, ho ho, we don't want no overflow *)
  Optint.((logand) revision_count_crc (of_int32 0xffffffffl))

let commit block entries =
  match block.commits with
  | [] ->
    let revision_count_crc = crc_of_revision_count block.revision_count in
    let starting_xor_tag = Cstruct.of_string "\xff\xff\xff\xff" in
    let commit = Commit.create starting_xor_tag revision_count_crc in
    let commit = Commit.addv commit entries in
    { commits = [commit]; revision_count = Int32.add block.revision_count 1l};
  | l ->
    let prev = last l in
    let commit = Commit.(create (last_tag prev) (running_crc prev)) in
    let commit = Commit.addv commit entries in
    {commits = List.rev @@ commit :: List.rev l;
     revision_count = Int32.add block.revision_count 1l;}

(* TODO: ugh, what if we need >1 block for the entries :( *)
let into_cstruct ~program_block_size cs block =
  match block.commits with
  | [] -> (* this is a somewhat degenerate case, but
             not pathological enough to throw an error IMO.
             Since there's nothing to write, write nothing *)
    ()
  | _ ->
    Cstruct.LE.set_uint32 cs 0 block.revision_count;
    let _after_last_crc, _last_tag, _last_crc = List.fold_left
        (fun (pointer, prev_commit_last_tag, starting_offset) commit ->
           (* it may be a bit surprising that we don't use `last_tag` from `commit` as the previous tag here.
            * as serialized, the last tag in the commit is the tag for the CRC, so we need to XOR with that
            * rather than the last non-CRC tag, which is what's represented in `commit.last_tag`. *)
           let this_commit_region = Cstruct.shift cs pointer in
           let (bytes_written, raw_crc_tag) = Commit.into_cstruct ~next_commit_valid:true
               ~program_block_size ~starting_xor_tag:prev_commit_last_tag ~starting_offset
               this_commit_region commit in
           (* we never want to pass a CRC *forward* into the next commit. Similarly,
            * only the first commit has an offset of 8; all subsequent ones have an offset of 0,
            * since each commit is padded to a multiple of the program block size. *)
           (pointer + bytes_written, raw_crc_tag, 0)
        ) (4, (Cstruct.of_string "\xff\xff\xff\xff"), 4) block.commits in
    ()

let to_cstruct ~program_block_size ~block_size block =
  let cs = Cstruct.create block_size in
  let () = into_cstruct ~program_block_size cs block in
  cs

let of_cstruct ~program_block_size block =
  let revision_count = Cstruct.LE.get_uint32 block 0 in
  let revision_count_crc = crc_of_revision_count revision_count in
  let commit_list = Cstruct.shift block 4 in
  let starting_xor_tag = Cstruct.of_string "\xff\xff\xff\xff" in
  (* the first commit is at an offset of 4, because the revision count is hanging out at the front of the block *)
  let commits = Commit.of_cstructv ~preceding_crc:revision_count_crc
      ~starting_offset:4 ~starting_xor_tag ~program_block_size
      commit_list
  in
  {revision_count; commits}
