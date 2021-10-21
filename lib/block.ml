(* a block is, physically, a revision count and series of commits *)

let program_block_size = 32l

type t = {
  revision_count : int32;
  commits : Commit.t list; (* the structure specified is more complex than this, but `list` will do for now *)
}

let empty = {
  revision_count = 0l;
  commits = [];
}

let crc_entries start_crc entries = List.fold_left Entry.crc start_crc entries

let commit _block_size block entries =
  match block.commits with
  | [] ->
    let sizeof_revision_count = 4
    and sizeof_crc = 4 in

    let revision_cs = Cstruct.create sizeof_revision_count in
    Cstruct.LE.set_uint32 revision_cs 0 block.revision_count;

    let start_crc = Checkseum.Crc32.digest_bigstring
      (Cstruct.to_bigarray revision_cs) 0 sizeof_crc Checkseum.Crc32.default in

    let full_crc = crc_entries start_crc entries in

    let unpadded_size = sizeof_revision_count + (Entry.lenv entries) +
                        Tag.size + sizeof_crc in
    let overhang = Int32.(rem (of_int @@ unpadded_size) program_block_size) in
    let padding = match overhang with
      | 0l -> 0
      | n -> Int32.(sub program_block_size n |> to_int)
    in

    { block with commits = [{ entries;
                              crc = full_crc;
                              padding;
                            }]
    }
  | _ -> block (* lol TODO *)

(* TODO: ugh, what if we need >1 block for the entries :( *)
let into_cstruct cs block =
  Cstruct.LE.set_uint32 cs 0 block.revision_count;
  let pointer = List.fold_left (fun pointer commit ->
      Commit.into_cstruct (Cstruct.shift cs pointer) commit;
      pointer + Commit.sizeof commit
    ) 4 block.commits in
  (* the chunk here has only 1 meaningful bit: the least significant bit,
   * which represents "valid state" - the expected value
   * of the "valid" bit for the next commit. Since we're hardcoding
   * all the "valid" bits to 1 for the moment, we'll always want this to be 1. *)
  (* this is part of a much larger TODO, but may as well mark it... *)
  let chunk = 1 lsl 8 in
  let crc_tag = Tag.({
      valid = true;
      type3 = Tag.LFS_TYPE_CRC, chunk;
      id = 0x3ff;
      length = block.padding - 4;
    }) in
  Tag.into_cstruct ~xor_tag_with:Int32.zero (Cstruct.shift cs pointer) crc_tag;
  Cstruct.LE.set_uint32 (Cstruct.shift cs @@ pointer + 4) block.crc;
  ()

let to_cstruct ~block_size block =
  let cs = Cstruct.create (Int32.to_int block_size) in
  into_cstruct cs block;
  cs
