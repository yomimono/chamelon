(* a block is, physically, a revision count and series of commits *)

type t = {
  revision_count : int32;
  commits : Commit.t list; (* the structure specified is more complex than this, but `list` will do for now *)
}

let empty = {
  revision_count = 0l;
  commits = [];
}

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

let commit ~program_block_size block entries =
  let () = () in
  match block.commits with
  | [] ->
    let padding = get_padding true program_block_size entries in
    { block with commits = [{ entries;
                              padding;
                            }]
    }
  | l ->
    let padding = get_padding false program_block_size entries in
    let commit = Commit.{entries; padding} in
    let commits = List.(rev @@ (commit :: (rev l))) in
    {block with commits; }

let pp_hexdump_le fmt n =
  let uint32 = Optint.to_int32 n in
  let b = Cstruct.create 4 in
  Cstruct.LE.set_uint32 b 0 uint32;
  Format.fprintf fmt "%a" Cstruct.hexdump_pp b

(* TODO: ugh, what if we need >1 block for the entries :( *)
let into_cstruct cs block =
  let sizeof_crc = 4 in
  let ffffffff = Cstruct.of_string "\xff\xff\xff\xff" in
  let start_crc = Checkseum.Crc32.default in
  Cstruct.LE.set_uint32 cs 0 block.revision_count;
  let revision_count_crc = Checkseum.Crc32.(digest_bigstring
              (Cstruct.to_bigarray cs) 0 sizeof_crc start_crc)
  in
  let revision_count_crc = Optint.lognot revision_count_crc in
  Format.printf "preceding CRC: %a. revision count CRC tag for this commit: %a\n%!"
    pp_hexdump_le start_crc pp_hexdump_le revision_count_crc;
  let _after_last_crc, _last_tag, _last_crc = List.fold_left
      (fun (pointer, starting_xor_tag, preceding_crc) commit ->
         let crc, last_tag_of_commit =
           Commit.into_cstruct ~next_commit_valid:true ~starting_xor_tag ~preceding_crc
             (Cstruct.shift cs pointer) commit
         in
         (pointer + Commit.sizeof commit, last_tag_of_commit, crc)
      ) (4, ffffffff, revision_count_crc) block.commits in
  ()

let to_cstruct ~block_size block =
  let cs = Cstruct.create (Int32.to_int block_size) in
  let crc = into_cstruct cs block in
  cs, crc
