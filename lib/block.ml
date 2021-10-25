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

(* TODO: ugh, what if we need >1 block for the entries :( *)
let into_cstruct cs block =
  let sizeof_crc = 4 in
  Cstruct.LE.set_uint32 cs 0 block.revision_count;
  let revision_count_crc = Checkseum.Crc32.(digest_bigstring
              (Cstruct.to_bigarray cs) 0 sizeof_crc default)
  in
  let _after_last_crc, last_crc = List.fold_left (fun (pointer, preceding_crc) commit ->
      let crc = Commit.into_cstruct ~preceding_crc (Cstruct.shift cs pointer) commit in
      (pointer + Commit.sizeof commit, crc)
    ) (4, revision_count_crc) block.commits in
  last_crc

let to_cstruct ~block_size block =
  let cs = Cstruct.create (Int32.to_int block_size) in
  let crc = into_cstruct cs block in
  cs, crc
