(* a block is, physically, a revision count and series of commits *)

type t = {
  revision_count : int32;
  commits : Commit.t list; (* the structure specified is more complex than this, but `list` will do for now *)
}

let empty = {
  revision_count = 0l;
  commits = [];
}

let commit ~program_block_size block entries =
  match block.commits with
  | [] ->
    let sizeof_revision_count = 4
    and sizeof_crc = 4 in

    let revision_cs = Cstruct.create sizeof_revision_count in
    Cstruct.LE.set_uint32 revision_cs 0 block.revision_count;

    let revision_count_crc = Checkseum.Crc32.digest_bigstring
      (Cstruct.to_bigarray revision_cs) 0 sizeof_crc Checkseum.Crc32.default in

    let unpadded_size = sizeof_revision_count + (Entry.lenv entries) +
                        Tag.size + sizeof_crc in
    let overhang = Int32.(rem (of_int unpadded_size) program_block_size) in
    let padding = match overhang with
      | 0l -> 0
      | n -> Int32.(sub program_block_size n |> to_int)
    in
    { block with commits = [{ entries;
                              preceding_crc = revision_count_crc;
                              padding;
                            }]
    }
  | _ -> block (* lol TODO *)

(* TODO: ugh, what if we need >1 block for the entries :( *)
let into_cstruct cs block =
  Cstruct.LE.set_uint32 cs 0 block.revision_count;
  let _after_last_crc, last_crc = List.fold_left (fun (pointer, _) commit ->
      let crc = Commit.into_cstruct (Cstruct.shift cs pointer) commit in
      (pointer + Commit.sizeof commit, crc)
    ) (4, Int32.zero) block.commits in
  last_crc

let to_cstruct ~block_size block =
  let cs = Cstruct.create (Int32.to_int block_size) in
  let crc = into_cstruct cs block in
  cs, crc
