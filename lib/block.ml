(* a block is, physically, a revision count and series of commits *)

type t = {
  revision_count : int32;
  commits : Commit.t list; (* the structure specified is more complex than this, but `list` will do for now *)
}

let empty = {
  revision_count = 0l;
  commits = [];
}

let crc_entries start_crc entries =
    List.fold_left Entry.crc start_crc entries

let commit block_size block entries =
  match block.commits with
  | [] ->
    let sizeof_revision_count = 4
    and sizeof_crc = 4 in

    let revision_cs = Cstruct.create sizeof_revision_count in
    Cstruct.LE.set_uint32 revision_cs 0 block.revision_count;

    let start_crc = Checkseum.Crc32.digest_bigstring
      (Cstruct.to_bigarray revision_cs) 0 sizeof_crc Checkseum.Crc32.default in

    let full_crc = crc_entries start_crc entries in

    let unpadded_size = sizeof_revision_count + (Entry.lenv entries) + sizeof_crc in
    let padding = match Int32.(rem (of_int unpadded_size) block_size) with
      | 0l -> 0
      | n -> Int32.(sub block_size n |> to_int)
    in

    { block with commits = [{ entries;
                              crc = full_crc;
                              padding;
                            }]
    }
  | _ -> block (* lol TODO *)

let into_cstruct cs block =
  Cstruct.LE.set_uint32 cs 0 block.revision_count;
  let _ = List.fold_left (fun pointer commit ->
      Commit.into_cstruct (Cstruct.shift cs pointer) commit;
      pointer + Commit.sizeof commit
    ) 4 block.commits in
  ()

let to_cstruct ~block_size block =
  let cs = Cstruct.create (Int32.to_int block_size) in
  into_cstruct cs block;
  cs
