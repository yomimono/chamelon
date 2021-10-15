(* a block is, physically, a revision count and series of commits *)

type commit = {
  entries : Entry.t list;
  crc : Optint.t;
  padding : int; (* make the commit 32-bit word-aligned *)
}

type block = {
  revision_count : int32;
  commits : commit list; (* the structure specified is more complex than this, but `list` will do for now *)
}

let empty = {
  revision_count = 0l;
  commits = [];
}

let crc_entries start_crc entries =
  (* TODO: it's unclear to my tired mind whether we're supposed
   * to CRC the whole entry, or just the tag *)
    List.fold_left (fun crc entry ->
      Checkseum.Crc32.digest_bigstring (Cstruct.to_bigarray entry) 0
        (Cstruct.length entry) crc
    ) start_crc entries

let commit block_size block entries =
  match block.commits with
  | [] ->
    let sizeof_revision_count = 4 in
    let sizeof_crc = 4 in

    let revision_count = Cstruct.create sizeof_crc in
    Cstruct.LE.set_uint32 revision_count 0 block.revision_count;
    let start_crc = Checkseum.Crc32.digest_bigstring
      (Cstruct.to_bigarray revision_count) 0 sizeof_crc Checkseum.Crc32.default in

    let full_crc = crc_entries start_crc entries in

    let unpadded_size = sizeof_revision_count + Cstruct.lenv entries + sizeof_crc in
    let padding = match unpadded_size mod block_size with
      | 0 -> 0
      | n -> block_size - n
    in
    
    {block with commits =
                  [{ entries;
                     crc = full_crc;
                     padding;
                   }]
    }
  | _ -> block (* lol TODO *)

let into_cstruct cs block =
  Cstruct.LE.set_uint32 cs 0 block.revision_count;
  let _copied, _left = Cstruct.fillv ~src:block.commits ~dst:(Cstruct.shift cs 4) in
  ()

let to_cstruct ~block_size block =
  let cs = Cstruct.create block_size in
  into_cstruct cs block;
  cs
