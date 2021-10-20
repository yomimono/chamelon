type t = {
  entries : Entry.t list;
  crc : Optint.t;
  padding : int; (* make the commit aligned with the program block size *)
}

let sizeof t =
  (Entry.lenv t.entries) + 4 + t.padding

let into_cstruct cs t =
  let crc_tag_pointer, _last_tag = Entry.into_cstructv t.entries cs in
  let crc_pointer = crc_tag_pointer + Tag.size in
  let crc_chunk = 1 lsl 8 in
  let crc_tag = Tag.({
      valid = true;
      type3 = Tag.LFS_TYPE_CRC, crc_chunk;
      id = 0x3ff;
      length = t.padding - 4;
    }) in
  (* TODO: pointer manipulation code smell here; find a nicer way to do this *)
  Tag.into_cstruct ~xor_tag_with:(Optint.to_int32 t.crc) (Cstruct.shift cs crc_tag_pointer) crc_tag;
  Cstruct.LE.set_uint32 (Cstruct.shift cs crc_pointer) 0 (Optint.to_int32 t.crc);
  match crc_tag.length with
  | n when n <= 0 -> ()
  | n ->
    Cstruct.memset (Cstruct.sub cs (crc_pointer + 4) n) 0
