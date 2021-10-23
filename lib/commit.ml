type t = {
  entries : Entry.t list;
  preceding_crc : Optint.t;
  padding : int; (* make the commit aligned with the program block size *)
}

let sizeof_crc = 4

let sizeof t =
  (Entry.lenv t.entries) + Tag.size + sizeof_crc + t.padding

let into_cstruct cs t =
  let crc_tag_pointer, last_tag = Entry.into_cstructv t.entries cs in
  let crc_pointer = crc_tag_pointer + Tag.size in
  let crc_chunk = 0x01 in (* lowest bit represents what we expect the next valid bit to be *)
  let crc_tag = Tag.({
      valid = false;
      type3 = Tag.LFS_TYPE_CRC, crc_chunk;
      id = 0x3ff;
      length = sizeof_crc + t.padding;
    }) in

  (* TODO: pointer manipulation code smell here; find a nicer way to do this *)
  let tag_region = Cstruct.sub cs crc_tag_pointer Tag.size in
  let crc_region = Cstruct.sub cs crc_pointer sizeof_crc in
  let padding_region = Cstruct.sub cs (crc_pointer + sizeof_crc) t.padding in

  Tag.into_cstruct ~xor_tag_with:last_tag tag_region crc_tag;

  let crc_with_tag = Checkseum.Crc32.digest_bigstring (Cstruct.to_bigarray cs) 0 crc_pointer t.preceding_crc |> Optint.to_int32 |> Int32.lognot in
  Cstruct.LE.set_uint32 crc_region 0 crc_with_tag;
  (* set the padding bytes to an obvious value *)
  if t.padding <= 0 then () else Cstruct.memset padding_region 0xff
