type t = {
  entries : Entry.t list;
  padding : int; (* make the commit aligned with the program block size *)
}

let sizeof_crc = 4

let sizeof t =
  (Entry.lenv t.entries) + Tag.size + sizeof_crc + t.padding

let into_cstruct ~starting_xor_tag ~preceding_crc cs t =
  let crc_tag_pointer, last_tag = Entry.into_cstructv ~starting_xor_tag cs t.entries in
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
  let tag = Cstruct.(LE.get_uint32 tag_region 0) in

  let crc_with_tag = Checkseum.Crc32.digest_bigstring (Cstruct.to_bigarray cs) 0 crc_pointer preceding_crc in
  Cstruct.LE.set_uint32 crc_region 0 (Optint.(to_int32 crc_with_tag |> Int32.lognot));
  (* set the padding bytes to an obvious value *)
  if t.padding <= 0 then () else Cstruct.memset padding_region 0xff;
  (crc_with_tag, tag)

