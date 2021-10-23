type t = {
  entries : Entry.t list;
  crc : Optint.t; (* CRC for the entry list - does not include the entry tag,
                     which we calculate on write *)
  padding : int; (* make the commit aligned with the program block size *)
}

let sizeof t =
  (Entry.lenv t.entries) + 4 + t.padding

let into_cstruct cs t =
  let crc_tag_pointer, last_tag = Entry.into_cstructv t.entries cs in
  let crc_pointer = crc_tag_pointer + Tag.size in
  let crc_chunk = 0x01 in (* lowest bit represents what we expect the next valid bit to be *)
  let crc_tag = Tag.({
      valid = false;
      type3 = Tag.LFS_TYPE_CRC, crc_chunk;
      id = 0x3ff;
      length = t.padding - 4;
    }) in
  (* TODO: pointer manipulation code smell here; find a nicer way to do this *)
  let tag_region = Cstruct.sub cs crc_tag_pointer Tag.size in
  Tag.into_cstruct ~xor_tag_with:last_tag tag_region crc_tag;
  let crc_with_tag = Checkseum.Crc32.digest_bigstring (Cstruct.to_bigarray tag_region) 0 Tag.size t.crc in
  Cstruct.LE.set_uint32 (Cstruct.shift cs crc_pointer) 0 (Optint.to_int32 crc_with_tag);
  (* set the padding bytes to 0x00 *)
  match crc_tag.length with
  | n when n <= 0 -> ()
  | n ->
    Cstruct.memset (Cstruct.sub cs (crc_pointer + 4) n) 0
