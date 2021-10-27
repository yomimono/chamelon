type t = {
  entries : Entry.t list;
  padding : int; (* make the commit aligned with the program block size *)
}

let sizeof_crc = 4

let sizeof t =
  (Entry.lenv t.entries) + Tag.size + sizeof_crc + t.padding

let into_cstruct ~next_commit_valid ~starting_xor_tag ~preceding_crc cs t =
  let crc_tag_pointer, last_tag = Entry.into_cstructv ~starting_xor_tag cs t.entries in
  let crc_pointer = crc_tag_pointer + Tag.size in
  let crc_chunk = if next_commit_valid then 0x01 else 0x00 in
  let crc_tag = Tag.({
      valid = true;
      type3 = Tag.LFS_TYPE_CRC, crc_chunk;
      id = 0x3ff;
      length = sizeof_crc + t.padding;
    }) in

  (* TODO: pointer manipulation code smell here; find a nicer way to do this *)
  let tag_region = Cstruct.sub cs crc_tag_pointer Tag.size in
  let crc_region = Cstruct.sub cs crc_pointer sizeof_crc in
  let padding_region = Cstruct.sub cs (crc_pointer + sizeof_crc) t.padding in
  let raw_tag = Tag.to_cstruct_raw crc_tag in
  Format.printf "raw CRC tag for this commit: %a\n%!" Cstruct.hexdump_pp raw_tag;

  Tag.into_cstruct ~xor_tag_with:last_tag tag_region crc_tag;

  let crc_with_tag = Checkseum.Crc32.digest_bigstring (Cstruct.to_bigarray cs) 0 crc_pointer preceding_crc in
  Cstruct.LE.set_uint32 crc_region 0 (Optint.(to_unsigned_int32 crc_with_tag |> Int32.lognot));
  (* set the padding bytes to an obvious value *)
  if t.padding <= 0 then () else Cstruct.memset padding_region 0xff;
  (crc_with_tag, raw_tag)

