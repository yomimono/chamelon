type t = {
  entries : Entry.t list;
  padding : int; (* make the commit aligned with the program block size *)
}

let sizeof_crc = 4

let sizeof t =
  (Entry.lenv t.entries) + Tag.size + sizeof_crc + t.padding

(** [into_cstruct cs t] writes [t] to [cs] starting at offset 0.
 * It returns the raw (i.e., not XOR'd with the tag before it) value
 * of the last tag of the commit, for use in writing later commits.
 * Unlike other modules the corresponding `to_cstruct` function is
 * not provided, because the caller is expected to be writing into
 * a larger buffer as part of a block write of a set of commits.
 * *)
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

  Tag.into_cstruct ~xor_tag_with:last_tag tag_region crc_tag;

  let crc_with_tag = Checkseum.Crc32.digest_bigstring (Cstruct.to_bigarray cs) 0 crc_pointer preceding_crc |> Optint.((logand) (of_unsigned_int32 0xffffffffl)) in
  let crc_with_tag = Optint.lognot crc_with_tag in
  Cstruct.LE.set_uint32 crc_region 0 (Optint.(to_int32 crc_with_tag));
  (* set the padding bytes to an obvious value *)
  if t.padding <= 0 then () else Cstruct.memset padding_region 0xff;
  raw_tag
