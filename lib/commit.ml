type t = {
  entries : Entry.t list;
  seed_tag : Cstruct.t;
  last_tag : Cstruct.t;
  crc_just_entries : Optint.t;
}

let sizeof_crc = 4

let last_tag t = t.last_tag
let seed_tag t = t.seed_tag
let running_crc t = t.crc_just_entries
let entries t = t.entries

let create starting_xor_tag preceding_crc =
  { entries = [];
    last_tag = starting_xor_tag;
    seed_tag = starting_xor_tag;
    crc_just_entries = preceding_crc;
  }

let addv t entries =
  (* unfortunately we need to serialize all the entries in order to get the crc *)
  let new_last_tag, cs = Entry.to_cstructv ~starting_xor_tag:t.last_tag entries in
  let crc = Checkseum.Crc32.digest_bigstring (Cstruct.to_bigarray cs) 0 (Cstruct.length cs) t.crc_just_entries in
  let crc = Optint.((logand) crc @@ of_unsigned_int32 0xffffffffl) in

  { entries = t.entries @ entries;
    seed_tag = t.last_tag;
    last_tag = new_last_tag;
    crc_just_entries = crc;
  }

let commit_after t entries =
  let new_last_tag, cs = Entry.to_cstructv ~starting_xor_tag:t.last_tag entries in
  (* the crc for any entry that's after another one (any non-first entry on a block)
   * doesn't depend on the revision count, so its calculation is more straightforward *)
  (* initialize the crc with good ol' 0xffffffff *)
  let crc = Checkseum.Crc32.digest_bigstring Cstruct.(to_bigarray @@ Cstruct.of_string "\xff\xff\xff\xff") 0 4 (Checkseum.Crc32.default) in
  let crc = Checkseum.Crc32.digest_bigstring (Cstruct.to_bigarray cs) 0 (Cstruct.length cs) crc in
  let crc = Optint.((logand) crc @@ of_unsigned_int32 0xffffffffl) in
  { entries;
    seed_tag = t.last_tag;
    last_tag = new_last_tag;
    crc_just_entries = crc;
  }

let of_entries starting_xor_tag preceding_crc entries =
  let entries = List.filter (fun (entry : Entry.t) ->
      (* we don't want to include the CRC tag in the read-back entry list,'
       * since we calculate that on write in our own code. *)
      let (tag, _data) = entry in
      match fst @@ tag.Tag.type3 with
      | Tag.LFS_TYPE_CRC -> false
      | _ -> true
    ) entries in
  addv (create starting_xor_tag preceding_crc) entries

(** [into_cstruct cs t] writes [t] to [cs] starting at offset 0.
 * It returns the raw (i.e., not XOR'd with the tag before it) value
 * of the last tag of the commit as serialized (i.e., the CRC tag),
 * for use in writing any commits that may follow [t].
 * Unlike other modules the corresponding `to_cstruct` function is
 * not provided, because the caller is expected to be writing into
 * a larger buffer as part of a block write of a set of commits.
 * *)
let into_cstruct ~starting_offset ~program_block_size ~starting_xor_tag ~next_commit_valid cs t =
  let entries_length = Entry.lenv t.entries in
  let unpadded_length = starting_offset + entries_length + Tag.size + sizeof_crc in
  let overhang = unpadded_length mod program_block_size in
  let padding = program_block_size - overhang in
  let crc_tag_pointer, last_tag = Entry.into_cstructv ~starting_xor_tag cs t.entries in
  let crc_pointer = crc_tag_pointer + Tag.size in
  let crc_chunk = if next_commit_valid then 0x00 else 0x01 in
  let crc_tag = Tag.({
      valid = true;
      type3 = Tag.LFS_TYPE_CRC, crc_chunk;
      id = 0x3ff;
      length = sizeof_crc + padding;
    }) in

  (* TODO: pointer manipulation code smell here; find a nicer way to do this *)
  let tag_region = Cstruct.sub cs crc_tag_pointer Tag.size in
  let crc_region = Cstruct.sub cs crc_pointer sizeof_crc in
  let padding_region = Cstruct.sub cs (crc_pointer + sizeof_crc) padding in
  Tag.into_cstruct ~xor_tag_with:last_tag tag_region crc_tag;

  (* the crc in t is the crc of all the entries, so we can use that input to a crc calculation of the tag *)
  let crc_with_tag = Checkseum.Crc32.digest_bigstring (Cstruct.to_bigarray tag_region) 0 Tag.size t.crc_just_entries |> Optint.((logand) (of_unsigned_int32 0xffffffffl)) in

  let crc_with_tag = Optint.(lognot crc_with_tag |> (logand) (of_unsigned_int32 0xffffffffl)) in

  Cstruct.LE.set_uint32 crc_region 0
    (Optint.(to_unsigned_int crc_with_tag) |> Int32.of_int);

  (* set the padding bytes to an obvious value *)
  if padding <= 0 then () else Cstruct.memset padding_region 0xff;

  (* this needs to be a separate cstruct entirely,
   * because we'll overwrite the raw value in `cs` with its xor'd value *)
  let raw_tag = Tag.to_cstruct_raw crc_tag in
  (unpadded_length + padding - starting_offset, raw_tag)

let rec of_cstructv ~starting_offset ~program_block_size ~starting_xor_tag ~preceding_crc cs =
  (* we don't have a good way to know how many valid
   * entries there are (since we filter out the CRC tags) ,
   * so we have to keep trying for the whole block :/ *)
  let entries, last_tag, read = Entry.of_cstructv ~starting_xor_tag cs in
  match entries with
  | [] -> []
  | entries ->
    let overhang = starting_offset + read mod program_block_size in
    let padding = program_block_size - overhang in
    if read + padding >= Cstruct.length cs then
      (of_entries starting_xor_tag preceding_crc entries) :: []
    else begin
      let next_commit = Cstruct.shift cs (read + padding) in
      (* only the first commit ever has a nonzero starting offset, so all our recursive calls should set it to 0 *)
      let commit = of_entries starting_xor_tag preceding_crc entries in
      commit :: of_cstructv ~preceding_crc:commit.crc_just_entries ~starting_offset:0 ~starting_xor_tag:last_tag ~program_block_size next_commit
    end
