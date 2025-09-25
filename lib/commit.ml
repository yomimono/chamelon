type t = {
  entries : Entry.t list;
  seed_tag : Cstruct.t;
  last_tag : Cstruct.t;
  start_crc : Optint.t; (* either the default CRC or the CRC of the revision count (for the first commit in a block) *)
}

let pp fmt t =
  Fmt.pf fmt "@[commit: ";
  Fmt.pf fmt "@[seed: %a@ @]" Cstruct.hexdump_pp t.seed_tag;
  Fmt.pf fmt "@[last: %a@ @]" Cstruct.hexdump_pp t.last_tag;
  Fmt.pf fmt "@[entries: %a@ @]" Fmt.(list Entry.pp) t.entries;
  Fmt.pf fmt "@]"

let sizeof_crc = 4

let last_tag t = t.last_tag
let seed_tag t = t.seed_tag
let entries t = t.entries

let create starting_xor_tag preceding_crc =
  { entries = [];
    last_tag = starting_xor_tag;
    seed_tag = starting_xor_tag;
    start_crc = preceding_crc;
  }

let addv t entries =
  (* unfortunately we need to serialize all the entries in order to get the crc *)
  let new_last_tag,_ = Entry.to_cstructv ~starting_xor_tag:t.last_tag entries in

  { entries = t.entries @ entries;
    seed_tag = t.last_tag;
    last_tag = new_last_tag;
    start_crc = t.start_crc;
  }

let commit_after {last_tag; _} entries =
  let seed_tag = last_tag in
  let new_last_tag, _cs = Entry.to_cstructv ~starting_xor_tag:seed_tag entries in
  (* the crc for any entry that's after another one (any non-first entry on a block)
   * doesn't depend on the revision count, so its calculation is more straightforward *)
  let start_crc = Checkseum.Crc32.default in
  (* we get the final result whether we do a lognot on this or, uh, not,
   * which indicates to me that maybe we're not actually using this value *)
  { entries;
    seed_tag;
    last_tag = new_last_tag;
    start_crc;
  }

let of_entries_filter_crc starting_xor_tag preceding_crc entries =
  let entries = List.filter (fun (entry : Entry.t) ->
      (* we don't want to include the CRC tag in the read-back entry list,
       * since we calculate that on write in our own code. *)
      let (tag, _data) = entry in
      match fst @@ tag.Tag.type3 with
      | Tag.LFS_TYPE_CRC -> false
      | _ -> true
    ) entries in
  addv (create starting_xor_tag preceding_crc) entries

(** [into_cstruct cs t] writes [t] to [cs] starting at offset 0.
 * It returns the raw (not XOR'd with the tag before it) value
 * of the last tag of the commit as serialized (i.e., the CRC tag),
 * for use in writing any commits that may follow [t].
 * Unlike other modules the corresponding `to_cstruct` function is
 * not provided, because the caller is expected to be writing into
 * a larger buffer as part of a block write of a set of commits.
 * *)
let into_cstruct ~filter_hardtail ~starting_offset ~program_block_size ~starting_xor_tag ~next_commit_valid cs t =
  (* we would like to be sure that we don't write any hardtail entries,
   * since the block level will be handling that *)
  let entries =
    if filter_hardtail then List.filter (fun (t, _d) -> not @@ Tag.is_hardtail t) t.entries
    else t.entries
  in
  let entries_length = Entry.lenv_less_hardtail entries in
  let unpadded_length = starting_offset + entries_length + Tag.size + sizeof_crc in
  let overhang = unpadded_length mod program_block_size in
  let padding = (program_block_size - overhang) mod program_block_size in
  
  (* for a lot of future calculation, we'll need to know where writing the (non-CRC) entries
   * into the buffer completed. *)
  let crc_tag_pointer, last_tag = Entry.into_cstructv ~starting_xor_tag cs entries in
  let crc_pointer = crc_tag_pointer + Tag.size in
  let crc_chunk = if next_commit_valid then 0x00 else 0x01 in
  let crc_tag = Tag.({
      valid = true;
      type3 = Tag.LFS_TYPE_CRC, crc_chunk;
      id = 0x3ff;
      length = sizeof_crc + padding;
    }) in

  let entry_region = Cstruct.sub cs 0 crc_tag_pointer in
  let tag_region = Cstruct.sub cs crc_tag_pointer Tag.size in
  let crc_region = Cstruct.sub cs crc_pointer sizeof_crc in
  let padding_region = Cstruct.sub cs (crc_pointer + sizeof_crc) padding in

  (* since the crc includes the tag for the crc itself, we need to write the tag before
   * we can calculate the crc value for the buffer *)
  Tag.into_cstruct ~xor_tag_with:last_tag tag_region crc_tag;

  let seed_crc = Checkseum.Crc32.digest_bigstring (Cstruct.to_bigarray entry_region) 0 crc_tag_pointer t.start_crc in

  (* the crc in t is the crc of all the entries, so we can use that input to a crc calculation of the tag *)
  let crc_with_tag = Checkseum.Crc32.digest_bigstring (Cstruct.to_bigarray tag_region) 0 Tag.size seed_crc
                   |> Optint.to_unsigned_int32
                   |> Int32.lognot
  in
  Cstruct.LE.set_uint32 crc_region 0 crc_with_tag;

  (* set the padding bytes to an obvious value *)
  if padding <= 0 then () else Cstruct.memset padding_region 0xff;

  let raw_tag = Tag.to_cstruct_raw crc_tag in
  (unpadded_length + padding - starting_offset, raw_tag)

let rec of_cstructv ~starting_offset:_ ~program_block_size ~starting_xor_tag ~preceding_crc cs =
  (* we don't have a good way to know how many valid
   * entries there are (since we filter out the CRC tags) ,
   * so we have to keep trying for the whole block :/ *)
  let entries, last_tag, read = Entry.of_cstructv ~starting_xor_tag cs in
  match entries with
  | [] -> []
  | entries ->
    (* `read` includes padding from CRC tags, so all reads after the first one should
     * be aligned with the program block size *)
    if read >= Cstruct.length cs then
      (of_entries_filter_crc starting_xor_tag preceding_crc entries) :: []
    else begin
      let next_commit = Cstruct.shift cs read in
      (* only the first commit ever has a nonzero starting offset, so all our recursive calls should set it to 0 *)
      let commit = of_entries_filter_crc starting_xor_tag preceding_crc entries in
      commit :: of_cstructv ~preceding_crc:Checkseum.Crc32.default ~starting_offset:0 ~starting_xor_tag:last_tag ~program_block_size next_commit
    end
