(* tags are the only thing in littlefs stored in big-endian. *)
(* be careful when editing to remember this :) *)

[%%cenum
type abstract_type =
  | LFS_TYPE_NAME [@id 0x0] (* associates IDs with file names and file types OR initializes them as files, directories, or superblocks *)
  | LFS_TYPE_STRUCT [@id 0x2] (* gives an id a structure (inline or CTZ) *)
  | LFS_TYPE_USERATTR [@id 0x3] (* 'user-defined', gross. "currently no standard user attributes" so we can just ignore them *)
  | LFS_TYPE_SPLICE [@id 0x4] (* create or delete file with a given ID depending on chunk info *)
  | LFS_TYPE_CRC [@id 0x5] (* CRC-32 for commits to the metadata block; polynomial of 0x04c11db7 initialized with 0xffffffff *)
  | LFS_TYPE_TAIL [@id 0x6] (* tail pointer for the metadata pair; hard or soft *)
  | LFS_TYPE_GSTATE [@id 0x7] (* global state entries; currently, only movestate *)
    (* data checksummed includes all metadata since previous CRC tag, including the CRC tag itself *)
[@@uint8_t]]

module Magic = struct
  let struct_dir = 0x00
  let struct_inline = 0x01
  let struct_ctz = 0x02

  let tail_soft = 0x00
  let tail_hard = 0x01

  let not_associated = 0x3ff (* special value for "id" field *)
  let deleted_tag = 0x3ff (* special value for "length" field *)

  let invalid_tags = [
    Cstruct.of_string "\x00\x00\x00\x00";
    Cstruct.of_string "\xff\xff\xff\xff";
  ]
end

type t = {
  valid : bool; (* "valid" does not have the meaning
                   the reader probably expects.
                   From SPEC.md in littlefs,
                   "Each tag contains a valid bit used to
                   indicate if the tag and containing
                   commit is valid. After XORing,
                   this bit should always be zero." *)
  type3 : (abstract_type * Cstruct.uint8);
  id : int;
  length : int; (* usually the length or 0, but 0x3ff means "deleted" *)
}

let size = 4 (* tags are always 32 bits, with internal
                numerical representations big-endian *)

let pp fmt tag =
  Format.fprintf fmt "@[id %d (%x),@ length %d (%x),@ valid %b,@ @[type is %x with chunk %x@]@]" tag.id tag.id
    tag.length tag.length (not tag.valid)
    (abstract_type_to_int (fst tag.type3))
    (snd tag.type3)

let xor ~into arg =
  (* it doesn't really need to be tags, it could be any 4-byte cstruct *)
  for i = 0 to 3 do
    let new_byte = (Cstruct.get_uint8 into i) lxor (Cstruct.get_uint8 arg i) in
    Cstruct.set_uint8 into i new_byte
  done

let is_file_struct tag =
  (fst tag.type3) = LFS_TYPE_STRUCT &&
  ((snd tag.type3) = Magic.struct_dir
   || snd tag.type3 = Magic.struct_ctz)

let is_hardtail {type3; _} =
  (fst type3) = LFS_TYPE_TAIL && (snd type3) = Magic.tail_hard

let has_links tag =
  is_file_struct tag || is_hardtail tag

(* this tag is LFS_TYPE_DELETED,
 * which is not the same as setting a *tag's* length value
 * to indicate that the tag itself has been deleted;
 * instead, it's to indicate a file is gone *)
let delete id =
  { valid = true;
    type3 = ( LFS_TYPE_SPLICE, 0xff );
    id;
    length = 0;
  }
    
let of_cstruct ~xor_tag_with cs =
  let tag_region = Cstruct.sub cs 0 size in
  if List.exists (Cstruct.equal tag_region) Magic.invalid_tags then Error (`Msg "invalid tag")
  else begin
    xor ~into:cs xor_tag_with;
    if List.exists (Cstruct.equal (Cstruct.sub cs 0 size)) Magic.invalid_tags then
      Error (`Msg "invalid tag")
    else begin
      let r32 = Cstruct.BE.get_uint32 cs 0 in
      let r = Int32.to_int r32 in
      let valid = Int.compare (Cstruct.get_uint8 cs 0) 128 < 0
      and abstract_type = (r lsr 28) land 0x7 |> int_to_abstract_type
      and chunk = (r lsr 20) land 0xff
      and id = (r lsr 10) land 0x3ff
      and length = r land 0x3ff
      in
      match abstract_type with
      | None -> Error (`Msg "invalid abstract type in metadata tag")
      | Some abstract_type ->
        let type3 = abstract_type, chunk in
        Ok {valid; type3; id; length}
    end
  end

let into_cstruct_raw cs t =
  let abstract_type, chunk = t.type3 in
  let id = t.id land 0x3ff
  and length = t.length land 0x3ff
  in
  (* most significant bit (31): valid or no? *)
  (* this is inverted from what we'd expect the value to be --
   * the spec isn't as explicit about this as I would be if I were writing something
   * where 1 was no and 0 was yes :/ *)
  let byte0 = if t.valid then 0x00 else 0x80 in
  (* bits 30, 29, and 28: abstract type *)
  let shifted_type = (0x7 land (abstract_type_to_int abstract_type)) lsl 4 in
  let byte0 = byte0 lor shifted_type in
  (* bits 27, 26, 25, 24 : first nibble of chunk *)
  let chunk_msb = (0xf0 land chunk) lsr 4 in
  let byte0 = byte0 lor chunk_msb in
  Cstruct.set_uint8 cs 0 byte0;

  (* bits 23, 22, 21, 20 : second nibble of chunk *)
  let byte1 = (0x0f land chunk) lsl 4 in
  (* bits 19, 18, 17, 16 : most significant 4 bits of id *)
  let id_4_msb = (0x3c0 land id) lsr 6 in
  let byte1 = byte1 lor id_4_msb in
  Cstruct.set_uint8 cs 1 byte1;

  (* bits 15, 14, 13, 12, 11, 10 : least significant 6 bits of id *)
  let byte2 = (0x03f land id) lsl 2 in
  (* bits 9, 8 : most significant 2 bits of length *)
  let length_2_msb = (0x300 land length) lsr 8 in
  let byte2 = byte2 lor length_2_msb in
  Cstruct.set_uint8 cs 2 byte2;

  (* bits 7, 6, 5, 4, 3, 2, 1, 0 : least significant 8 bits of length *)
  let byte3 = length land 0xff in
  Cstruct.set_uint8 cs 3 byte3

let to_cstruct_raw t =
  let cs = Cstruct.create 4 in
  into_cstruct_raw cs t;
  cs

let into_cstruct ~xor_tag_with cs t =
  into_cstruct_raw cs t;
  xor ~into:cs xor_tag_with

let to_cstruct ~xor_tag_with t =
  let cs = Cstruct.create 4 in
  into_cstruct ~xor_tag_with cs t;
  cs
