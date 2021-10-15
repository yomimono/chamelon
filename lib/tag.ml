(* tags are the only thing in littlefs stored in big-endian. *)
(* be careful when editing to remember this :) *)

type tag_repr = Cstruct.uint32

[%%cenum
type abstract_type =
  | LFS_TYPE_NAME [@id 0x0] (* associates IDs with file names and file types OR initializes them as files, directories, or superblocks *)
  | LFS_TYPE_STRUCT [@id 0x2] (* gives an id a directory structure (inline or CTZ) *)
  | LFS_TYPE_USERATTR [@id 0x3] (* 'user-defined', gross. "currently no standard user attributes" so we can just ignore them *)
  | LFS_TYPE_FILE [@id 0x4] (* create or delete file with a given ID depending on chunk info *)
  | LFS_TYPE_CRC [@id 0x5] (* CRC-32 for commits to the metadata block; polynomial of 0x04c11db7 initialized with 0xffffffff *)
  | LFS_TYPE_TAIL [@id 0x6] (* tail pointer for the metadata pair; hard or soft *)
  | LFS_TYPE_GSTATE [@id 0x7] (* global state entries; currently, only movestate *)
    (* data checksummed includes all metadata since previous CRC tag, including the CRC tag itself *)
[@@uint8_t]]

type chunk = int
type type3 = abstract_type * chunk

type t = {
  valid : bool;
  type3 : (abstract_type * Cstruct.uint8);
  id : int;
  length : int;
}

let not_associated = 0x3ff (* special value for "id" field *)
let deleted_tag = 0x3ff (* special value for "length" field *)

let size = 4 (* tags are always 32 bits, with internal
                numerical representations big-endian *)

let parse r =
  let valid = (1 = r lsr 31)
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

let to_int32 t =
  let bit_1 = if t.valid then 1 lsl 31 else 0
  and abstract_type = (abstract_type_to_int @@ fst t.type3) lsl 28
  and chunk = (snd t.type3) lsl 20
  and id = t.id lsl 10
  in
  bit_1 + abstract_type + chunk + id + t.length |> Int32.of_int

let into_cstruct ~xor_tag_with cs t =
  Cstruct.BE.set_uint32 cs 0 @@ Int32.logxor (to_int32 t) xor_tag_with

let to_cstruct ~xor_tag_with t =
  let cs = Cstruct.create 4 in
  into_cstruct ~xor_tag_with cs t;
  cs
