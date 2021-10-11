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

type tag = {
  valid : bool;
  type3 : (abstract_type * int);
  id : int;
  length : int;
}

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
