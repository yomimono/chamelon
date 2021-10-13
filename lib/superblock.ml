let magic = "littlefs"
let version = 0x00020004l (* major = 2, minor = 4 . Spec says minor 0 but the implementation says minor 4 *)
let name_length_max = 1024l
let file_size_max = Int32.max_int
let file_attribute_size_max = Int32.max_int

type superblock = {
  version : Cstruct.uint32;
  block_size : Cstruct.uint32;
  block_count : Cstruct.uint32;
  name_length_max : Cstruct.uint32;
  file_size_max : Cstruct.uint32;
  file_attribute_size_max : Cstruct.uint32;
}

[%%cstruct
  type superblock = {
    version : uint32_t;
    block_size : uint32_t;
    block_count : uint32_t;
    name_length_max : uint32_t;
    file_size_max : uint32_t;
    file_attribute_size_max : uint32_t;
  } [@@big_endian]]

let parse cs =
  {
    version = get_superblock_version cs;
    block_size = get_superblock_block_size cs;
    block_count = get_superblock_block_count cs;
    name_length_max = get_superblock_name_length_max cs;
    file_size_max = get_superblock_file_size_max cs;
    file_attribute_size_max = get_superblock_file_attribute_size_max cs;
  }

let print_to cs sb =
  set_superblock_version cs sb.version;
  set_superblock_block_size cs sb.block_size;
  set_superblock_block_count cs sb.block_count;
  set_superblock_name_length_max cs sb.name_length_max;
  set_superblock_file_size_max cs sb.file_size_max;
  set_superblock_file_attribute_size_max cs sb.file_attribute_size_max

let print sb =
  let cs = Cstruct.create sizeof_superblock in
  print_to cs sb;
  cs

let name =
  let open Tag in
  let cs = Cstruct.create @@ 4 + String.length magic in
  let name_tag = {
    valid = true;
    type3 = (Tag.LFS_TYPE_NAME, 0xff);
    id = 0;
    length = String.length magic;
  } in
  print_to cs name_tag;
  Cstruct.blit_from_string magic 0 cs 4 (String.length magic);
  cs

(* for now, maximum parameters are hardcoded *)
let inline_struct ~block_size ~block_count =
  let cs = Cstruct.create @@ 4 + sizeof_superblock in
  let inline_struct_tag = Tag.({
    valid = true;
    type3 = (Tag.LFS_TYPE_STRUCT, 0x01);
    id = 0;
    length = sizeof_superblock;
  })
  and superblock = {
    version;
    block_size;
    block_count;
    name_length_max;
    file_size_max;
    file_attribute_size_max;
  } in
  print_to (Cstruct.shift cs 4) superblock;
  inline_struct_tag, cs
