let magic = "littlefs"
let version = (2, 0) (* major = 2, minor = 0 . Spec says minor 0 but the implementation says minor 4 as of this writing :/ *)
let name_length_max = 1024l
let file_size_max = Int32.max_int
let file_attribute_size_max = Int32.max_int

type superblock = {
  version_major : Cstruct.uint16;
  version_minor : Cstruct.uint16;
  block_size : Cstruct.uint32;
  block_count : Cstruct.uint32;
  name_length_max : Cstruct.uint32;
  file_size_max : Cstruct.uint32;
  file_attribute_size_max : Cstruct.uint32;
}

[%%cstruct
  type superblock = {
    version_major : uint16_t;
    version_minor : uint16_t;
    block_size : uint32_t;
    block_count : uint32_t;
    name_length_max : uint32_t;
    file_size_max : uint32_t;
    file_attribute_size_max : uint32_t;
  } [@@big_endian]]

let parse cs =
  {
    version_major = get_superblock_version_major cs;
    version_minor = get_superblock_version_minor cs;
    block_size = get_superblock_block_size cs;
    block_count = get_superblock_block_count cs;
    name_length_max = get_superblock_name_length_max cs;
    file_size_max = get_superblock_file_size_max cs;
    file_attribute_size_max = get_superblock_file_attribute_size_max cs;
  }

let into_cstruct cs sb =
  set_superblock_version_major cs sb.version_major;
  set_superblock_version_minor cs sb.version_minor;
  set_superblock_block_size cs sb.block_size;
  set_superblock_block_count cs sb.block_count;
  set_superblock_name_length_max cs sb.name_length_max;
  set_superblock_file_size_max cs sb.file_size_max;
  set_superblock_file_attribute_size_max cs sb.file_attribute_size_max

let to_cstruct sb =
  let cs = Cstruct.create sizeof_superblock in
  into_cstruct cs sb;
  cs

let name =
  let tag = Tag.({
      valid = true;
      type3 = LFS_TYPE_NAME, 0xff;
      id = 0;
      length = 8; })
  in
  (tag, Cstruct.of_string magic)

let inline_struct block_size block_count =
  let entry = {
      version_major = (fst version);
      version_minor = (snd version);
      block_size;
      block_count;
      name_length_max;
      file_size_max;
      file_attribute_size_max;
    } 
  and tag = Tag.({
      valid = true;
      type3 = LFS_TYPE_STRUCT, 0x01;
      id = 0;
      length = sizeof_superblock;
    })
  in
  (tag, to_cstruct entry)
