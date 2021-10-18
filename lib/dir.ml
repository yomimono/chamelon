type directory = {
  block_pair : Block.t * Block.t;
  revision_count : Cstruct.uint32;
  tail : Block.t * Block.t;
}

let create = Tag.({
    valid = true;
    type3 = (Tag.LFS_TYPE_FILE, 0x01);
    id = 0x3ff;
    length = 0;
  })

let dir length = Tag.({
    valid = true;
    type3 = (Tag.LFS_TYPE_NAME, 0x00);
    id = 0x3ff;
    length;
  })

let dirstruct = Tag.({
    valid = true;
    type3 = (Tag.LFS_TYPE_STRUCT, 0x00);
    id = 0x3ff;
    length = 32 * 2 ;(* content is pointers to the metadata pair, two 32-bit values *)
  })

let blocks (block1, block2) =
  let cs = Cstruct.create @@ 32 * 2 in
  Cstruct.LE.set_uint32 cs 0 block1;
  Cstruct.LE.set_uint32 cs 4 block2;
  cs

let create_dir path bs =
  if String.length path > Limits.max_filename_length then Error `Too_long
  else begin
    let start_create = (create, Cstruct.empty)
    and directory = dir (String.length path), Cstruct.of_string path
    and structure = dirstruct, (blocks bs)
    in
    Ok (start_create, directory, structure)
  end
