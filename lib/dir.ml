type directory = {
  block_pair : Block.t * Block.t;
  revision_count : Cstruct.uint32;
  tail : Block.t * Block.t;
}

let create id = Tag.({
    valid = true;
    type3 = (Tag.LFS_TYPE_FILE, 0x01);
    id;
    length = 0;
  })

let dir ~id length = Tag.({
    valid = true;
    type3 = (Tag.LFS_TYPE_NAME, 0x00);
    id;
    length;
  })

let dirstruct id = Tag.({
    valid = true;
    type3 = (Tag.LFS_TYPE_STRUCT, 0x00);
    id;
    length = 32 * 2 ;(* content is pointers to the metadata pair, two 32-bit values *)
  })

let soft_tail = Tag.({
    valid = true;
    type3 = Tag.LFS_TYPE_TAIL, 0x00;
    id = 0x3ff;
    length = 32 * 2;
  })

let blocks (block1, block2) =
  let cs = Cstruct.create @@ 32 * 2 in
  Cstruct.LE.set_uint32 cs 0 block1;
  Cstruct.LE.set_uint32 cs 4 block2;
  cs

let create_root_dir id path bs =
  if String.length path > Limits.max_filename_length then Error `Too_long
  else begin
    let start_create = (create id, Cstruct.empty)
    and directory = dir ~id (String.length path), Cstruct.of_string path
    and structure = (dirstruct id), (blocks bs)
    and soft_tail = soft_tail, (blocks bs)
    in
    Ok (start_create, directory, structure, soft_tail)
  end

let dirstruct_of_cstruct cs =
  if Cstruct.length cs < (4 + 4) then Error (`Msg "dirstruct too small to contain a metadata pair pointer")
  else Ok (Cstruct.LE.(get_uint32 cs 0 |> Int64.of_int32,
                       get_uint32 cs 4 |> Int64.of_int32))
