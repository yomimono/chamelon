let size (tag, data) =
  if tag.Tag.id >= 0x3ff || tag.Tag.id <= 0x00
  then `Skip
  else begin
    match tag.Tag.type3 with
    | Tag.LFS_TYPE_STRUCT, c when c = Tag.Magic.struct_inline ->
      `File tag.Tag.length
    | Tag.LFS_TYPE_STRUCT, c when c = Tag.Magic.struct_ctz -> begin
      match File.ctz_of_cstruct data with
      | None -> `Skip
      | Some (_, file_size) -> `File (Int32.to_int file_size)
    end
    | Tag.LFS_TYPE_STRUCT, c when c = Tag.Magic.struct_dir -> begin
        match Dir.dirstruct_of_cstruct data with
        | Some pair -> `Dir pair
        | None -> `Skip
      end
    | _ -> `Skip
  end
