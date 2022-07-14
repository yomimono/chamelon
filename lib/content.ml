let size (tag, data) =
  match tag.Tag.type3 with
  | Tag.LFS_TYPE_STRUCT, c when c = File.inline_struct_chunk ->
    `File tag.Tag.length
  | Tag.LFS_TYPE_STRUCT, c when c = File.ctz_chunk -> begin
      match File.ctz_of_cstruct data with
      | None -> `Skip
      | Some (_, file_size) -> `File (Int32.to_int file_size)
    end
  | Tag.LFS_TYPE_STRUCT, c when c = 0x00 -> begin
    match Dir.dirstruct_of_cstruct data with
    | Some pair -> `Dir pair
    | None -> `Skip
  end
  | _ -> `Skip
