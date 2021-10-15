module Make(Mirage_block : Mirage_block.S) = struct

  let format device ~block_size =
    let open Lwt.Infix in
    Mirage_block.get_info device >>= fun {sector_size; size_sectors; _} ->
    let block_count =
      let size_in_bytes = Int64.(mul (of_int sector_size) size_sectors) in
      Int64.(div size_in_bytes @@ of_int32 block_size) |> Int64.to_int32
    in
    let name = Littlefs.Superblock.name in
    let inline_struct = Littlefs.Superblock.inline_struct block_size block_count in
    let write_me = Littlefs.Block.commit block_size Littlefs.Block.empty [name; inline_struct] in
    Mirage_block.write device 0L [(Littlefs.Block.to_cstruct ~block_size write_me)] >>= fun _ ->
    Mirage_block.write device (Int64.of_int32 block_size) [(Littlefs.Block.to_cstruct ~block_size write_me)];
end
