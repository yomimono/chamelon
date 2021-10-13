module Make(Block : Mirage.BLOCK) = struct

  (* format [block] with [logical] block size, [program] block size, and [read] block size *)
  let format ~logical ~program ~read block =
    let info = Block.get_info block in
    (* I... guess "logical" is what we use by default *)
    let block_count =
      let open Int64 in
      info.size_sector * (of_int info.size_sectors) / (of_int logical)
    in
    let struct_tag, inline_struct = Superblock.inline_struct ~block_size:logical ~block_count in
    let name = Superblock.name in
    let new_block = Block.({revision_count = Int32.zero;
                            entries = [name; inline_struct];
    Block.write block 0 [name; inline_struct]
    ()
end
