let program_block_size = 16
(* fairly arbitrary. probably should be specifiable in keys, but y'know *)

open Lwt.Infix

type error = [
  | `Block of Mirage_block.error
  | `KV of Mirage_kv.error
]

type littlefs_write_error = [
    `Too_long (* path exceeds the allowable file name size *)
]

module Make(Sectors: Mirage_block.S) = struct
  module This_Block = Block_ops.Make(Sectors)
  type t = {
    block : This_Block.t;
    block_size : int;
    program_block_size : int;
  }

  type write_error = [
    | `Block_write of This_Block.write_error
    | `KV_write of Mirage_kv.write_error
    | `Littlefs_write of littlefs_write_error
  ]

  module Allocator = struct
    (* TODO: uh, eventually we'll need a real allocator :sweat_smile: *)
    let next _ = (2l, 3l)
  end

  let connect device ~program_block_size ~block_size : (t, error) result Lwt.t =
    This_Block.connect ~block_size device >>= fun block ->
    let block_0, block_1 = Cstruct.(create block_size, create block_size) in
    This_Block.read block 0L [block_0] >>= fun _ ->
    This_Block.read block (Int64.of_int block_size) [block_1] >>= fun _ ->
    (* TODO: we should see which is the more recent write and treat that one as authoritative *)
    match Littlefs.Block.of_cstruct ~program_block_size block_0 with
    | _b1 -> begin
        (* TODO: for now, everything we would care about
         * from reading the FS is either hardcoded in
         * the implementation, or needs to be provided
         * in order to read the filesystem. If we can
         * make some meaning out of the blocks, call
         * that good enough. *)
        Lwt.return (Ok {block; block_size; program_block_size})
      end

  let format t =
    let program_block_size = t.program_block_size in
    let block_size = t.block_size in
    let write_whole_block n b = This_Block.write t.block n
        [Littlefs.Block.to_cstruct ~program_block_size ~block_size b]
    in
    let name = Littlefs.Superblock.name in
    let block_count = This_Block.block_count t.block in
    let superblock_inline_struct = Littlefs.Superblock.inline_struct (Int32.of_int block_size) (Int32.of_int block_count) in
    let block_0 = Littlefs.Block.of_entries ~revision_count:1 [name; superblock_inline_struct] in
    let block_1 = Littlefs.Block.of_entries ~revision_count:2 [name; superblock_inline_struct] in
    write_whole_block 0L block_0 >>= fun _ ->
    write_whole_block 1L block_1
  
  let id_of_key block key =
    let data_matches c =
      0 = Mirage_kv.Key.(compare key @@ v @@ Cstruct.to_string c)
    in
    let tag_matches t =
      Littlefs.Tag.(fst t.type3 = LFS_TYPE_NAME && snd t.type3 = 0x01)
    in
    List.find_map (fun c ->
        match List.find_opt (fun (tag, data) ->
            tag_matches tag && data_matches data
          ) (Littlefs.Commit.entries c) with
        | Some (tag, _) -> Some (c, tag.Littlefs.Tag.id)
        | None -> None
      ) (Littlefs.Block.commits block)

  let find_inline_file_contents block id =
    let commits = Littlefs.Block.commits block in
    (* TODO: we shouldn't keep searching after one result *)
    let matches (tag, _) =
      let type3, chunk = tag.Littlefs.Tag.type3 in
      0 = compare tag.Littlefs.Tag.id id &&
      type3 = Littlefs.Tag.LFS_TYPE_STRUCT &&
      chunk = 0x01
    in
    let aux c = List.find_opt matches (Littlefs.Commit.entries c) in
    List.find_map aux commits

  let get {program_block_size; block; block_size; } key =
    (* TODO: use the filesystem to find the right blocks to search *)
    let block_location = 0L in
    let cs = Cstruct.create block_size in
    This_Block.read block block_location [cs] >>= function
    | Error b -> Lwt.return (Error (`Block b))
    | Ok () ->
      match Littlefs.Block.of_cstruct ~program_block_size cs with
      | Error _ -> Lwt.fail_with "couldn't read block"
      | Ok extant_block ->
        match id_of_key extant_block key with
        | None -> Lwt.return (Error (`Not_found key))
        | Some (_commit, id) -> match find_inline_file_contents extant_block id with
          | None -> Lwt.return (Error (`Not_found key))
          | Some (_tag, data) -> Lwt.return (Ok (Cstruct.to_string data))

  (* TODO: we really need a convenience function for "read me this pair of blocks" *)

  let set {block_size; program_block_size; block} key data =
    (* for now, all keys are just their basenames *)
    let filename = Mirage_kv.Key.basename key in
    (* for now, all writes and reads occur in the root blocks *)
    let blockpair = (0L, 1L) in
    (* get the set of already-committed IDs in these blocks *)
    let block_0, _block_1 = Cstruct.(create block_size, create block_size) in
    This_Block.read block (fst blockpair) [block_0] >>= function
    | Error _ as e -> Lwt.return e
    | Ok () ->
      match Littlefs.Block.of_cstruct ~program_block_size block_0 with
      | Error _ -> Lwt.fail_with "couldn't deserialize block 0"
      | Ok extant_block ->
        let used_ids = Littlefs.Block.ids extant_block in
        let next = (Littlefs.Block.IdSet.max_elt used_ids) + 1 in
        let file = Littlefs.File.write filename next (Cstruct.of_string data) in
        let new_block = Littlefs.Block.add_commit extant_block file in
        Littlefs.Block.into_cstruct ~program_block_size block_0 new_block;
        This_Block.write block (fst blockpair) [block_0] >>= function
        | Error _ -> Lwt.fail_with "couldn't write back to block 0"
        | Ok () -> Lwt.return (Ok ())

end
