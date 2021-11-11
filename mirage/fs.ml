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

  let block_of_block_number {block_size; block; program_block_size; _} block_location =
    let cs = Cstruct.create block_size in
    This_Block.read block block_location [cs] >>= function
    | Error b -> Lwt.return (Error (`Block b))
    | Ok () ->
      match Littlefs.Block.of_cstruct ~program_block_size cs with
      | Error _ -> Lwt.return (Error (`Littlefs_read))
      | Ok extant_block -> Lwt.return (Ok extant_block)

  let connect device ~program_block_size ~block_size : (t, error) result Lwt.t =
    (* TODO: for now, everything we would care about
     * from reading the FS is either hardcoded in
     * the implementation, or needs to be provided
     * in order to read the filesystem. For now, if we can
     * connect to the underlying device, call that good enough. *)
    This_Block.connect ~block_size device >>= fun block ->
    Lwt.return (Ok {block; block_size; program_block_size})

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
  
  let id_of_key (ty, chunk) block key =
    let data_matches c =
      0 = Mirage_kv.Key.(compare key @@ v @@ Cstruct.to_string c)
    in
    let tag_matches t =
      Littlefs.Tag.(fst t.type3 = ty && snd t.type3 = chunk)
    in
    List.find_map (fun c ->
        match List.find_opt (fun (tag, data) ->
            tag_matches tag && data_matches data
          ) (Littlefs.Commit.entries c) with
        | Some (tag, _) -> Some (c, tag.Littlefs.Tag.id)
        | None -> None
      ) (Littlefs.Block.commits block)

  let entries_of_id block id =
    let commits = Littlefs.Block.commits block in
    let matches (tag, _) =
      0 = compare tag.Littlefs.Tag.id id
    in
    let aux c = List.find_all matches (Littlefs.Commit.entries c) in
    List.(flatten @@ map aux commits)

  (* for now, ignore key -- in the future, we'll look up which metadata
   * pairs it points to and list those instead of block 0 *)
  let list t _key =
    let block_location = 0L in
    block_of_block_number t block_location >>= function
    | Error _ as e -> Lwt.return e
    | Ok extant_block ->
      (* we want to list all names in all commits in the block,
       * preserving information about which of them are files and which directories *)
      let info_of_entry (tag, data) =
        match tag.Littlefs.Tag.type3 with
        | (LFS_TYPE_NAME, 0x01) ->
          Some (Cstruct.to_string data, `Value)
        | (LFS_TYPE_NAME, 0x02) ->
          Some (Cstruct.to_string data, `Dictionary)
        | _ -> None
      in
      let commits = Littlefs.Block.commits extant_block in
      let relevant_entries c = List.filter_map info_of_entry
          (Littlefs.Commit.entries c)
      in
      Lwt.return @@ Ok (List.flatten @@ List.map relevant_entries commits)

  let get t key =
    let get_from_block block_location =
      block_of_block_number t block_location >>= function
      | Error _ as e -> Lwt.return e
      | Ok extant_block ->
        match id_of_key (Littlefs.Tag.LFS_TYPE_NAME, 0x01) extant_block key with
        | None -> Lwt.return (Error (`Not_found key))
        | Some (_commit, id) ->
          match entries_of_id extant_block id with
          | [] -> Lwt.return (Error (`Not_found key))
          | l ->
            match List.find_opt (fun (tag, _data) ->
                Littlefs.Tag.((fst tag.type3) = LFS_TYPE_STRUCT) &&
                Littlefs.Tag.((snd tag.type3) = 0x01)
              ) l with
          (* TODO: we should make sure the dictionary entry is there before returning this error *)
            | None -> Lwt.return (Error (`Value_expected key))
            | Some (_tag, data) -> Lwt.return (Ok (Cstruct.to_string data))
    in
    (* TODO: we have a bit more work to do to discover the right block ;) *)
    get_from_block 0L

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
