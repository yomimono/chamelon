let program_block_size = 16
(* fairly arbitrary. probably should be specifiable in keys, but y'know *)

open Lwt.Infix

type error = [
  | `Block of Mirage_block.error
  | `KV of Mirage_kv.error
  | `Littlefs of [ `Corrupt ]
]

type littlefs_write_error = [
    `Too_long (* path exceeds the allowable file name size *)
  | `Out_of_space (* no more blocks are available *)
  | `Corrupt
]

module Make(Sectors: Mirage_block.S) = struct
  module This_Block = Block_ops.Make(Sectors)
  type t = {
    block : This_Block.t;
    block_size : int;
    program_block_size : int;
    lookahead : int64 list;
  }

  type write_error = [
    | `Block_write of This_Block.write_error
    | `KV_write of Mirage_kv.write_error
    | `Littlefs_write of littlefs_write_error
  ]

  let block_of_block_number {block_size; block; program_block_size; _} block_location =
    let cs = Cstruct.create block_size in
    This_Block.read block block_location [cs] >>= function
    | Error b -> Lwt.return @@ Error (`Block b)
    | Ok () ->
      match Littlefs.Block.of_cstruct ~program_block_size cs with
      | Error _ -> Lwt.return @@ Error (`Corrupt)
      | Ok extant_block -> Lwt.return @@ Ok extant_block

  (* from the littlefs spec, we should be checking whether
   * the on-disk data matches what we have in memory after
   * doing this write. Then if it doesn't, we should rewrite
   * to a different block, and note the block as bad so we don't
   * try to write to it in the future.
   *
   * I don't think that's necessary in our execution context.
   * we're not writing directly to a flash controller,
   * we're probably writing to a file on another filesystem
   * managed by an OS with its own bad block detection.
   * That's my excuse for punting on it for now, anyway. *)
  let block_to_block_number {block_size; block; program_block_size; _} data block_location =
    let cs = Cstruct.create block_size in
    Littlefs.Block.into_cstruct ~program_block_size cs data;
    This_Block.write block block_location [cs]

  let block_of_block_pair t (l1, l2) =
    block_of_block_number t l1 >>= function
    | Error _ as e -> Lwt.return e
    | Ok b1 -> block_of_block_number t l2 >>= function
      | Error _ as e -> Lwt.return e
      | Ok b2 -> if Littlefs.Block.(compare (revision_count b1) (revision_count b2)) < 0
        then Lwt.return @@ Ok b2
        else Lwt.return @@ Ok b1

  let block_to_block_pair t data (b1, b2) =
    block_to_block_number t data b1 >>= function
    | Error _ as e -> Lwt.return e
    | Ok () -> block_to_block_number t data b2

  let linked_blocks root =
    let entries = Littlefs.Commit.entries in
    let links_of_commit c = List.filter_map Littlefs.Entry.links @@ entries c in
    List.(flatten @@ map links_of_commit (Littlefs.Block.commits root))

  let rec get_ctz_pointers t l index pointer =
    match l with
    | Error _ as e -> Lwt.return e
    | Ok l ->
      let data = Cstruct.create t.block_size in
      This_Block.read t.block pointer [data] >>= function
      | Error _ as e -> Lwt.return e
      | Ok () -> begin
          let pointers, _data_region = Littlefs.File.of_block index data in
          match pointers with
          | [] -> Lwt.return @@ Ok (pointer::l)
          | next::_ -> get_ctz_pointers t (Ok (pointer::l)) (index - 1) (Int64.of_int32 next)
        end

  let rec follow_links t = function
    | Littlefs.Entry.Data (pointer, length) -> begin
        let file_size = Int32.to_int length in
        let index = Littlefs.File.last_block_index ~file_size ~block_size:t.block_size in
        get_ctz_pointers t (Ok []) index (Int64.of_int32 pointer)
      end
    | Littlefs.Entry.Metadata (a, b) ->
      block_of_block_pair t (a, b) >>= function
      | Error _ -> Lwt.return @@ Ok []
      | Ok block ->
        let links = linked_blocks block in
        Lwt_list.fold_left_s (fun l link ->
            follow_links t link >>= function
            | Error _ -> Lwt.return @@ l
            | Ok new_links ->
              Lwt.return @@ (new_links @ l)
          ) ([]) links
        >>= fun list -> Lwt.return @@ Ok (a :: b :: list)

  let unused t l1 =
    let module IntSet = Set.Make(Int64) in
    let possible_blocks = This_Block.block_count t.block in
    let all_indices = IntSet.of_list (List.init possible_blocks (fun a -> Int64.of_int a)) in
    let set1 = IntSet.of_list l1 in
    let candidates = IntSet.diff all_indices set1 in
    (* TODO: c'mon *)
    [IntSet.min_elt candidates]

  let get_block t =
    match t.lookahead with
    | block::l -> Lwt.return @@ Ok (block, {t with lookahead = l})
    | [] ->
      (* TODO try to repopulate the lookahead buffer *)
      follow_links t (Littlefs.Entry.Metadata (0L, 1L)) >|= function
      | Error _ -> Error (`Littlefs `Corrupt) (* TODO: not quite *)
      | Ok used_blocks ->
        match unused t used_blocks with
        | [] -> Error (`Littlefs_write `Out_of_space)
        | block::l -> Ok (block, {t with lookahead = l})

  let connect device ~program_block_size ~block_size : (t, error) result Lwt.t =
    This_Block.connect ~block_size device >>= fun block ->
    (* TODO: setting an empty lookahead to generate a good-enough `t`
     * to populate the lookahead buffer
     * feels very kludgey and error-prone. We should either
     * make the functions that don't need allocable blocks marked
     * in the type system somehow,
     * or have them only take the arguments they need instead of a full `t` *)
    let t = {block; block_size; program_block_size; lookahead = []} in
    follow_links t (Littlefs.Entry.Metadata (0L, 1L)) >>= function
    | Error _e -> Lwt.fail_with "couldn't get list of used blocks"
    | Ok used_blocks ->
      let lookahead = unused t used_blocks in
      Lwt.return @@ Ok {lookahead; block; block_size; program_block_size}

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
    let tag_matches t = Littlefs.Tag.(fst t.type3 = LFS_TYPE_NAME)
    in
    List.find_map (fun c ->
        match List.find_opt (fun (tag, data) ->
            tag_matches tag && data_matches data
          ) (Littlefs.Commit.entries c) with
        | Some (tag, _) -> Some tag.Littlefs.Tag.id
        | None -> None
      ) (Littlefs.Block.commits block)

  let entries_of_id block id =
    let commits = Littlefs.Block.commits block in
    let matches (tag, _) =
      0 = compare tag.Littlefs.Tag.id id
    in
    let aux c = List.find_all matches (Littlefs.Commit.entries c) in
    List.(flatten @@ map aux commits)

  let rec find_directory t block = function
    | [] -> Lwt.return (`Basename_on block)
    | key::remaining ->
      match id_of_key block (Mirage_kv.Key.v key) with
      | None -> Lwt.return `No_id
      | Some id ->
        match entries_of_id block id with
        | [] -> Lwt.return `No_entry
        | l ->
          match List.filter_map Littlefs.Dir.of_entry l with
          | [] -> Lwt.return `No_structs
          | next_blocks::_ ->
            block_of_block_pair t next_blocks >>= function
            | Error _ -> Lwt.return `Bad_pointer
            | Ok next_block ->
             find_directory t next_block remaining

  let list_block block =
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
    let commits = Littlefs.Block.commits block in
    let relevant_entries c = List.filter_map info_of_entry
        (Littlefs.Commit.entries c)
    in
    List.flatten @@ List.map relevant_entries commits

  let list t key =
    let block_location = 0L in
    block_of_block_number t block_location >>= function
    | Error _ as e -> Lwt.return e
    | Ok start_block ->
      match (Mirage_kv.Key.segments key) with
      | [] -> Lwt.return @@ Ok (list_block start_block)
      | segments ->
        find_directory t start_block segments >>= function
        | `No_id | `No_structs | `No_entry | `Bad_pointer ->
          Lwt.return @@ Error (`Not_found key)
        | `Basename_on extant_block ->
          Lwt.return @@ Ok (list_block extant_block)

  let get_ctz t (pointer, length) =
    let rec read_block l index pointer =
      let data = Cstruct.create t.block_size in
      This_Block.read t.block pointer [data] >>= function
      | Error _ as e -> Lwt.return e
      | Ok () ->
        let pointers, data_region = Littlefs.File.of_block index data in
        match pointers with
        | next::_ ->
          read_block (data_region :: l) (index - 1) (Int64.of_int32 next)
        | [] ->
          Lwt.return @@ Ok (data_region :: l)
    in
    let index = Littlefs.File.last_block_index ~file_size:length
                                ~block_size:t.block_size in
    read_block [] index pointer >>= function
    | Error e -> Lwt.return @@ Error (`Block e)
    | Ok l ->
      (* the last block very likely needs to be trimmed *)
      let cs = Cstruct.sub (Cstruct.concat l) 0 length in
      let s = Cstruct.(to_string cs) in
      Lwt.return @@ Ok s

  let get_value block key =
    match id_of_key block key with
    | None -> Error (`Not_found key)
    | Some id ->
      match entries_of_id block id with
      | [] -> Error (`Not_found key)
      | l ->
        let inline_files = List.find_opt (fun (tag, _data) ->
            Littlefs.Tag.((fst tag.type3) = LFS_TYPE_STRUCT) &&
            Littlefs.Tag.((snd tag.type3) = 0x01)
          ) 
        in
        let ctz_files = List.find_opt (fun (tag, _block) ->
            Littlefs.Tag.((fst tag.type3 = LFS_TYPE_STRUCT) &&
                          Littlefs.Tag.((snd tag.type3 = 0x02)
                                       ))) in
        match inline_files l, ctz_files l with
        (* TODO: we should make sure a dictionary entry is there
         * before returning this error *)
        | None, None -> Error (`Value_expected key)
        | Some (_tag, data), None -> Ok (`Inline (Cstruct.to_string data))
        | _, Some (_, ctz) ->
          match Littlefs.File.ctz_of_cstruct ctz with
          | Some (pointer, length) -> Ok (`Ctz (Int64.of_int32 pointer, Int32.to_int length))
          | None -> Error (`Value_expected key)

  let get t key =
    let default = (0L, 1L) in
    block_of_block_pair t default >>= function
    | Error _ as e -> Lwt.return e
    | Ok extant_block ->
      match Mirage_kv.Key.segments key with
      | [] -> Lwt.return @@ Error (`Not_found key)
      | basename::[] -> begin
          match get_value extant_block @@ Mirage_kv.Key.v basename with
          | Ok (`Inline d) -> Lwt.return (Ok d)
          | Ok (`Ctz ctz) -> get_ctz t ctz
          | Error _ as e -> Lwt.return e
        end
      | _ ->
        let dirname = Mirage_kv.Key.(parent key |> segments) in
        find_directory t extant_block dirname >>= function
        | `Basename_on block -> begin
            match get_value block Mirage_kv.Key.(v @@ basename key) with
            | Ok (`Inline d) -> Lwt.return (Ok d)
            | Ok (`Ctz ctz) -> get_ctz t ctz
            | Error _ as e -> Lwt.return e
          end
        | _ -> Lwt.return @@ Error (`Not_found key)

  let set_value blockpair t key data =
    (* for now, all keys are just their basenames *)
    (* we could handle adding to extant directories without a block allocator
     * (for writes small enough to be inline, that is)
     * but in order to make new directories, we need to be
     * able to make new metadata pairs,
     * which means we need a block allocator *)
    let filename = Mirage_kv.Key.basename key in
    (* get the set of already-committed IDs in these blocks *)
    block_of_block_pair t blockpair >>= function
    | Error e -> Lwt.return (Error (`Corrupt e))
    | Ok extant_block ->
      (* TODO: we may need to do more work if the root directory
       * goes on past the first metadata pair *)
      let used_ids = Littlefs.Block.ids extant_block in
      let next = (Littlefs.Block.IdSet.max_elt used_ids) + 1 in
      let file = Littlefs.File.write filename next (Cstruct.of_string data) in
      let new_block = Littlefs.Block.add_commit extant_block file in
      block_to_block_pair t new_block blockpair >>= function
      | Error e -> Lwt.return (Error (`Littlefs_write e))
      | Ok () -> Lwt.return (Ok ())

  let set t key data =
    (* if `key` is just a basename, write to the root directory *)
    if Mirage_kv.Key.(equal empty @@ parent key) then
      set_value (0L, 1L) t key data
    else Lwt.return (Error (`Littlefs `Corrupt))

end
