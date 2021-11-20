let root_pair = (0L, 1L)

open Lwt.Infix

module Make(Sectors: Mirage_block.S) = struct
  module This_Block = Block_ops.Make(Sectors)
  type t = {
    block : This_Block.t;
    block_size : int;
    program_block_size : int;
    lookahead : ([`Before | `After ] * (int64 list)) ref;
  }

  type key = Mirage_kv.Key.t

  (* error type definitions straight outta mirage-kv *)
  type error = [
    | `Not_found           of key (** key not found *)
    | `Dictionary_expected of key (** key does not refer to a dictionary. *)
    | `Value_expected      of key (** key does not refer to a value. *)
  ]
  type write_error = [
    | error
    | `No_space                (** No space left on the device. *)
    | `Too_many_retries of int (** {!batch} has been trying to commit [n] times
                                   without success. *)
  ]

  let block_of_block_number {block_size; block; program_block_size; _} block_location =
    let cs = Cstruct.create block_size in
    This_Block.read block block_location [cs] >>= function
    | Error b -> Lwt.return @@ Error (`Block b)
    | Ok () ->
      match Littlefs.Block.of_cstruct ~program_block_size cs with
      | Error _ -> Lwt.return @@ Error (`Littlefs `Corrupt)
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

  let unused ~bias t l1 =
    let module IntSet = Set.Make(Int64) in
    let possible_blocks = This_Block.block_count t.block in
    let all_indices = IntSet.of_list (List.init possible_blocks (fun a -> Int64.of_int a)) in
    let set1 = IntSet.of_list l1 in
    let candidates = IntSet.diff all_indices set1 in
    let pivot = Int64.(div (of_int possible_blocks) 2L) in
    let set = match bias with
      | `Before -> let s, _, _ = IntSet.split pivot candidates in s
      | `After -> let _, _, s = IntSet.split pivot candidates in s
    in
    IntSet.elements set

  let opp = function
    | `Before -> `After
    | `After -> `Before
  
  let get_block t =
    match !(t.lookahead) with
    | bias, block::l ->
      t.lookahead := bias, l;
      Lwt.return @@ Ok block
    | bias, [] ->
      (* TODO try to repopulate the lookahead buffer *)
      follow_links t (Littlefs.Entry.Metadata root_pair) >|= function
      | Error _ -> Error (`Littlefs `Corrupt) (* TODO: not quite *)
      | Ok used_blocks ->
        match unused ~bias t used_blocks with
        | [] -> Error (`Littlefs_write `Out_of_space)
        | block::l ->
          t.lookahead := (opp bias, l);
          Ok block

  let connect device ~program_block_size ~block_size : (t, error) result Lwt.t =
    This_Block.connect ~block_size device >>= fun block ->
    (* TODO: setting an empty lookahead to generate a good-enough `t`
     * to populate the lookahead buffer
     * feels very kludgey and error-prone. We should either
     * make the functions that don't need allocable blocks marked
     * in the type system somehow,
     * or have them only take the arguments they need instead of a full `t` *)
    let t = {block; block_size; program_block_size; lookahead = ref (`Before, [])} in
    follow_links t (Littlefs.Entry.Metadata root_pair) >>= function
    | Error _e -> Lwt.fail_with "couldn't get list of used blocks"
    | Ok used_blocks ->
      let lookahead = ref (`After, unused ~bias:`Before t used_blocks) in
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
    write_whole_block (fst root_pair) block_0 >>= fun b0 ->
    write_whole_block (snd root_pair) block_1 >>= fun b1 ->
    match b0, b1 with
    | Ok (), Ok () -> Lwt.return @@ Ok ()
    | _, _ -> Lwt.return @@ Error `No_space
  
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

  (** [follow_directory_pointers t block_pair l] returns:
   * an error or
   * `Basename_on block_pair: you have reached the termination of the directory traversal;
   * items within the path can be found at [block_pair]
   * `Continue (l, block_pair) : there is more directory structure to be traversed;
   * the remaining elements [l] can be found starting at [block_pair]
   * *)
  let follow_directory_pointers t block_pair = function
    | [] -> Lwt.return (`Basename_on block_pair)
    | key::remaining ->
      block_of_block_pair t block_pair >>= function
      | Error _ -> Lwt.return `Bad_pointer
      | Ok block ->
        match id_of_key block (Mirage_kv.Key.v key) with
        | None -> Lwt.return (`No_id key)
        | Some id ->
          match entries_of_id block id with
          | [] -> Lwt.return `No_entry
          | l ->
            match List.filter_map Littlefs.Dir.of_entry l with
            | [] -> Lwt.return `No_structs
            | next_blocks::_ -> Lwt.return (`Continue (remaining, next_blocks))

  let rec find_directory_block_pair t block_pair key =
    follow_directory_pointers t block_pair key >>= function
    | `Continue (path, next_blocks) -> find_directory_block_pair t next_blocks path
    | `No_id child ->
      let path = String.concat "/" key in
      let path = Mirage_kv.Key.(to_string @@ v path / child) in
      Lwt.return (`No_id path)
    | a -> Lwt.return a

  (* `dirname` is the name of the directory relative to `rootpair`. It should be
   * a value that could be returned from `Mirage_kv.Key.basename` - in other words
   * it should contain no separators. *)
  let plain_mkdir t rootpair (dirname : string) =
    follow_directory_pointers t rootpair [dirname] >>= function
    | `Continue (_path, next_blocks) -> Lwt.return @@ Ok next_blocks
    | `Basename_on next_blocks -> Lwt.return @@ Ok next_blocks
    | _ ->
      get_block t >>= function
      | Error _ -> Lwt.return @@ Error (`Not_found dirname)
      | Ok dir_block_0 ->
        get_block t >>= function
        | Error _ -> Lwt.return @@ Error (`Not_found dirname)
        | Ok dir_block_1 ->
          block_of_block_pair t rootpair >>= function
          | Error _ -> Lwt.return @@ Error (`Not_found dirname)
          | Ok root_block ->
            let dir_id = Littlefs.Block.(IdSet.max_elt @@ ids root_block) + 1 in
            let name = Littlefs.Dir.name dirname dir_id in
            let dirstruct = Littlefs.Dir.mkdir ~to_pair:(dir_block_0, dir_block_1) dir_id in
            let new_block = Littlefs.Block.add_commit root_block [name; dirstruct] in
            block_to_block_pair t new_block rootpair >>= function
            | Error _ -> Lwt.return @@ Error (`Not_found dirname)
            | Ok () -> Lwt.return @@ Ok (dir_block_0, dir_block_1)

  let rec mkdir t rootpair key =
    match key with
    | [] -> Lwt.return @@ Ok rootpair
    | dirname::more ->
      plain_mkdir t rootpair dirname >>= function
      | Error _ as e -> Lwt.return e
      | Ok newpair ->
        mkdir t newpair more
  
  let rec find_directory t block key =
    match key with
    | [] -> Lwt.return (`Basename_on block)
    | key::remaining ->
      match id_of_key block (Mirage_kv.Key.v key) with
      | None -> Lwt.return @@ `No_id key
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

  let list t key : ((string * [`Dictionary | `Value]) list, error) result Lwt.t =
    block_of_block_pair t root_pair >>= function
    | Error _ -> Lwt.return @@ Error (`Not_found key)
    | Ok start_block ->
      match (Mirage_kv.Key.segments key) with
      | [] -> Lwt.return @@ Ok (list_block start_block)
      | segments ->
        find_directory t start_block segments >>= function
        | `No_id k -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v k))
        | `No_structs | `No_entry | `Bad_pointer ->
          Lwt.return @@ Error (`Not_found key)
        | `Basename_on extant_block ->
          Lwt.return @@ Ok (list_block extant_block)

  let get_ctz t key (pointer, length) =
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
    | Error _ -> Lwt.return @@ Error (`Not_found key)
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

  let get t key : (string, error) result Lwt.t =
    block_of_block_pair t root_pair >>= function
    | Error _ -> Lwt.return @@ Error (`Not_found key)
    | Ok extant_block ->
      match Mirage_kv.Key.segments key with
      | [] -> Lwt.return @@ Error (`Not_found key)
      | basename::[] -> begin
          match get_value extant_block @@ Mirage_kv.Key.v basename with
          | Ok (`Inline d) -> Lwt.return (Ok d)
          | Ok (`Ctz ctz) -> get_ctz t key ctz
          | Error _ -> Lwt.return @@ Error (`Not_found key)
        end
      | _ ->
        let dirname = Mirage_kv.Key.(parent key |> segments) in
        find_directory t extant_block dirname >>= function
        | `Basename_on block -> begin
            match get_value block Mirage_kv.Key.(v @@ basename key) with
            | Ok (`Inline d) -> Lwt.return (Ok d)
            | Ok (`Ctz ctz) -> get_ctz t key ctz
            | Error _ -> Lwt.return @@ Error (`Not_found key)
          end
        | _ -> Lwt.return @@ Error (`Not_found key)

  let rec write_ctz_block t l index so_far data =
    if Int.compare so_far (String.length data) >= 0 then begin
      (* we purposely don't reverse the list because we're going to want
       * the *last* block for inclusion in the ctz structure *)
      Lwt.return @@ Ok l
    end else begin
      get_block t >>= function
      | Error _ -> Lwt.return @@ Error `No_space
      | Ok block_number ->
        Printf.eprintf "using block number %Ld (0x%Lx) for block index %d (0x%x)\n%!" block_number block_number index index;
        let pointer = Int64.to_int32 block_number in
        let block_cs = Cstruct.create t.block_size in
        let skip_list_size = Littlefs.File.n_pointers index in
        let skip_list_length = skip_list_size * 4 in
        let data_length = min (t.block_size - skip_list_length) ((String.length data) - so_far) in
        (* TODO: this does not implement writing the full skip list;
         * rather it writes only the first pointer (the one to the
         * previous block) and leaves the rest blank *)
        (match l with
         | [] -> ()
         | (_, last_pointer)::_ ->
           Cstruct.LE.set_uint32 block_cs 0 last_pointer
        );
        Cstruct.blit_from_string data so_far block_cs skip_list_length data_length;
        This_Block.write t.block (Int64.of_int32 pointer) [block_cs] >>= function
        | Error _ -> Lwt.return @@ Error `No_space
        | Ok () ->
          write_ctz_block t ((index, pointer)::l) (index + 1) (so_far + data_length) data
    end

  let write_in_ctz root_block_pair t filename data =
    block_of_block_pair t root_block_pair >>= function
    | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v filename))
    | Ok root ->
      let file_size = String.length data in
      write_ctz_block t [] 0 0 data >>= function
      | Error _ as e -> Lwt.return e
      | Ok [] -> Lwt.return @@ Error `No_space
      | Ok ((last_index, last_pointer)::_) ->
        Printf.eprintf "wrote raw data blocks; last index is %d (0x%x) at %ld (0x%lx)\n%!" last_index last_index last_pointer last_pointer;
        let used_ids = Littlefs.Block.ids root in
        let next = match Littlefs.Block.IdSet.max_elt_opt used_ids with
          | None -> 1
          | Some n -> n + 1
        in
        let name = Littlefs.File.name filename next in
        let ctz = Littlefs.File.create_ctz next
            ~pointer:last_pointer ~file_size:(Int32.of_int file_size)
        in
        let new_block = Littlefs.Block.add_commit root [name; ctz] in
        block_to_block_pair t new_block root_block_pair >>= function
        | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v filename))
        | Ok () -> Lwt.return @@ Ok ()

  let write_inline block_pair t filename data =
    block_of_block_pair t block_pair >>= function
    | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v filename))
    | Ok extant_block ->
      (* TODO: we may need to do more work if the root directory
       * goes on past the first metadata pair *)
      let used_ids = Littlefs.Block.ids extant_block in
      let next = match Littlefs.Block.IdSet.max_elt_opt used_ids with
        | None -> 1
        | Some n -> n + 1
      in
      let file = Littlefs.File.write_inline filename next (Cstruct.of_string data) in
      let new_block = Littlefs.Block.add_commit extant_block file in
      block_to_block_pair t new_block block_pair >>= function
      | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v filename))
      | Ok () -> Lwt.return @@ Ok ()

  let set_in_directory block_pair t (filename : string) data =
    if (String.length data) > (t.block_size / 4) then
      write_in_ctz block_pair t filename data
    else
      write_inline block_pair t filename data

  let set t key data : (unit, write_error) result Lwt.t =
    let dir = Mirage_kv.Key.parent key in
    find_directory_block_pair t root_pair (Mirage_kv.Key.segments dir) >>= function
    | `Basename_on block_pair -> set_in_directory block_pair t (Mirage_kv.Key.basename key) data
    | `No_id path -> begin
      mkdir t root_pair (Mirage_kv.Key.segments dir) >>= function
      | Error _ -> Lwt.return @@ (Error (`Not_found (Mirage_kv.Key.v path)))
      | Ok block_pair ->
        set_in_directory block_pair t (Mirage_kv.Key.basename key) data
      end
    | _ -> Lwt.return @@ Error (`Not_found key)

end
