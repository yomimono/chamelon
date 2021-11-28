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

  let block_of_block_pair t (l1, l2) =
    let open Lwt_result.Infix in
    Lwt_result.both (block_of_block_number t l1) (block_of_block_number t l2) >>= fun (b1, b2) ->
    if Littlefs.Block.(compare (revision_count b1) (revision_count b2)) < 0
    then Lwt.return @@ Ok b2
    else Lwt.return @@ Ok b1

  module Traversal = struct
    let linked_blocks root =
      let entries = List.flatten @@ List.map Littlefs.Commit.entries @@ Littlefs.Block.commits root in
      (* we call `compact` on the entry list because otherwise we'd incorrectly follow
       * deleted entries *)
      List.filter_map Littlefs.Entry.links (Littlefs.Entry.compact entries)

    let rec get_ctz_pointers t l index pointer =
      match l with
      | Error _ as e -> Lwt.return e
      | Ok l ->
        let open Lwt_result.Infix in
        let data = Cstruct.create t.block_size in
        This_Block.read t.block pointer [data] >>= fun ()->
        let pointers, _data_region = Littlefs.File.of_block index data in
        match pointers with
        | [] -> Lwt.return @@ Ok (pointer::l)
        | next::_ -> get_ctz_pointers t (Ok (pointer::l)) (index - 1) (Int64.of_int32 next)

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
              | Ok new_links -> Lwt.return @@ (new_links @ l)
            ) ([]) links
          >>= fun list -> Lwt.return @@ Ok (a :: b :: list)

(* [last_block t pair] returns the last blockpair in the hardtail
 * linked list starting at [pair], which may well be [pair] itself *)
    let rec last_block t pair =
      let open Lwt_result.Infix in
      block_of_block_pair t pair >>= fun block ->
      match List.find_opt (fun e ->
          Littlefs.Tag.is_hardtail (fst e)
        ) (Littlefs.Block.entries block) with
      | None -> Lwt.return @@ Ok pair
      | Some entry -> match Littlefs.Dir.hard_tail_links entry with
        | None -> Lwt.return @@ Ok pair
        | Some next_pair -> last_block t next_pair
  end

  module Allocator = struct

    let opp = function
      | `Before -> `After
      | `After -> `Before

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

    let get_block t =
      match !(t.lookahead) with
      | bias, block::l ->
        t.lookahead := bias, l;
        Lwt.return @@ Ok block
      | bias, [] ->
        Traversal.follow_links t (Littlefs.Entry.Metadata root_pair) >|= function
        | Error _ -> Error (`Littlefs `Corrupt) (* TODO: not quite *)
        | Ok used_blocks ->
          match unused ~bias t used_blocks with
          | [] -> Error (`Littlefs_write `Out_of_space)
          | block::l ->
            t.lookahead := (opp bias, l);
            Ok block

  end

  let split block next_blockpair =
    match Littlefs.Block.commits block with
    | [] -> (* trivial case - just add the hardtail to block,
               since we have no commits to move *)
      (* we'll overwhelmingly more often end up in this case, because
       * in most situations we'll compact before we split,
       * meaning that there will be only 1 commit on the block *)
      let block = Littlefs.Block.add_commit block [Littlefs.Dir.hard_tail_at next_blockpair] in
      (block, Littlefs.Block.of_entries ~revision_count:0 [])
    | _ ->
      let entries = Littlefs.Block.entries block |> Littlefs.Entry.compact in
      let length = List.length entries in
      let half = (List.length entries) / 2 in
      let first_half = List.init half (fun i -> List.nth entries i) in
      let second_half = List.init (length - half) (fun i -> List.nth entries (half + i)) in
      let new_block = Littlefs.Block.of_entries ~revision_count:0 second_half in
      let old_block = Littlefs.Block.(of_entries ~revision_count:(revision_count block) first_half) in
      let old_block = Littlefs.Block.add_commit old_block [Littlefs.Dir.hard_tail_at next_blockpair] in
      (old_block, new_block)

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
  let block_to_block_number t data block_location =
    let {block_size; block; program_block_size; _} = t in
    let cs = Cstruct.create block_size in
    match Littlefs.Block.into_cstruct ~program_block_size cs data with
    | `Split_emergency -> Lwt.return @@ Error `Split_emergency
    | `Split -> begin
      This_Block.write block block_location [cs] >>= function
      | Error _ -> Lwt.return @@ Error `Split_emergency
      | Ok () -> Lwt.return @@ Error `Split
    end
    | `Ok ->
      This_Block.write block block_location [cs] >>= function
      | Error _ -> Lwt.return @@ Error `No_space
      | Ok () -> Lwt.return @@ Ok `Done

  let rec block_to_block_pair t data (b1, b2) =
    let split () =
      Lwt_result.both (Allocator.get_block t) (Allocator.get_block t) >>= function
      | Error _ -> Lwt.return @@ Error `No_space
      | Ok (a1, a2) -> begin
        (* it's not strictly necessary to order these,
         * but it makes it easier for the debugging human to "reason about" *)
        let old_block, new_block = split data ((min a1 a2), (max a1 a2)) in
        Lwt_result.both
          (block_to_block_pair t old_block (b1, b2))
          (block_to_block_pair t new_block (a1, a2)) >>= function
        | Error `Split | Error `Split_emergency ->
          Lwt.return @@ Error `No_space
        | Error _ as e -> Lwt.return e
        | Ok ((), ()) -> Lwt.return @@ Ok ()
      end
    in
    Lwt_result.both
      (block_to_block_number t data b1)
      (block_to_block_number t data b2)
    >>= function
    | Ok _ -> Lwt.return @@ Ok ()
        (* `Split happens when the write did succeed, but a split operation
         * needs to happen to provide future problems *)
    | Error `Split -> begin
        (* try a compaction first *)
        Lwt_result.both
          (block_to_block_number t (Littlefs.Block.compact data) b1)
          (block_to_block_number t (Littlefs.Block.compact data) b2) 
        >>= function
        | Ok _ -> Lwt.return @@ Ok ()
        | Error `Split | Error `Split_emergency -> split ()
        | Error `No_space -> Lwt.return @@ Error `No_space
      end
    | Error `Split_emergency -> split ()
    | Error `No_space -> Lwt.return @@ Error `No_space

  let rec entries_following_hard_tail t (block_pair : int64 * int64) =
    block_of_block_pair t block_pair >>= function
    | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v "hard_tail"))
    | Ok block ->
      let this_blocks_entries = Littlefs.Block.entries block in
      match List.filter_map Littlefs.Dir.hard_tail_links this_blocks_entries with
      | [] -> Lwt.return @@ Ok this_blocks_entries
      | nextpair::_ ->
        entries_following_hard_tail t nextpair >>= function
        | Ok entries -> Lwt.return @@ Ok (this_blocks_entries @ entries)
        | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v "hard_tail"))

  let entries_of_name t block_pair name =
    let entries_of_id entries id =
      let matches (tag, _) =
        0 = compare tag.Littlefs.Tag.id id
      in
      List.find_all matches entries
    in
    let id_of_key entries key =
      let data_matches c =
        0 = String.(compare key @@ Cstruct.to_string c)
      in
      let tag_matches t = Littlefs.Tag.(fst t.type3 = LFS_TYPE_NAME)
      in
      match List.find_opt (fun (tag, data) ->
          tag_matches tag && data_matches data
        ) entries  with
    | Some (tag, _) -> Some tag.Littlefs.Tag.id
    | None -> None
    in
    entries_following_hard_tail t block_pair >>= function
    | Error _ as e -> Lwt.return e
    | Ok entries ->
      match id_of_key entries name with
      | None -> Lwt.return @@ Error `No_id
      | Some id ->
        match entries_of_id entries id with
        | []-> Lwt.return @@ Error `No_struct
        | entries -> Lwt.return @@ Ok (Littlefs.Entry.compact entries)

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
      entries_of_name t block_pair key >>= function
      | Error _ -> Lwt.return @@ `No_id key
      | Ok l ->
        match List.filter_map Littlefs.Dir.of_entry l with
        | [] -> Lwt.return `No_structs
        | next_blocks::_ -> Lwt.return (`Continue (remaining, next_blocks))

  let rec find_directory_block_pair t block_pair key =
    follow_directory_pointers t block_pair key >>= function
    | `Continue (path, next_blocks) -> find_directory_block_pair t next_blocks path
    | `No_id child ->
      let path = String.concat "/" key in
      let path = Mirage_kv.Key.(to_string @@ v path / child) in
      Lwt.return @@ `No_id path
    | `No_structs -> Lwt.return `No_structs
    | `Basename_on block_pair ->
      Traversal.last_block t block_pair >|= function
      | Error _ -> `Basename_on block_pair
      | Ok last_pair -> `Basename_on last_pair

  (* `dirname` is the name of the directory relative to `rootpair`. It should be
   * a value that could be returned from `Mirage_kv.Key.basename` - in other words
   * it should contain no separators. *)
  let plain_mkdir t rootpair (dirname : string) =
    follow_directory_pointers t rootpair [dirname] >>= function
    | `Continue (_path, next_blocks) -> Lwt.return @@ Ok next_blocks
    | `Basename_on next_blocks -> Lwt.return @@ Ok next_blocks
    | _ ->
      Allocator.get_block t >>= function
      | Error _ -> Lwt.return @@ Error (`Not_found dirname)
      | Ok dir_block_0 ->
        Allocator.get_block t >>= function
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
      entries_of_name t block key >>= function
      | Error `No_struct -> Lwt.return @@ `No_entry
      | Error _ -> Lwt.return @@ `No_id key
      | Ok l ->
        match List.filter_map Littlefs.Dir.of_entry l with
        | [] -> Lwt.return `No_structs
        | next_blocks::_ -> find_directory t next_blocks remaining

  let list_block entries =
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
    List.filter_map info_of_entry entries

  let list t key : ((string * [`Dictionary | `Value]) list, error) result Lwt.t =
    match (Mirage_kv.Key.segments key) with
    | [] -> begin
      entries_following_hard_tail t root_pair >>= function
      | Error _ -> Lwt.return @@ Error (`Not_found key)
      | Ok entries -> Lwt.return @@ Ok (list_block entries)
    end
    | segments ->
      find_directory t root_pair segments >>= function
      | `No_id k -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v k))
      | `No_structs | `No_entry | `Bad_pointer ->
        Lwt.return @@ Error (`Not_found key)
      | `Basename_on pair ->
        entries_following_hard_tail t pair >>= function
        | Error _ -> Lwt.return @@ Error (`Not_found key)
        | Ok entries -> Lwt.return @@ Ok (list_block entries)

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

  let get_value t block_pair key =
    entries_of_name t block_pair key >|= function
    | Error _ -> Error (`Not_found key)
    | Ok l ->
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
    let map_errors = function
        | Ok (`Inline d) -> Lwt.return (Ok d)
        | Ok (`Ctz ctz) -> get_ctz t key ctz
        | Error _ -> Lwt.return @@ Error (`Not_found key)
    in
    match Mirage_kv.Key.segments key with
    | [] -> Lwt.return @@ Error (`Value_expected key)
    | basename::[] -> begin
        get_value t root_pair basename >>= map_errors
      end
    | _ ->
      let dirname = Mirage_kv.Key.(parent key |> segments) in
      find_directory t root_pair dirname >>= function
      | `Basename_on pair -> begin
        get_value t pair (Mirage_kv.Key.basename key) >>= map_errors
        end
      | _ -> Lwt.return @@ Error (`Not_found key)

  let rec write_ctz_block t l index so_far data =
    if Int.compare so_far (String.length data) >= 0 then begin
      (* we purposely don't reverse the list because we're going to want
       * the *last* block for inclusion in the ctz structure *)
      Lwt.return @@ Ok l
    end else begin
      Allocator.get_block t >>= function
      | Error _ -> Lwt.return @@ Error `No_space
      | Ok block_number ->
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
      | Ok ((_last_index, last_pointer)::_) ->
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
    | `Basename_on block_pair ->
      set_in_directory block_pair t (Mirage_kv.Key.basename key) data
    | `No_id path -> begin
      mkdir t root_pair (Mirage_kv.Key.segments dir) >>= function
      | Error _ -> Lwt.return @@ (Error (`Not_found (Mirage_kv.Key.v path)))
      | Ok block_pair ->
        set_in_directory block_pair t (Mirage_kv.Key.basename key) data
      end
    | _ -> Lwt.return @@ Error (`Not_found key)

  let connect device ~program_block_size ~block_size : (t, error) result Lwt.t =
    This_Block.connect ~block_size device >>= fun block ->
    (* TODO: setting an empty lookahead to generate a good-enough `t`
     * to populate the lookahead buffer
     * feels very kludgey and error-prone. We should either
     * make the functions that don't need allocable blocks marked
     * in the type system somehow,
     * or have them only take the arguments they need instead of a full `t` *)
    let t = {block; block_size; program_block_size; lookahead = ref (`Before, [])} in
    Traversal.follow_links t (Littlefs.Entry.Metadata root_pair) >>= function
    | Error _e -> Lwt.fail_with "couldn't get list of used blocks"
    | Ok used_blocks ->
      let open Allocator in
      let lookahead = ref (`After, unused ~bias:`Before t used_blocks) in
      Lwt.return @@ Ok {lookahead; block; block_size; program_block_size}

  let format t =
    let program_block_size = t.program_block_size in
    let block_size = t.block_size in
    let write_whole_block n b = This_Block.write t.block n
        [fst @@ Littlefs.Block.to_cstruct ~program_block_size ~block_size b]
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
end
