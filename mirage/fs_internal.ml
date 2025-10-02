open Lwt.Infix

module Make(Shimmed_block : Block_ops.Block_shim) = struct
  type lookahead = {
    offset : int;
    blocks : int64 list ;
  }
  type t = {
    block : Shimmed_block.t;
    block_size : int;
    program_block_size : int;
    lookahead : lookahead ref;
    file_size_max : Cstruct.uint32;
    name_length_max : Cstruct.uint32;
    new_block_mutex : Lwt_mutex.t;
  }
  type blockpair = int64 * int64
  type directory_head = blockpair
  type key = Mirage_kv.Key.t

  type write_result = [ `No_space | `Split | `Split_emergency ]
  (* error type definitions straight outta mirage-kv *)
  type error = [
    | `Not_found           of key (** key not found *)
    | `Dictionary_expected of key (** key does not refer to a dictionary. *)
    | `Value_expected      of key (** key does not refer to a value. *)
  ]
  type write_error = [
    | error
    | `No_space                (** No space left on the device. *)
    | `Already_present of key (** The key is already present. *)
    | `Rename_source_prefix of key * key (** The source is a prefix of destination in rename. *)
  ]

  let root_pair = (0L, 1L)
  let pp_blockpair = Fmt.(pair ~sep:comma int64 int64)

  let log_src = Logs.Src.create "chamelon-fs-internal" ~doc:"chamelon FS internal layer"
  module Log = (val Logs.src_log log_src : Logs.LOG)

  module Read = struct

    (* get the wodge of data at this block number, and attempt to parse it *)
    let block_of_block_number {block_size; block; program_block_size; _} block_location =
      let cs = Cstruct.create block_size in
      Shimmed_block.read block block_location [cs] >>= function
      | Error b -> Lwt.return @@ Error (`Block b)
      | Ok () ->
        match Chamelon.Block.of_cstruct ~program_block_size cs with
        | Error (`Msg s) ->
          Log.err (fun f -> f "error reading block %Ld : %s"
                      block_location s);
          Lwt.return @@ Error (`Chamelon `Corrupt)
        | Ok extant_block -> Lwt.return @@ Ok extant_block

    (* get the blocks at pair (first, second), parse them, and return whichever is more recent *)
    let block_of_block_pair t (l1, l2) =
      let open Lwt_result.Infix in
      Lwt_result.both (block_of_block_number t l1) (block_of_block_number t l2) >>= fun (b1, b2) ->
      Lwt.return @@ Ok (Chamelon.Block.latest b1 b2)

  end

  module Find : sig
    type blockwise_entry_list = blockpair * (Chamelon.Entry.t list)

    (** [all_entries_in_dir t head] gives an *uncompacted* list of all
     * entries in the directory starting at [head].
     * the returned entries in the directory are split up by block,
     * so the caller can distinguish between re-uses of the same ID number
     * when the directory spans multiple block numbers *)
    val all_entries_in_dir : t -> directory_head ->
      (blockwise_entry_list list, [ `Not_found of key ]) result Lwt.t

    (** [entries_of_name t head name] scans [head] (and any subsequent blockpairs in the directory's
     * hardtail list) for `id` entries matching [name]. If an `id` is found for [name],
     * all entries matching `id` from the directory are returned (compacted). *)
    val entries_of_name : t -> directory_head -> string -> (blockwise_entry_list list,
                                                           [`No_id of string
                                                           | `Not_found of key]
                                                          ) result Lwt.t

    (** [find_first_blockpair_of_directory t head l] finds and enters
     *  the segments in [l] recursively until none remain.
     *  It returns [`No_id s] if an entry for [s] is not present,
     *  [`Not_directory s] if an entry for [s] is present,
     *  but does not represent a valid directory. *)
    val find_first_blockpair_of_directory : t -> directory_head -> string list ->
      [ `Final_dir_on of directory_head
      | `No_id of string
      | `Not_directory of string ] Lwt.t

    val get_directory_head : t -> directory_head -> string -> (directory_head option, 
                                                               [ `Not_found of key ]
                                                              ) result Lwt.t

  end = struct
    type blockwise_entry_list = blockpair * (Chamelon.Entry.t list)

    (* nb: all does mean *all* here; the list is returned uncompacted,
     * so the caller may have to compact to avoid reporting on expired state *)
    let rec all_entries_in_dir t block_pair =
      Read.block_of_block_pair t block_pair >>= function
      | Error _ -> Lwt.return @@ Error (`Not_found Mirage_kv.Key.empty)
      | Ok block ->
        let this_blocks_entries = Chamelon.Block.entries block in
        match Chamelon.Block.hardtail block with
        | None -> Lwt.return @@ Ok [(block_pair, this_blocks_entries)]
        | Some nextpair ->
          all_entries_in_dir t nextpair >>= function
          | Ok entries -> Lwt.return @@ Ok ((block_pair, this_blocks_entries) :: entries)
          | Error _ -> Lwt.return @@ Error (`Not_found Mirage_kv.Key.empty)

    let entries_of_name t block_pair name =
      let entries_of_id entries id =
        let matches (tag, _chunk) = (0 = compare tag.Chamelon.Tag.id id &&
                                tag.Chamelon.Tag.length < Chamelon.Tag.Magic.deleted_tag)
        in
        List.find_all matches entries
      in
      let id_of_key entries key =
        let module DataMap = Map.Make(Cstruct) in
        let data_matches c = 0 = String.(compare key @@ Cstruct.to_string c) in
        let tag_matches t =
          Chamelon.Tag.(fst t.type3 = LFS_TYPE_NAME) &&
          Chamelon.Tag.(snd t.type3 = 0x01 || snd t.type3 = 0x02) &&
          Chamelon.Tag.(t.length != Magic.deleted_tag)
        in
        let map = DataMap.empty in
        (* we need to be sure to grab the *last* matching tag from the entry set *)
        let map =
          List.fold_left (fun map (tag, data) ->
              if tag_matches tag && data_matches data then
                DataMap.add data tag.Chamelon.Tag.id map
              else map
            ) map entries
        in
        DataMap.find_opt (Cstruct.of_string key) map
      in
      let open Lwt_result in
      all_entries_in_dir t block_pair >>= fun entries_by_block ->
      let entries_matching_name (block, entries) =
        match id_of_key (Chamelon.Entry.compact entries) name with
        | None ->
          Log.debug (fun m -> m "id for %S not found in %d entries from %a"
                         name (List.length entries) pp_blockpair block);
          Error (`No_id name)
        | Some id ->
          Log.debug (fun m -> m "name %S is associated with id %d on blockpair %a"
                         name id pp_blockpair block);
          let entries = entries_of_id entries id in
          Log.debug (fun m -> m "found %d entries for id %d in %a"
                         (List.length entries) id pp_blockpair block);
          let tags = List.map fst entries in
          Log.debug (fun m -> m "here are the tags for the entries for id %d: %a"
                        id
                        Fmt.(list Chamelon.Tag.pp) tags);
          let compacted = Chamelon.Entry.compact entries in
          Log.debug (fun m -> m "after compaction, %d entries for id %d in %a"
                         (List.length compacted) id pp_blockpair block);
          Ok (block, compacted)
      in
      let blockwise_matches = List.filter_map (fun es ->
          match entries_matching_name es with
          | Ok (_, []) | Error _ -> None
          | Ok (block, l) -> Some (block, l)
        ) entries_by_block in
      Lwt.return (Ok blockwise_matches)

    let get_directory_head t block_pair name =
      entries_of_name t block_pair name >>= function
      | Error _ as e -> Lwt.return e
      | Ok l ->
        let get_dirs = List.filter_map Chamelon.Dir.of_entry in
        match List.map (fun (_block, dirs) -> get_dirs dirs) l |> List.flatten with
        | [] -> Lwt.return @@ Ok None
        | a::[] -> Lwt.return @@ Ok (Some a)
        | l ->
          let last = List.rev l |> List.hd in
          Log.debug (fun f -> f "directory at %a had multiple directory entries for key %s. choosing the last one, which points to %a"
                        pp_blockpair block_pair name pp_blockpair last);
          Lwt.return @@ Ok (Some last)

    let rec find_first_blockpair_of_directory t block_pair key =
      match key with
      | [] -> Lwt.return @@ `Final_dir_on block_pair
      | next::remaining ->
        get_directory_head t block_pair next >>= function
        | Ok None -> Lwt.return @@ `No_id next
        | Ok (Some next_directory_head)  ->
          find_first_blockpair_of_directory t next_directory_head remaining
        | Error _e -> Lwt.return @@ `No_id next

  end
  module Traverse : sig
    val follow_links : t -> blockpair list -> Chamelon.Entry.link ->
      (int64 list, Shimmed_block.error) result Lwt.t

    val last_block : t -> blockpair -> (blockpair, 
                                       [`Block of Shimmed_block.error |
                                        `Chamelon of [> `Corrupt] ]) result Lwt.t

  end = struct
    let rec get_ctz_pointers t l index pointer =
      match l, index with
      | Error _ as e, _ -> Lwt.return e
      | Ok l, 1 -> Lwt.return @@ Ok (pointer :: l)
      | Ok l, index ->
        let open Lwt_result.Infix in
        let data = Cstruct.create t.block_size in
        Shimmed_block.read t.block pointer [data] >>= fun ()->
        let pointers, _data_region = Chamelon.File.of_block index data in
        match pointers with
        | [] -> Lwt.return @@ Ok (pointer::l)
        | next::_ -> get_ctz_pointers t (Ok (pointer::l)) (index - 1) (Int64.of_int32 next)

    let rec follow_links t visited = function
      | Chamelon.Entry.Data (pointer, length) -> begin
          let file_size = Int32.to_int length in
          let index = Chamelon.File.last_block_index ~file_size ~block_size:t.block_size in
          Log.debug (fun f -> f "data block: last block index %d found starting at %ld (0x%lx)" index pointer pointer);
          get_ctz_pointers t (Ok []) index (Int64.of_int32 pointer)
        end
      | Chamelon.Entry.Metadata (a, b) ->
        match List.mem (a, b) visited with
        | true -> Log.err (fun f -> f "cycle detected: blockpair %a encountered again after initial visit." pp_blockpair (a, b));
          Lwt.return @@ Error `Disconnected
        | false ->
          Read.block_of_block_pair t (a, b) >>= function
          | Error (`Block e) ->
            Log.err (fun m -> m
                        "block-level error reading block pair %Ld, %Ld (0x%Lx, 0x%Lx): %a"
                        a b a b Shimmed_block.pp_error e);
            Lwt.return @@ Error e
          | Error (`Chamelon `Corrupt) ->
            Log.err (fun f -> f
                        "filesystem seems corrupted; we couldn't make sense of a block pair");
            Lwt.return @@ Error `Disconnected
          | Ok block ->
            Log.debug (fun f -> f "finding blocks linked from %a" pp_blockpair (a, b));
            let links = Chamelon.Block.linked_blocks block in
            Log.debug (fun f -> f "found %d linked blocks" (List.length links));
            Lwt_list.fold_left_s (fun so_far link ->
                match so_far with
                | Error _ as e -> Lwt.return e
                | Ok l ->
                  follow_links t ((a, b)::visited) link >>= function
                  | Error e ->
                    Log.err (fun f -> f
                                "FILESYSTEM CORRUPTION. error following outbound links \
                                 from blockpair %a: %a"
                                pp_blockpair (a, b)
                                Shimmed_block.pp_error e);
                    Lwt.return @@ Error `Disconnected
                  | Ok new_links -> Lwt.return @@ Ok (new_links @ l)
              ) (Ok []) links
            >>= function
            | Ok list -> Lwt.return @@ Ok (a :: b :: list)
            | e -> Lwt.return e

    (* [last_block t pair] returns the last blockpair in the hardtail
     * linked list starting at [pair], which may well be [pair] itself *)
    (* write operations should target the returned blockpair *)
    let rec last_block t pair =
      let open Lwt_result.Infix in
      Read.block_of_block_pair t pair >>= fun block ->
      match List.find_opt (fun e ->
          Chamelon.Tag.is_hardtail (fst e)
        ) (Chamelon.Block.entries block) with
      | None -> Lwt.return @@ Ok pair
      | Some entry -> match Chamelon.Dir.hard_tail_links entry with
        | None -> Lwt.return @@ Ok pair
        | Some next_pair -> last_block t next_pair
  end
  module Allocate = struct
    let unused t used_blocks =
      let module IntSet = Set.Make(Int64) in
      let actual_blocks = Shimmed_block.block_count t.block in
      let prev_offset = !(t.lookahead).offset in
      let alloc_size = max 16 ((min 256 actual_blocks) / 4) in
      let this_offset =
        if (prev_offset + alloc_size) >= Shimmed_block.block_count t.block then 0
        else prev_offset + alloc_size
      in
      let pool = IntSet.of_list @@ List.init alloc_size
          (fun a -> Int64.of_int @@ a + this_offset)
      in
      this_offset, IntSet.(elements @@ diff pool (of_list used_blocks))

    let populate_lookahead ~except t =
      Traverse.follow_links t [] (Chamelon.Entry.Metadata root_pair) >|= function
      | Error e ->
        Log.err (fun f -> f "error attempting to find unused blocks: %a" Shimmed_block.pp_error e);
        Error `No_space
      | Ok used_blocks ->
        Log.debug (fun f -> f "%d blocks used: %a" (List.length used_blocks) Fmt.(list ~sep:sp uint64) used_blocks);
        Ok (unused t (used_blocks @ except))

    let get_blocks t n : (int64 list, write_error) result Lwt.t =
      let get_block t =
        let l = !(t.lookahead).blocks in
        t.lookahead := {!(t.lookahead) with blocks = (List.tl l)};
        Lwt.return @@ Ok (List.hd l)
      in
      (* zero or fewer blocks is a pretty easy request to fulfill *)
      if n <= 0 then Lwt.return @@ Ok []
      else begin
        let rec aux t acc n =
          match !(t.lookahead).blocks with
          (* if we have exactly enough blocks, just return the whole list *)
          | l when List.length l = n ->
            t.lookahead := {!(t.lookahead) with blocks = []};
            Lwt.return @@ Ok (l @ acc)
          (* if we have enough in the lookahead buffer, just grab the first one n times *)
          | l when List.length l > n -> begin
              let l = List.init n (fun a -> a) in
              Lwt_list.fold_left_s (fun acc _ ->
                  match acc with | Error _ as e -> Lwt.return e
                                 | Ok l -> begin
                                     get_block t >>= function
                                     | Error _ as e -> Lwt.return e
                                     | Ok b -> Lwt.return @@ Ok (b::l)
                                   end
                ) (Ok acc) l
            end
          | l ->
            (* this is our sad case: not enough blocks in the lookahead allocator
                    to satisfy the request.
               Claim the blocks that are there already, and try to get more;
               if the allocator can't give us any, give up *)
            let open Lwt_result.Infix in
            populate_lookahead ~except:(l @ acc) t >>= function
            | _, [] ->
              Log.err (fun f -> f "no blocks remain free on filesystem");
              Lwt.return @@ Error `No_space
            | new_offset, next_l ->
              t.lookahead := ({offset = new_offset; blocks = next_l});
              Log.debug (fun f -> f "adding %d blocks to lookahead buffer (%a)" (List.length next_l) Fmt.(list ~sep:comma int64) next_l);
              aux t (l @ acc) (n - (List.length l))
        in
        aux t [] n
      end

    (* [get_block_pair fs] wraps [get_blocks fs 2] to return a pair for the caller's convenience *)
    let get_block_pair t =
      get_blocks t 2 >|= function
      | Ok (block1::block2::_) -> Ok (block1, block2)
      | Ok _ | Error _ -> Error `No_space

  end
  module Write = struct
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
    let block_to_block_number t data block_location : (unit, write_result) result Lwt.t =
      let block_device = t.block in
      Shimmed_block.write block_device block_location [data] >>= function
      | Ok () -> Lwt.return @@ Ok ()
      | Error e ->
        Log.err (fun m -> m "block write error: %a" Shimmed_block.pp_write_error e);
        Lwt.return @@ Error `No_space

    let rec block_to_block_pair t data (b1, b2) : (unit, write_result) result Lwt.t =
      let split data  =
        Allocate.get_block_pair t >>= function
        | Error `No_space -> Lwt.return @@ Error `No_space
        | Ok (new_block_1, new_block_2) when Int64.equal new_block_1 new_block_2 ->
          (* if there is only 1 block remaining, we'll get the same one twice.
           * That's not enough for the split. *)
          Lwt.return @@ Error `No_space
        | Ok (new_block_1, new_block_2) -> begin
            Log.debug (fun m -> m "splitting block pair %Ld, %Ld to %Ld, %Ld"
                           b1 b2 new_block_1 new_block_2);
            let old_block, new_block = Chamelon.Block.split data (new_block_1, new_block_2) in
            Log.debug (fun m -> m "keeping %d entries in the old block, and putting %d in the new one"
                           (List.length @@ Chamelon.Block.entries old_block)
                           (List.length @@ Chamelon.Block.entries new_block));
            (* be very explicit about writing the new block first, and only overwriting
             * the old block pair if the new block pair write succeeded *)
            block_to_block_pair t new_block (new_block_1, new_block_2) >>= function
            | Error `Split | Error `Split_emergency | Error `No_space ->
              Lwt.return @@ Error `No_space
            | Ok () -> begin
                Log.debug (fun f -> f "wrote new pair; overwriting old pair");
                (* ignore any warnings about needing to split, etc *)
                let cs1 = Cstruct.create t.block_size in
                let serialize = Chamelon.Block.into_cstruct ~program_block_size:t.program_block_size in
                match serialize cs1 old_block with
                | `Unwriteable ->
                  Log.err (fun f -> f
                              "unwriteable block during split operation: %a"
                              Chamelon.Block.pp old_block);
                  Lwt.return (Error `No_space)
                | `Split | `Split_emergency | `Ok ->
                  Lwt_result.both
                    (block_to_block_number t cs1 b1)
                    (block_to_block_number t cs1 b2) >>= function
                  | Ok ((), ()) -> Lwt.return @@ Ok ()
                  | Error _ as e -> Lwt.return e
              end
          end
      in
      let cs1 = Cstruct.create t.block_size in
      let serialize = Chamelon.Block.into_cstruct ~program_block_size:t.program_block_size in
      match serialize cs1 data with
      | `Ok -> begin
        Lwt_result.both
          (block_to_block_number t cs1 b1)
          (block_to_block_number t cs1 b2)
        >>= function
        | Ok _ -> Lwt.return @@ Ok ()
        | Error _ as e -> Lwt.return e
      end
      | `Split | `Split_emergency | `Unwriteable ->
        (* try a compaction first *)
        Cstruct.memset cs1 0x00;
        let compacted = Chamelon.Block.compact data in
        Log.debug (fun m -> m "split requested for a block with %d entries. compacted, we had %d"
                       (List.length @@ Chamelon.Block.entries data)
                       (List.length @@ Chamelon.Block.entries compacted)
                   );
        Log.debug (fun m -> m "@[entries pre-compaction:@ @[%a@]@]"
                      Fmt.(list ~sep:comma Chamelon.Entry.pp) (Chamelon.Block.entries data));
        Log.debug (fun m -> m "@[entries post-compaction:@ @[%a@]@]"
                      Fmt.(list ~sep:comma Chamelon.Entry.pp) (Chamelon.Block.entries compacted));
        match serialize cs1 compacted, Chamelon.Block.hardtail compacted with
        | `Ok, _ -> begin
            Log.debug (fun f -> f "compaction was sufficient. Will not split %a" pp_blockpair (b1, b2));
            Lwt_result.both
              (block_to_block_number t cs1 b1)
              (block_to_block_number t cs1 b2)
            >>= function
            | Ok _ -> Lwt.return @@ Ok ()
            | Error _ as e -> Lwt.return e
          end
        | `Split, None | `Split_emergency, None -> begin
            Log.debug (fun f -> f "compaction was insufficient and the block has no hardtail. Splitting %a" pp_blockpair (b1, b2));
            split compacted
          end
        | `Split, _ -> begin
            Log.debug (fun f -> f "split needed, but the block's already split. Writing the compacted block");
            Lwt_result.both
              (block_to_block_number t cs1 b1)
              (block_to_block_number t cs1 b2)
            >>= function
            | Ok _ -> Lwt.return @@ Ok ()
            | Error _ as e -> Lwt.return e
          end
        | `Split_emergency, _  | `Unwriteable, _ ->
          Log.err (fun f -> f "Couldn't write to block %a" pp_blockpair (b1, b2));
          Lwt.return @@ Error `No_space
  end
  module Delete = struct
    let delete_in_directory directory_head t name =
      Find.entries_of_name t directory_head name >>= function
        (* several "it's not here" cases *)
      | Error (`No_id _) | Error (`Not_found _) ->
        Log.debug (fun m -> m "no id or nothing found for %s" name);
        Lwt.return @@ Ok ()
      | Ok [] | Ok ((_,[])::_) ->
        Log.debug (fun m -> m "no entries on %a for %s"
                       pp_blockpair directory_head name);
        Lwt.return @@ Ok ()
      | Ok ((blockpair_with_id, hd::_tl)::_) ->
        let id = Chamelon.Tag.((fst hd).id) in
        Log.debug (fun m -> m "id %d found for name %s on block %a" id name
                   pp_blockpair blockpair_with_id);
        Read.block_of_block_pair t blockpair_with_id >>= function
        | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v name))
        | Ok block ->
          Log.debug (fun m -> m "adding deletion for id %d on block pair %a"
                         id pp_blockpair blockpair_with_id);
          let deletion = Chamelon.Tag.delete id in
          let new_block = Chamelon.Block.add_commit block [(deletion, Cstruct.empty)] in
          Write.block_to_block_pair t new_block blockpair_with_id >>= function
          | Error _ -> Lwt.return @@ Error `No_space
          | Ok () -> Lwt.return @@ Ok ()

  end

  let mkdir_nonrec t containing_directory_head new_directory_name =
    (* first, make sure this name isn't already in this directory *)
    Find.entries_of_name t containing_directory_head new_directory_name >>= function
    | Ok (hd::tl) -> begin
      (* do any of the entries represent a valid directory entry? if so, we don't need to do anything *)
      (* TODO: the blockwise entry list returned by `entries_of_name` is pretty annoying to work with *)
      let get_dirs = List.filter_map Chamelon.Dir.of_entry in
      let dirs = List.map (fun (_block, dirs) -> get_dirs dirs) (hd::tl) |> List.flatten in
      match List.rev dirs with
      | pair::_ -> Lwt.return (Ok pair)
      | _ -> Lwt.return @@ Error (`Already_present new_directory_name)
    end
    | Error (`No_id _) | Error (`Not_found _) | Ok [] ->
      Traverse.last_block t containing_directory_head >>= function
      | Error (`Chamelon (`Corrupt)) ->
        Log.err (fun f -> f "FS CORRUPTION SUSPECTED: mkdir attempt failed. it was not possible to find the last block \
                             in the directory whose head is at %a"
                    pp_blockpair containing_directory_head);
        Lwt.return @@ Error `No_space
      | Error (`Block e) ->
        Log.err (fun f -> f "block-level error encountered attempting to find last block in the directory whose head is at %a: %a"
                    pp_blockpair containing_directory_head Shimmed_block.pp_error e);
        Lwt.return @@ Error `No_space
      | Ok last_pair_in_containing_directory ->
        (* create a new entry for a directory `new_directory_name` in `pair` *)
        Log.debug (fun f -> f "found last pair %a in directory starting at %a"
                      pp_blockpair last_pair_in_containing_directory
                      pp_blockpair containing_directory_head);
        (* we expect to alter the structure of last_pair_in_containing_directory,
         * so make sure we can make sense of it to begin with *)
        Read.block_of_block_pair t last_pair_in_containing_directory >>= function
        | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v new_directory_name))
        | Ok containing_directory_last_block_content ->
          (* we need a block pair on which the new directory will have its head *)
          (* since we need to hold on to these blocks until they're reachable from the root blockpair,
           * this whole thing needs to be within the `new_block_mutex` *)
          Lwt_mutex.with_lock t.new_block_mutex @@ fun () -> begin
            Allocate.get_block_pair t >>= function
            | Error _ -> Lwt.return @@ Error (`No_space)
            | Ok new_pair ->
              (* first, write empty commits to the new blockpair; if that fails,
               * we want to bail before making any more structure
               * point anyway *)
              Write.block_to_block_pair t (Chamelon.Block.of_entries ~revision_count:1l []) new_pair >>= function
              | Error _ ->
                Log.err (fun f -> f "error initializing a directory at a fresh block pair %a"
                            pp_blockpair new_pair);
                Lwt.return @@ Error `No_space
              | Ok () ->
                (* finally, we're ready to add our new directory to the contents of the containing directory *)
                let extant_ids = Chamelon.Block.ids containing_directory_last_block_content in
                (* TODO: it's definitely possible to manipulate this such that you overflow *)
                let new_dir_id = match Chamelon.Block.IdSet.max_elt_opt extant_ids with
                  | None -> 1
                  | Some s -> s + 1
                in
                let name = Chamelon.Dir.name new_directory_name new_dir_id in
                let dirstruct = Chamelon.Dir.mkdir ~to_pair:new_pair new_dir_id in
                let containing_directory_updated_last_block =
                  Chamelon.Block.add_commit containing_directory_last_block_content [name; dirstruct]
                in
                Write.block_to_block_pair t
                  containing_directory_updated_last_block last_pair_in_containing_directory
                >>= function
                | Error _ -> Lwt.return @@ Error `No_space
                | Ok () -> Lwt.return @@ Ok new_pair
          end

  let rec get_directory ~ensure_path t root segments =
    match segments with
    | [] -> Lwt.return @@ Ok root
    | next::l ->
      if ensure_path then begin
        (* `mkdir_nonrec` will return (Ok directory_head) if the directory already exists *)
        mkdir_nonrec t root next >>= function
          | Error _ as e -> Lwt.return e
          | Ok dir_head -> get_directory ~ensure_path t dir_head l
        end else begin
        (* in this case we shouldn't make it if it's not already there, so just go look for it *)
        Find.get_directory_head t root next >>= function
        | Error (`Not_found s) -> Lwt.return @@ Error (`Not_found s)
        | Ok None -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v next))
        | Ok (Some dir_head) -> get_directory ~ensure_path t dir_head l
      end
end
