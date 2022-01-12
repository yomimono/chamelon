let root_pair = (0L, 1L)

open Lwt.Infix

module Make(Sectors: Mirage_block.S)(Clock : Mirage_clock.PCLOCK) = struct
  module This_Block = Block_ops.Make(Sectors)
  type t = {
    block : This_Block.t;
    block_size : int;
    program_block_size : int;
    lookahead : ([`Before | `After ] * (int64 list)) ref;
    new_block_mutex : Lwt_mutex.t;
  }

  type key = Mirage_kv.Key.t

  let log_src = Logs.Src.create "chamelon-fs" ~doc:"chamelon FS layer"
  module Log = (val Logs.src_log log_src : Logs.LOG)

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

  module Read = struct

    (* get the wodge of data at this block number, and attempt to parse it *)
    let block_of_block_number {block_size; block; program_block_size; _} block_location =
      let cs = Cstruct.create block_size in
      This_Block.read block block_location [cs] >>= function
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
      if Chamelon.Block.(compare (revision_count b1) (revision_count b2)) < 0
      then Lwt.return @@ Ok b2
      else Lwt.return @@ Ok b1
          
  end

  module Traverse = struct
    let rec get_ctz_pointers t l index pointer =
      match l, index with
      | Error _ as e, _ -> Lwt.return e
      | Ok l, 0 -> Lwt.return @@ Ok l
      | Ok l, index ->
        let open Lwt_result.Infix in
        let data = Cstruct.create t.block_size in
        This_Block.read t.block pointer [data] >>= fun ()->
        let pointers, _data_region = Chamelon.File.of_block index data in
        match pointers with
        | [] -> Lwt.return @@ Ok (pointer::l)
        | next::_ -> get_ctz_pointers t (Ok (pointer::l)) (index - 1) (Int64.of_int32 next)

    let rec follow_links t = function
      | Chamelon.Entry.Data (pointer, length) -> begin
          let file_size = Int32.to_int length in
          let index = Chamelon.File.last_block_index ~file_size ~block_size:t.block_size in
          get_ctz_pointers t (Ok []) index (Int64.of_int32 pointer)
        end
      | Chamelon.Entry.Metadata (a, b) ->
        Read.block_of_block_pair t (a, b) >>= function
        | Error (`Block e) ->
          Logs.err (fun m -> m "error reading block pair %Ld, %Ld (0x%Lx, 0x%Lx): %a" a b a b This_Block.pp_error e);
          Lwt.return @@ Error `Disconnected
        | Error (`Chamelon `Corrupt) ->
          Logs.err (fun f -> f "filesystem seems corrupted; we couldn't make sense of a block pair");
          Lwt.return @@ Error `Unimplemented
        | Ok block ->
          let links = Chamelon.Block.linked_blocks block in
          Lwt_list.fold_left_s (fun so_far link ->
              match so_far with
              | Error _ as e -> Lwt.return e
              | Ok l ->
                follow_links t link >>= function
                | Error e ->
                  Logs.err (fun f -> f "filesystem seems corrupted; we couldn't get a list of unused blocks: %a" This_Block.pp_error e);
                  Lwt.return @@ Error `Unimplemented
                | Ok new_links -> Lwt.return @@ Ok (new_links @ l)
            ) (Ok []) links
          >>= function
          | Ok list -> Lwt.return @@ Ok (a :: b :: list)
          | e -> Lwt.return e

(* [last_block t pair] returns the last blockpair in the hardtail
 * linked list starting at [pair], which may well be [pair] itself *)
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
      let set = match bias, IntSet.split pivot candidates with
        | `After, (_, true, s) -> IntSet.add pivot s
        | `Before, (s, true, _)
        | `Before, (s, false, _)
        | `After, (_, false, s) -> s
      in
      Logs.debug (fun f -> f "filesystem has %d (0x%x) blocks. of these %d are used and %d available for immediate allocation" possible_blocks possible_blocks (List.length l1) (IntSet.cardinal set));
      IntSet.elements set

    let populate_lookahead t bias =
      Traverse.follow_links t (Chamelon.Entry.Metadata root_pair) >|= function
      | Error e ->
        Log.err (fun f -> f "error attempting to find unused blocks: %a" This_Block.pp_error e);
        Error `No_space
      | Ok used_blocks ->
        Ok (unused ~bias t used_blocks)

    let get_block t =
      match !(t.lookahead) with
      | bias, block::l ->
        t.lookahead := bias, l;
        Lwt.return @@ Ok block
      | bias, [] ->
        let open Lwt_result.Infix in
        populate_lookahead t bias >>= function
        | [] ->
          Log.err (fun f -> f "no blocks remain free on filesystem");
          Lwt.return @@ Error `No_space
        | block::l ->
          t.lookahead := (opp bias, l);
          Log.debug (fun f -> f "adding %d blocks to lookahead buffer (%a)" (List.length l) Fmt.(list int64) l);
          Lwt.return @@ Ok block

    (* this is a common enough case that it makes sense to handle it explicitly *)
    let get_block_pair t =
      match !(t.lookahead) with
      | bias, block1::block2::l ->
        t.lookahead := bias, l;
        Lwt.return @@ Ok (block1, block2)
      (* whether we have 1 block left or none, we still need to repopulate the lookahead buffer
       * so go just go ahead and do it first, and then take the first two blocks *)
      | bias, _ ->
        let open Lwt_result.Infix in
        populate_lookahead t bias >>= function
        | block1::block2::l ->
          t.lookahead := (opp bias, l);
          Log.debug (fun f -> f "adding %d blocks to lookahead buffer (%a)" (List.length l) Fmt.(list int64) l);
          Lwt.return @@ Ok (block1, block2)
        | l ->
          Log.debug (fun f -> f "%d unused blocks: %a" (List.length l) Fmt.(list int64) l);
          Log.err (fun f -> f "insufficient free blocks to allocate a new block pair");
          Lwt.return @@ Error `No_space

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
    let block_to_block_number t data block_location =
      let {block_size; block; program_block_size; _} = t in
      let cs = Cstruct.create block_size in
      Logs.debug (fun f -> f "writing block %Ld (0x%Lx)..." block_location block_location);
      match Chamelon.Block.into_cstruct ~program_block_size cs data with
      | `Split_emergency ->
        Logs.debug (fun f -> f "we need to split this block or we won't be able to write it");
        Lwt.return @@ Error `Split_emergency
      | `Split ->
        Logs.debug (fun f -> f "we need to split this, but not urgently");
        Lwt.return @@ Error `Split
      | `Ok ->
        This_Block.write block block_location [cs] >>= function
        | Ok () -> Lwt.return @@ Ok `Done
        | Error e ->
          Log.err (fun m -> m "block write error: %a" This_Block.pp_write_error e);
          Lwt.return @@ Error `No_space

    let rec block_to_block_pair t data (b1, b2) =
      let split () =
        Allocate.get_block_pair t >>= function
        | Error _ as e -> Lwt.return e
        | Ok (new_block_1, new_block_2) when Int64.equal new_block_1 new_block_2 ->
          (* if there is only 1 block remaining, we'll get the same one twice.
           * That's not enough for the split. *)
          Lwt.return @@ Error `No_space
        | Ok (new_block_1, new_block_2) -> begin
            Logs.debug (fun m -> m "splitting block pair %Ld, %Ld to %Ld, %Ld"
                           b1 b2 new_block_1 new_block_2);
            (* it's not strictly necessary to order these,
             * but it makes it easier for the debugging human to "reason about" *)
            let old_block, new_block = Chamelon.Block.split data (new_block_1, new_block_2) in
            Lwt_result.both
              (block_to_block_pair t old_block (b1, b2))
              (block_to_block_pair t new_block (new_block_1, new_block_2)) >>= function
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
          Logs.debug (fun m -> m "split required for block write to %Ld, %Ld" b1 b2);
          let compacted = Chamelon.Block.compact data in
          Logs.debug (fun m -> m "need to split %d entries. compacted, we had %d"
                         (List.length @@ Chamelon.Block.entries data)
                         (List.length @@ Chamelon.Block.entries compacted)
                     );
          (* try a compaction first *)
          if (List.length @@ Chamelon.Block.entries data) > (List.length @@ Chamelon.Block.entries compacted) then begin
            Logs.debug (fun m -> m "beginning compaction...");
            (* TODO compaction is the right point at which to attempt shedding the hardtail,
             * if possible. *)
            Lwt_result.both
              (block_to_block_number t compacted b1)
              (block_to_block_number t compacted b2) 
            >>= function
            | Ok _ ->
              Logs.debug (fun f -> f "compaction of the data was sufficient; not splitting");
              Lwt.return @@ Ok ()
            | Error `Split | Error `Split_emergency ->
              Logs.debug (fun f -> f "compaction was insufficient; splitting");
              split ()
            | Error `No_space -> Lwt.return @@ Error `No_space
          end else begin
            (* if we didn't drop any entries by compacting, don't bother and just split *)
            Logs.debug (fun f -> f "not compacting. Will begin splitting");
            split ()
          end
        end
      | Error `Split_emergency -> split ()
      | Error `No_space -> Lwt.return @@ Error `No_space
  end

  module Find : sig
    (** entries in the directory are split up by block,
     * so the caller can distinguish between re-uses of the same ID number
     * when the directory spans multiple block numbers *)
    val all_entries_in_dir : t -> int64 * int64 -> (Chamelon.Entry.t list list, error) result Lwt.t

    val entries_of_name : t -> int64 * int64 -> string -> (Chamelon.Entry.t list, 
                                                           [`No_id of key
                                                           | `Not_found of key]
                                                          ) result Lwt.t

    val find_directory : t -> int64 * int64 -> string list ->
      [`Basename_on of int64 * int64 | `No_entry | `No_id of string | `No_structs] Lwt.t

  end = struct

    (* nb: all does mean *all* here; the list is returned uncompacted,
     * so the caller may have to compact to avoid reporting on expired state *)
    let rec all_entries_in_dir t (block_pair : int64 * int64) =
      Read.block_of_block_pair t block_pair >>= function
      | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v "hard_tail"))
      | Ok block ->
        let this_blocks_entries = Chamelon.Block.entries block in
        match List.filter_map Chamelon.Dir.hard_tail_links this_blocks_entries with
        | [] -> Lwt.return @@ Ok [this_blocks_entries]
        | nextpair::_ ->
          all_entries_in_dir t nextpair >>= function
          | Ok entries -> Lwt.return @@ Ok (this_blocks_entries :: entries)
          | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v "hard_tail"))

    let entries_of_name t block_pair name =
      let entries_of_id entries id =
        let matches (tag, _) = 0 = compare tag.Chamelon.Tag.id id in
        List.find_all matches entries
      in
      let id_of_key entries key =
        let data_matches c = 0 = String.(compare key @@ Cstruct.to_string c) in
        let tag_matches t = Chamelon.Tag.(fst t.type3 = LFS_TYPE_NAME)
        in
        match List.find_opt (fun (tag, data) ->
            tag_matches tag && data_matches data
          ) entries with
        | Some (tag, _) -> Some tag.Chamelon.Tag.id
        | None -> None
      in
      let open Lwt_result in
      all_entries_in_dir t block_pair >>= fun entries_by_block ->
      let entries_matching_name entries =
        match id_of_key (Chamelon.Entry.compact entries) name with
        | None ->
          Logs.debug (fun m -> m "id for %s not found in %d entries from %Ld, %Ld"
                         name (List.length entries) (fst block_pair) (snd block_pair));
          Error (`No_id (Mirage_kv.Key.v name))
        | Some id ->
          Logs.debug (fun m -> m "found %d entries for id %d"
                         (List.length @@ entries_of_id entries id)
                         id);
          Ok (Chamelon.Entry.compact @@ entries_of_id entries id)
      in
      let blockwise_matches = List.filter_map (fun es ->
          match entries_matching_name es with
          | Ok [] | Error _ -> None
          | Ok l -> Some l
        ) entries_by_block in
      match List.rev blockwise_matches with
      | [] -> Lwt.return @@ Ok []
      | last_block_with_entries::_ -> Lwt.return @@ Ok last_block_with_entries

    let rec find_directory t block_pair key =
      match key with
      | [] -> begin
        (* if this block has a hardtail, follow it *)
        Read.block_of_block_pair t block_pair >>= function
        | Error _ ->
          Logs.debug (fun f -> f "couldn't read a block where we think a directory is present. Assuming it doesn't have a hardtail");
          Lwt.return (`Basename_on block_pair)
        | Ok block ->
          match Chamelon.Block.hardtail block with
          | None -> Lwt.return (`Basename_on block_pair)
          | Some next_blockpair -> find_directory t next_blockpair key
      end
      | key::remaining ->
        entries_of_name t block_pair key >>= function
        | Error _ -> Lwt.return @@ `No_id key
        | Ok l ->
          match List.filter_map Chamelon.Dir.of_entry l with
          | [] -> Lwt.return `No_structs
          | next_blocks::_ -> find_directory t next_blocks remaining

  end

  let rec mkdir t rootpair key =
    (* mkdir has its own function for traversing the directory structure
     * because we want to make anything that's missing along the way,
     * rather than having to get an error, mkdir, get another error, mkdir... *)
    let follow_directory_pointers t block_pair = function
      | [] -> Lwt.return (`Basename_on block_pair)
      | key::remaining ->
        Find.entries_of_name t block_pair key >>= function
        | Error _ -> Lwt.return @@ `Not_found key
        | Ok l ->
          match List.filter_map Chamelon.Dir.of_entry l with
          | [] -> Lwt.return `No_structs
          | next_blocks::_ -> Lwt.return (`Continue (remaining, next_blocks))
    in 
    (* `dirname` is the name of the directory relative to `rootpair`. It should be
     * a value that could be returned from `Mirage_kv.Key.basename` - in other words
     * it should contain no separators. *)
    let find_or_mkdir t rootpair (dirname : string) =
      follow_directory_pointers t rootpair [dirname] >>= function
      | `Continue (_path, next_blocks) -> Lwt.return @@ Ok next_blocks
      | `Basename_on next_blocks -> Lwt.return @@ Ok next_blocks
      | _ ->
        Lwt_mutex.with_lock t.new_block_mutex @@ fun () ->
        (* for any error case, try making the directory *)
        (* TODO: it's probably wise to put a delete entry first here if we got No_structs
         * or another "something weird happened" case *)
        Allocate.get_block_pair t >>= function
        | Error _ -> Lwt.return @@ Error (`No_space)
        | Ok (dir_block_0, dir_block_1) ->
          Read.block_of_block_pair t rootpair >>= function
          | Error _ -> Lwt.return @@ Error (`Not_found dirname)
          | Ok root_block ->
            let extant_ids = Chamelon.Block.ids root_block in
            let dir_id = match Chamelon.Block.IdSet.max_elt_opt extant_ids with
              | None -> 1
              | Some s -> s + 1
            in
            let name = Chamelon.Dir.name dirname dir_id in
            let dirstruct = Chamelon.Dir.mkdir ~to_pair:(dir_block_0, dir_block_1) dir_id in
            let new_block = Chamelon.Block.add_commit root_block [name; dirstruct] in
            Write.block_to_block_pair t new_block rootpair >>= function
            | Error _ -> Lwt.return @@ Error `No_space
            | Ok () -> Lwt.return @@ Ok (dir_block_0, dir_block_1)
    in
    match key with
    | [] -> Lwt.return @@ Ok rootpair
    | dirname::more ->
      let open Lwt_result in
      find_or_mkdir t rootpair dirname >>= fun newpair ->
      mkdir t newpair more

  module File_read : sig
    val get : t -> Mirage_kv.Key.t -> (string, error) result Lwt.t
  end = struct

    let get_ctz t key (pointer, length) =
      let rec read_block l index pointer =
        let data = Cstruct.create t.block_size in
        This_Block.read t.block pointer [data] >>= function
        | Error _ as e -> Lwt.return e
        | Ok () ->
          let pointers, data_region = Chamelon.File.of_block index data in
          match pointers with
          | next::_ ->
            read_block (data_region :: l) (index - 1) (Int64.of_int32 next)
          | [] ->
            Lwt.return @@ Ok (data_region :: l)
      in
      let index = Chamelon.File.last_block_index ~file_size:length
          ~block_size:t.block_size in
      read_block [] index pointer >>= function
      | Error _ -> Lwt.return @@ Error (`Not_found key)
      | Ok l ->
        (* the last block very likely needs to be trimmed *)
        let cs = Cstruct.sub (Cstruct.concat l) 0 length in
        let s = Cstruct.(to_string cs) in
        Lwt.return @@ Ok s

    let get_value t block_pair filename =
      Find.entries_of_name t block_pair filename >|= function
      | Error _ -> Error (`Not_found filename)
      | Ok compacted ->
        let inline_files = List.find_opt (fun (tag, _data) ->
            Chamelon.Tag.((fst tag.type3) = LFS_TYPE_STRUCT) &&
            Chamelon.Tag.((snd tag.type3) = 0x01)
          )
        in
        let ctz_files = List.find_opt (fun (tag, _block) ->
            Chamelon.Tag.((fst tag.type3 = LFS_TYPE_STRUCT) &&
                          Chamelon.Tag.((snd tag.type3 = 0x02)
                                       ))) in
        Log.debug (fun m -> m "found %d entries with name %s" (List.length compacted) filename);
        match inline_files compacted, ctz_files compacted with
        | Some (_tag, data), None -> Ok (`Inline (Cstruct.to_string data))
        | None, None -> begin
          (* is it actually a directory? *)
            match List.find_opt (fun (tag, _data) ->
                Chamelon.Tag.((fst tag.type3) = LFS_TYPE_STRUCT) &&
                Chamelon.Tag.((snd tag.type3) = 0x00)
              ) compacted with
            | Some _ -> Error (`Value_expected filename)
            | None ->
              Log.debug (fun f -> f "an ID was found for the filename, but no entries are structs or files");
              Error (`Not_found filename)
        end
        | _, Some (_, ctz) ->
          match Chamelon.File.ctz_of_cstruct ctz with
          | Some (pointer, length) -> Ok (`Ctz (Int64.of_int32 pointer, Int32.to_int length))
          | None -> Error (`Value_expected filename)

    let get t key : (string, error) result Lwt.t =
      let map_errors = function
        | Ok (`Inline d) -> Lwt.return (Ok d)
        | Ok (`Ctz ctz) -> get_ctz t key ctz
        | Error (`Not_found k) -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v k))
        | Error (`Value_expected k) -> Lwt.return @@ Error (`Value_expected (Mirage_kv.Key.v k))
      in
      match Mirage_kv.Key.segments key with
      | [] -> Lwt.return @@ Error (`Value_expected key)
      | basename::[] -> get_value t root_pair basename >>= map_errors
      | _ ->
        let dirname = Mirage_kv.Key.(parent key |> segments) in
        Find.find_directory t root_pair dirname >>= function
        | `Basename_on pair -> begin
            get_value t pair (Mirage_kv.Key.basename key) >>= map_errors
          end
        | _ -> Lwt.return @@ Error (`Not_found key)

  end

  module File_write : sig
    (** [set_in_directory block_pair t filename data] creates entries in
     * [block_pair] for [filename] pointing to [data] *)
    val set_in_directory : int64 * int64 -> t -> string -> string ->
      (unit, write_error) result Lwt.t

  end = struct  

    let rec write_ctz_block t l index so_far data =
      if Int.compare so_far (String.length data) >= 0 then begin
        (* we purposely don't reverse the list because we're going to want
         * the *last* block for inclusion in the ctz structure *)
        Lwt.return @@ Ok l
      end else begin
        Allocate.get_block t >>= function
        | Error _ -> Lwt.return @@ Error `No_space
        | Ok block_number ->
          let pointer = Int64.to_int32 block_number in
          let block_cs = Cstruct.create t.block_size in
          let skip_list_size = Chamelon.File.n_pointers index in
          let skip_list_length = skip_list_size * 4 in
          let data_length = min (t.block_size - skip_list_length) ((String.length data) - so_far) in
          (* the 0th item in the skip list is always (index - 1). Only exception
           * is the last block in the list (the first block in the file),
           * which has no skip list *)
          (match l with
           | [] -> ()
           | (_last_index, last_pointer)::_ ->
             (* the first entry in the skip list should be for block _last_index,
              * which is index - 1 *)
             Cstruct.LE.set_uint32 block_cs 0 last_pointer
          );
          for n_skip_list = 1 to (skip_list_size - 1) do
            let point_index = List.assoc (n_skip_list / (2 lsl n_skip_list)) l in
            Cstruct.LE.set_uint32 block_cs (n_skip_list * 4) point_index
          done;
          Cstruct.blit_from_string data so_far block_cs skip_list_length data_length;
          This_Block.write t.block (Int64.of_int32 pointer) [block_cs] >>= function
          | Error _ -> Lwt.return @@ Error `No_space
          | Ok () ->
            write_ctz_block t ((index, pointer)::l) (index + 1) (so_far + data_length) data
      end

    let write_in_ctz dir_block_pair t filename data entries =
      Read.block_of_block_pair t dir_block_pair >>= function
      | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v filename))
      | Ok root ->
        let file_size = String.length data in
        write_ctz_block t [] 0 0 data >>= function
        | Error _ as e -> Lwt.return e
        | Ok [] -> Lwt.return @@ Error `No_space
        | Ok ((_last_index, last_pointer)::_) ->
          let next = match Chamelon.Block.(IdSet.max_elt_opt @@ ids root) with
            | None -> 1
            | Some n -> n + 1
          in
          let name = Chamelon.File.name filename next in
          let ctime = Chamelon.Entry.ctime next (Clock.now_d_ps ()) in
          let ctz = Chamelon.File.create_ctz next
              ~pointer:last_pointer ~file_size:(Int32.of_int file_size)
          in
          let new_entries = entries @ [name; ctime; ctz] in
          Logs.debug (fun m -> m "writing ctz %d entries for ctz for file %s" (List.length new_entries) filename);
          let new_block = Chamelon.Block.add_commit root new_entries in
          Write.block_to_block_pair t new_block dir_block_pair >>= function
          | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v filename))
          | Ok () -> Lwt.return @@ Ok ()

    let write_inline block_pair t filename data entries =
      Read.block_of_block_pair t block_pair >>= function
      | Error _ ->
        Logs.err (fun m -> m "error reading block pair %Ld, %Ld"
                     (fst block_pair) (snd block_pair));
        Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v filename))
      | Ok extant_block ->
        let used_ids = Chamelon.Block.ids extant_block in
        let next = match Chamelon.Block.IdSet.max_elt_opt used_ids with
          | None -> 1
          | Some n -> n + 1
        in
        let ctime = Chamelon.Entry.ctime next (Clock.now_d_ps ()) in
        let file = Chamelon.File.write_inline filename next (Cstruct.of_string data) in
        let commit = entries @ (ctime :: file) in
        Logs.debug (fun m -> m "writing %d entries for inline file %s" (List.length file) filename);
        let new_block = Chamelon.Block.add_commit extant_block commit in
        Write.block_to_block_pair t new_block block_pair >>= function
        | Error `No_space -> Lwt.return @@ Error `No_space
        | Error `Split
        | Error `Split_emergency -> Logs.err (fun m -> m "couldn't write a block, because it got too big");
          Lwt.return @@ Error `No_space
        | Ok () -> Lwt.return @@ Ok ()

    let set_in_directory block_pair t (filename : string) data =
      if String.length filename < 1 then Lwt.return @@ Error (`Value_expected Mirage_kv.Key.empty)
      else begin
        Lwt_mutex.with_lock t.new_block_mutex @@ fun () ->
        Find.entries_of_name t block_pair filename >>= function
        | Error (`Not_found _ ) as e -> Lwt.return e
        | Ok [] | Error (`No_id _) -> begin
            Logs.debug (fun m -> m "writing new file %s" filename);
            if (String.length data) > (t.block_size / 4) then
              write_in_ctz block_pair t filename data []
            else
              write_inline block_pair t filename data []
          end
        | Ok (hd::_) ->
          (* we *could* replace the previous ctz/inline entry,
           * instead of deleting the whole mapping and replacing it,
           * but since we do both the deletion and the new addition
           * in the same commit, I think this saves us some potentially
           * error-prone work *)
          let id = Chamelon.Tag.((fst hd).id) in
          Logs.debug (fun m -> m "deleting existing entry %s at id %d" filename id);
          let delete = (Chamelon.Tag.(delete id), Cstruct.create 0) in
          if (String.length data) > (t.block_size / 4) then
            write_in_ctz block_pair t filename data [delete]
          else
            write_inline block_pair t filename data [delete]
      end

  end

  module Delete = struct
    let delete_in_directory block_pair t name =
      Find.entries_of_name t block_pair name >>= function
        (* several "it's not here" cases *)
      | Error (`No_id _) | Error (`Not_found _) ->
        Logs.debug (fun m -> m "no id or nothing found for %s" name);
        Lwt.return @@ Ok ()
      | Ok [] ->
        Logs.debug (fun m -> m "no entries on %Ld, %Ld for %s"
                       (fst block_pair) (snd block_pair) name);
        Lwt.return @@ Ok ()
      | Ok (hd::_tl) ->
        let id = Chamelon.Tag.((fst hd).id) in
        let deletion = Chamelon.Tag.delete id in
        Logs.debug (fun m -> m "adding deletion for id %d on block pair %Ld, %Ld"
                       id (fst block_pair) (snd block_pair));
        Read.block_of_block_pair t block_pair >>= function
        | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v name))
        | Ok block ->
        let new_block = Chamelon.Block.add_commit block [(deletion, Cstruct.empty)] in
        Write.block_to_block_pair t new_block block_pair >>= function
        | Error _ -> Lwt.return @@ Error `No_space
        | Ok () -> Lwt.return @@ Ok ()

  end

  (* `device` should be an already-connected block device *)
  let connect ~program_block_size ~block_size device : (t, error) result Lwt.t =
    This_Block.connect ~block_size device >>= fun block ->
    Logs.debug (fun f -> f "initiating filesystem with block size %d (0x%x)" block_size block_size);
    let first_block = Cstruct.create block_size in
    This_Block.read block 0L [first_block] >>= function
    | Error e ->
      Logs.err (fun f -> f "first block read failed: %a" This_Block.pp_error e);
      Lwt.return @@ Error (`Not_found (Mirage_kv.Key.empty))
    | Ok () ->
      let t = {block; block_size; program_block_size; lookahead = ref (`Before, []); new_block_mutex = Lwt_mutex.create ()} in
      Lwt_mutex.lock t.new_block_mutex >>= fun () ->
      Traverse.follow_links t (Chamelon.Entry.Metadata root_pair) >>= function
      | Error _e ->
        Lwt_mutex.unlock t.new_block_mutex;
        Lwt.fail_with "couldn't get list of used blocks"
      | Ok used_blocks ->
        Logs.debug (fun f -> f "found %d used blocks on block-based key-value store" (List.length used_blocks));
        let open Allocate in
        let lookahead = ref (`After, unused ~bias:`Before t used_blocks) in
        Lwt_mutex.unlock t.new_block_mutex;
        Lwt.return @@ Ok {t with lookahead; block; block_size; program_block_size}

  let format ~program_block_size ~block_size (sectors : Sectors.t) :
    (unit, write_error) result Lwt.t =
    This_Block.connect ~block_size sectors >>= fun block ->
    let write_whole_block n b = This_Block.write block n
        [fst @@ Chamelon.Block.to_cstruct ~program_block_size ~block_size b]
    in
    let name = Chamelon.Superblock.name in
    let block_count = This_Block.block_count block in
    let superblock_inline_struct = Chamelon.Superblock.inline_struct (Int32.of_int block_size) (Int32.of_int block_count) in
    let block_0 = Chamelon.Block.of_entries ~revision_count:1 [name; superblock_inline_struct] in
    let block_1 = Chamelon.Block.of_entries ~revision_count:2 [name; superblock_inline_struct] in
    Lwt_result.both
    (write_whole_block (fst root_pair) block_0)
    (write_whole_block (snd root_pair) block_1) >>= function
    | Ok ((), ()) -> Lwt.return @@ Ok ()
    | _ -> Lwt.return @@ Error `No_space

end
