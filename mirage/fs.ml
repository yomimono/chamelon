open Lwt.Infix

module Make(Sectors: Mirage_block.S) = struct
  module This_Block = Block_ops.Make(Sectors)
  module Internal = Fs_internal.Make(This_Block)

  type key = Mirage_kv.Key.t

  type error = Internal.error
  type write_error = Internal.write_error

  type t = Internal.t
  type blockpair = Internal.blockpair
  type directory_head = blockpair

  let log_src = Logs.Src.create "chamelon-fs" ~doc:"chamelon FS layer"
  module Log = (val Logs.src_log log_src : Logs.LOG)

  let pp_blockpair = Internal.pp_blockpair

  let name_length_max t = t.Internal.name_length_max

  let mkdir t key =
    Internal.get_directory ~ensure_path:true t Internal.root_pair key

  let ls t key =
    let cmp (name1, _) (name2, _) = String.compare name1 name2 in
    (* once we've found the (first) directory pair of the *parent* directory,
     * get the list of all entries naming files or directories
     * and sort them *)
    let ls_in_dir dir_pair =
      Internal.Find.all_entries_in_dir t dir_pair >>= function
      | Error _ -> Lwt.return @@ Error (`Not_found key)
      | Ok entries_by_block ->
        let translate entries = List.filter_map Chamelon.Entry.info_of_entry entries |> List.sort cmp in
        (* we have to compact first, because IDs are unique per *block*, not directory.
         * If we compact after flattening the list, we might wrongly conflate multiple
         * entries in the same directory, but on different blocks. *)
        let compacted = List.map (fun (_block, entries) -> Chamelon.Entry.compact entries) entries_by_block in
        let to_key (k, x) = Mirage_kv.Key.add key k, x in
        Lwt.return @@ Ok (List.map to_key (translate @@ List.flatten compacted))
    in
    (* find the parent directory of the [key] *)
    match (Mirage_kv.Key.segments key) with
    | [] -> ls_in_dir Internal.root_pair
    | segments ->
      (* descend into each segment until we run out, at which point we'll be in the
       * directory we want to list *)
      Internal.Find.find_first_blockpair_of_directory t Internal.root_pair segments >>= function
      | `No_id k ->
        (* be sure to return `k` as the error value, so the user might find out
         * which part of a complex path is missing and be more easily able to fix the problem *)
        Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v k))
      (* No_structs is returned if part of the path is present, but not a directory (usually meaning
       * it's a file instead) *)
      | `Not_directory k -> Lwt.return @@ Error (`Dictionary_expected (Mirage_kv.Key.v k))
      | `Final_dir_on pair -> ls_in_dir pair

  let last_modified_value t key =
    (* find (parent key) on the filesystem *)
    Internal.(Find.find_first_blockpair_of_directory t root_pair Mirage_kv.Key.(segments @@ parent key)) >>= function
    | `No_id k -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v k))
    | `Not_directory k -> Lwt.return @@ Error (`Dictionary_expected (Mirage_kv.Key.v k))
    | `Final_dir_on block_pair ->
      (* get all the entries in (parent key) *)
      Internal.(Find.entries_of_name t block_pair @@ Mirage_kv.Key.basename key) >>= function
      | Error (`No_id k) -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v k))
      | Error (`Not_found k) -> Lwt.return @@ Error (`Not_found k)
      | Ok l ->
	(* [l] contains all entries associated with (basename key),
	 * some of which are hopefully last-updated metadata entries. *)
	(* We don't care which block any of the entries were on *)
	let l = List.(map snd l |> flatten) in
	(* find the mtime-looking entries *)
	match List.find_opt (fun (tag, _data) ->
	    Chamelon.Tag.(fst @@ tag.type3) = LFS_TYPE_USERATTR &&
	    Chamelon.Tag.(snd @@ tag.type3) = 0x74
	  ) l with
	| None ->
	  Log.warn (fun m -> m "Key %a found but it had no time attributes associated" Mirage_kv.Key.pp key);
	  Lwt.return @@ Error (`Not_found key)
	| Some (_tag, data) ->
	  match Chamelon.Entry.ctime_of_cstruct data with
	  | None ->
	    Log.err (fun m -> m "Time attributes (%a) found for %a but they were not parseable" Cstruct.hexdump_pp data Mirage_kv.Key.pp key);
            Lwt.return @@ Error (`Not_found key)
	  | Some k -> Lwt.return @@ Ok k
  module File_read : sig
    val get : t -> Mirage_kv.Key.t -> (string, error) result Lwt.t
    val get_partial : t -> Mirage_kv.Key.t -> offset:int -> length:int ->
       (string, error) result Lwt.t

  end = struct

    let get_ctz t key (pointer, file_size) =
      let rec read_block l index pointer =
        let data = Cstruct.create t.Internal.block_size in
        This_Block.read t.block pointer [data] >>= function
        | Error _ as e -> Lwt.return e
        | Ok () ->
          let pointers, data_region = Chamelon.File.of_block index data in
          let pointer_region = Cstruct.sub data 0 (4 * List.length pointers) in
          Log.debug (fun f -> f "block index %d at block number %Ld has %d bytes of data and %d outgoing pointers: %a (raw %a)"
                        index pointer (Cstruct.length data_region) (List.length pointers) Fmt.(list ~sep:comma int32) pointers Cstruct.hexdump_pp pointer_region);
          match pointers with
          | next::_ ->
            read_block (data_region :: l) (index - 1) (Int64.of_int32 next)
          | [] ->
            Lwt.return @@ Ok (data_region :: l)
      in
      let index = Chamelon.File.last_block_index ~file_size
          ~block_size:t.block_size in
      Log.debug (fun f -> f "last block has index %d (file size is %d)" index file_size);
      read_block [] index pointer >>= function
      | Error _ -> Lwt.return @@ Error (`Not_found key)
      | Ok l ->
        (* the last block very likely needs to be trimmed *)
        let cs = Cstruct.sub (Cstruct.concat l) 0 file_size in
        let s = Cstruct.(to_string cs) in
        Lwt.return @@ Ok s

    let get_value t parent_dir_head filename =
      Internal.Find.entries_of_name t parent_dir_head filename >|= function
      | Error _ | Ok [] -> Error (`Not_found filename)
      | Ok compacted ->
        (* if there are >1 block with entries, we only care about the last one *)
        let compacted = snd @@ List.(hd @@ rev compacted) in
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
        | Some (tag, data), None ->
          Log.debug (fun m -> m "found an inline entry with tag %a" Chamelon.Tag.pp tag);
          Ok (`Inline (Cstruct.to_string data))
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
      let map_result = function
        | Ok (`Inline d) -> Lwt.return (Ok d)
        | Ok (`Ctz ctz) -> get_ctz t key ctz
        | Error (`Not_found k) -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v k))
        | Error (`Value_expected k) -> Lwt.return @@ Error (`Value_expected (Mirage_kv.Key.v k))
      in
      match Mirage_kv.Key.segments key with
      | [] -> Lwt.return @@ Error (`Value_expected key)
      | basename::[] -> get_value t Internal.root_pair basename >>= map_result
      | _ ->
        let dirname = Mirage_kv.Key.(parent key |> segments) in
        Internal.(Find.find_first_blockpair_of_directory t root_pair dirname) >>= function
        | `Final_dir_on pair -> begin
            get_value t pair (Mirage_kv.Key.basename key) >>= map_result
          end
        | _ -> Lwt.return @@ Error (`Not_found key)

    let rec address_of_index t ~desired_index (pointer, index) =
      if desired_index = index then Lwt.return @@ Ok pointer
      else begin
        let data = Cstruct.create t.Internal.block_size in
        This_Block.read t.block pointer [data] >>= function
        | Error e ->
          Log.err (fun f -> f "block read error: %a" This_Block.pp_error e);
          Lwt.return @@ Error (`Not_found (Mirage_kv.Key.empty))
        | Ok () ->
          let pointers, _ = Chamelon.File.of_block index data in
          if desired_index > index / 2 || (List.length pointers) < 2 then begin
          (* worst case: we want an index that's between our index and
           * (our index / 2), so we can't jump to it (or this index isn't
           * a multiple of 2, so we only have one link);
           * we just have to iterate backward until we get there *)
            match pointers with
            | next::_ ->
               address_of_index t ~desired_index (Int64.of_int32 next, (index - 1))
            | _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.empty))
          end else begin
            match pointers with
            (* TODO: we can do better than this if the index is even smaller than index / 2 *)
            | _ :: n_div_2 :: _ ->
              address_of_index t ~desired_index (Int64.of_int32 n_div_2, (index / 2))
            | _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.empty))
          end
      end

    let get_ctz_partial t key ~offset ~length (pointer, file_size) =
      let rec read_raw_blocks ~offset_index l index pointer =
        let data = Cstruct.create t.Internal.block_size in
        This_Block.read t.block pointer [data] >>= function
        | Error _ as e -> Lwt.return e
        | Ok () ->
          let pointers, data_region = Chamelon.File.of_block index data in
          let accumulated_data = data_region :: l in
          if index <= offset_index then Lwt.return @@ Ok accumulated_data else
          match pointers with
          | next::_ ->
            read_raw_blocks ~offset_index accumulated_data (index - 1) (Int64.of_int32 next)
          | [] ->
            Lwt.return @@ Ok accumulated_data
      in
      let last_byte = min file_size (offset + length) in
      let last_overall_block_index = Chamelon.File.last_block_index ~file_size
          ~block_size:t.block_size in
      let last_byte_of_interest_index = Chamelon.File.last_block_index ~file_size:last_byte ~block_size:t.block_size in
      address_of_index t ~desired_index:last_byte_of_interest_index (pointer, last_overall_block_index) >>= function
      | Error _ as e -> Lwt.return e
      | Ok last_byte_of_interest_pointer ->
        let offset_index = Chamelon.File.last_block_index ~file_size:offset ~block_size:t.block_size in
        read_raw_blocks ~offset_index [] last_byte_of_interest_index last_byte_of_interest_pointer >>= function
        | Error _ -> Lwt.return @@ Error (`Not_found key)
        | Ok [] -> Lwt.return @@ Ok ""
        | Ok (h::more_blocks) ->
          (* since our list is just the raw block contents of the relevant bit of the file,
           * we probably need to drop some bytes from the beginning in order to correctly
           * return the file starting at the right offset *)
          let first_block_offset = Chamelon.File.first_byte_on_index
              ~block_size:t.block_size offset_index
          in
          (* this calculation is correct *if* we correctly identified the
           * first block associated with this offset.
           * Otherwise it's wrong garbage nonsense, so let's hope we got that
           * first block correct :sweat_smile: *)
          let new_hd = Cstruct.shift h (offset - first_block_offset) in
          let offset_cs = Cstruct.concat @@ new_hd :: more_blocks |> Cstruct.to_string in
          (* we need to trim the results to either:
           * the requested length, if offset + length is < file_size
           * the file size minus the offset, if offset + length is > file_size.
          *)
          let final_length = if offset + length > file_size then (file_size - offset) else length in
          Lwt.return @@ Ok (String.sub offset_cs 0 final_length)

    let get_partial t key ~offset ~length : (string, error) result Lwt.t =
      if offset < 0 then begin
        Log.err (fun f -> f "read requested with negative offset");
        Lwt.return @@ Error (`Not_found key)
      end else if length <= 0 then begin
        Log.err (fun f -> f "read requested with length <= 0");
        Lwt.return @@ Error (`Not_found key)
      end else begin
        let map_result = function
          | Error (`Not_found k) -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v k))
          | Error (`Value_expected k) -> Lwt.return @@ Error (`Value_expected (Mirage_kv.Key.v k))
          | Ok (`Inline d) -> begin
            try Lwt.return @@ Ok (String.sub d offset @@ min length @@ (String.length d) - offset)
            with Invalid_argument _ -> Lwt.return @@ Error (`Not_found key)
          end
          | Ok (`Ctz ctz) ->
            get_ctz_partial t key ~offset ~length ctz
        in
        match Mirage_kv.Key.segments key with
        | [] -> Lwt.return @@ Error (`Value_expected key)
        | basename::[] -> get_value t Internal.root_pair basename >>= map_result
        | _ ->
          let dirname = Mirage_kv.Key.(parent key |> segments) in
          Internal.(Find.find_first_blockpair_of_directory t root_pair dirname) >>= function
          | `Final_dir_on pair -> begin
              get_value t pair (Mirage_kv.Key.basename key) >>= map_result
            end
          | _ -> Lwt.return @@ Error (`Not_found key)
      end
  end

  module Size = struct

    let get_file_size t parent_dir_head filename =
      Internal.Find.entries_of_name t parent_dir_head filename >|= function
      | Error _ | Ok [] -> Error (`Not_found (Mirage_kv.Key.v filename))
      | Ok compacted ->
        let entries = snd @@ List.(hd @@ rev compacted) in
        let inline_files = List.find_opt (fun (tag, _data) ->
            Chamelon.Tag.((fst tag.type3) = LFS_TYPE_STRUCT) &&
            Chamelon.Tag.((snd tag.type3) = Chamelon.Tag.Magic.struct_inline)
          )
        in
        let ctz_files = List.find_opt (fun (tag, _block) ->
            Chamelon.Tag.((fst tag.type3 = LFS_TYPE_STRUCT) &&
                          Chamelon.Tag.((snd tag.type3 = Chamelon.Tag.Magic.struct_ctz)
                                       ))) in
        Log.debug (fun m -> m "found %d entries with name %s" (List.length compacted) filename);
        match inline_files entries, ctz_files entries with
        | None, None -> Error (`Not_found (Mirage_kv.Key.v filename))
        | Some (tag, _data), None ->
          Ok (Optint.Int63.of_int tag.Chamelon.Tag.length)
        | _, Some (_tag, data) ->
          match Chamelon.File.ctz_of_cstruct data with
          | Some (_pointer, length) -> Ok (Optint.Int63.of_int32 length)
          | None -> Error (`Value_expected (Mirage_kv.Key.v filename))

    let rec size_all t blockpair =
      Internal.Find.all_entries_in_dir t blockpair >>= function
      | Error _ -> Lwt.return Optint.Int63.zero
      | Ok l ->
        let entries = List.(map snd l |> flatten) in
        Lwt_list.fold_left_s (fun acc e ->
            match Chamelon.Content.size e with
            | `File n -> Lwt.return @@ Optint.Int63.add n acc
            | `Skip -> Lwt.return @@ acc
            | `Dir p ->
              Log.debug (fun f -> f "descending into dirpair %a" pp_blockpair p);
              size_all t p >>= fun s -> Lwt.return @@ Optint.Int63.add s acc
          ) Optint.Int63.zero entries

    let size t key : (Optint.Int63.t, error) result Lwt.t =
      Log.debug (fun f -> f "getting size on key %a" Mirage_kv.Key.pp key);
      match Mirage_kv.Key.segments key with
      | [] -> size_all t Internal.root_pair >>= fun i -> Lwt.return @@ Ok i
      | basename::[] -> get_file_size t Internal.root_pair basename
      | segments ->
        Log.debug (fun f -> f "descending into segments %a" Fmt.(list ~sep:comma string) segments);
        Internal.(Find.find_first_blockpair_of_directory t root_pair segments) >>= function
        | `Final_dir_on p -> size_all t p >|= fun i -> Ok i
        | `No_id _ | `Not_directory _ -> begin
            (* no directory by that name, so try for a file *)
            Internal.(Find.find_first_blockpair_of_directory t root_pair segments) >>= function
            | `Final_dir_on pair -> begin
                get_file_size t pair (Mirage_kv.Key.basename key)
              end
            | _ -> Lwt.return @@ Error (`Not_found key)
        end

  end

  module File_write : sig
    (** [set_in_directory directory_head t filename data] creates entries in
     * [directory] for [filename] pointing to [data] *)
    val set_in_directory : directory_head -> t -> string -> string ->
      (unit, write_error) result Lwt.t

  end = struct

    (* write_ctz_block continues writing a CTZ `data` to `t` from the block list `blocks`. *)
    let rec write_ctz_block t blocks written index so_far data =
      if Int.compare so_far (String.length data) >= 0 then begin
        (* we purposely don't reverse the list because we're going to want
         * the *last* block for inclusion in the ctz structure *)
        Lwt.return @@ Ok written
      end else begin
        match blocks with
        | [] ->
          Log.err (fun f -> f "get_blocks gave us too few blocks for our CTZ file");
          Lwt.return @@ Error `No_space
        | block_number::blocks ->
          let pointer = Int64.to_int32 block_number in
          let block_cs = Cstruct.create t.Internal.block_size in
          let skip_list_size = Chamelon.File.n_pointers index in
          let skip_list_length = skip_list_size * 4 in
          let data_length = min (t.block_size - skip_list_length) ((String.length data) - so_far) in
          (* the 0th item in the skip list is always (index - 1). Only exception
           * is the last block in the list (the first block in the file),
           * which has no skip list *)
          (match written with
           | [] -> ()
           | (_last_index, last_pointer)::_ ->
             (* the first entry in the skip list should be for block _last_index,
              * which is index - 1 *)
             Cstruct.LE.set_uint32 block_cs 0 last_pointer
          );
          for n_skip_list = 1 to (skip_list_size - 1) do
            let destination_block_index = index / (1 lsl n_skip_list) in
            let point_index = List.assoc destination_block_index written in
            Cstruct.LE.set_uint32 block_cs (n_skip_list * 4) point_index
          done;
          Cstruct.blit_from_string data so_far block_cs skip_list_length data_length;
          This_Block.write t.block (Int64.of_int32 pointer) [block_cs] >>= function
          | Error _ -> Lwt.return @@ Error `No_space
          | Ok () ->
            write_ctz_block t blocks ((index, pointer)::written) (index + 1) (so_far + data_length) data
      end

    (* Get the correct number of blocks to write `data` as a CTZ, then write it. *)
    let write_ctz t data =
      let data_length = String.length data in
      let last_block_index = Chamelon.File.last_block_index
        ~file_size:data_length ~block_size:t.Internal.block_size
      in
      Internal.Allocate.get_blocks t (last_block_index + 1) >>= function
      | Error _ as e -> Lwt.return e
      | Ok blocks ->
        write_ctz_block t blocks [] 0 0 data

    (* Find the correct directory structure in which to write the metadata entry for the CTZ pointer.
     * Write the CTZ, then write the metadata. *)
    let rec write_in_ctz ?(ids_seen=Chamelon.Block.IdSet.empty) dir_block_pair t filename data entries =
      Internal.Read.block_of_block_pair t dir_block_pair >>= function
      | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v filename))
      | Ok root ->
        (* we need to keep track of which ids we've seen so far,
         * since we may traverse to a block that doesn't have any commits yet *)
        let ids_seen = Chamelon.Block.(IdSet.union ids_seen (ids root)) in
        match Chamelon.Block.hardtail root with
        | Some next_blockpair -> write_in_ctz ~ids_seen next_blockpair t filename data entries
        | None ->
          let file_size = String.length data in
          write_ctz t data >>= function
          | Error _ as e -> Lwt.return e
          | Ok [] -> Lwt.return @@ Error `No_space
          | Ok ((_last_index, last_pointer)::_) ->
            (* the file has been written; find an ID and write the appropriate metadata *)
            let next = match Chamelon.Block.(IdSet.max_elt_opt ids_seen) with
              | None -> 1
              | Some n -> n + 1
            in
            let name = Chamelon.File.name filename next in
            let ctime = Chamelon.Entry.ctime next (Mirage_ptime.now_d_ps ()) in
            let ctz = Chamelon.File.create_ctz next
                ~pointer:last_pointer ~file_size:(Int32.of_int file_size)
            in
            let new_entries = entries @ [name; ctime; ctz] in
            Log.debug (fun m -> m "writing %d entries for ctz for file %s of size %d" (List.length new_entries) filename file_size);
            let new_block = Chamelon.Block.add_commit root new_entries in
            Internal.Write.block_to_block_pair t new_block dir_block_pair >>= function
            | Error `No_space -> Lwt.return @@ Error `No_space
            | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v filename))
            | Ok () -> Lwt.return @@ Ok ()

    let rec write_inline ?(ids_seen=Chamelon.Block.IdSet.empty) block_pair t filename data entries =
      Internal.Read.block_of_block_pair t block_pair >>= function
      | Error _ ->
        Log.err (fun m -> m "error reading block pair %Ld, %Ld"
                     (fst block_pair) (snd block_pair));
        Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v filename))
      | Ok extant_block ->
        (* we need to keep track of which ids we've seen so far,
         * since we may traverse to a block that doesn't have any commits yet *)
        let ids_seen = Chamelon.Block.(IdSet.union ids_seen (ids extant_block)) in
        match Chamelon.Block.hardtail extant_block with
        | Some next_blockpair -> write_inline ~ids_seen next_blockpair t filename data entries
        | None ->
          let next = match Chamelon.Block.IdSet.max_elt_opt ids_seen with
            | None -> 1
            | Some n -> n + 1
          in
          let ctime = Chamelon.Entry.ctime next (Mirage_ptime.now_d_ps ()) in
          let file = Chamelon.File.write_inline filename next (Cstruct.of_string data) in
          let commit = entries @ (ctime :: file) in
          Log.debug (fun m -> m "writing %d entries for inline file %s" (List.length file) filename);
          let new_block = Chamelon.Block.add_commit extant_block commit in
          Internal.Write.block_to_block_pair t new_block block_pair >>= function
          | Error `No_space -> Lwt.return @@ Error `No_space
          | Error `Split | Error `Split_emergency ->
            Log.err (fun m -> m "couldn't write a block, because it got too big");
            Lwt.return @@ Error `No_space
          | Ok () -> Lwt.return @@ Ok ()

    let set_in_directory block_pair t (filename : string) data =
      let delete_entry block id =
        (* we *could* replace the previous ctz/inline entry,
         * instead of deleting the whole mapping and replacing it,
         * but since we do both the deletion and the new addition
         * in the same commit, I think this saves us some potentially
         * error-prone work *)
        Log.debug (fun m -> m "deleting existing entry %s at id %d on block %a" filename id pp_blockpair block);
        let delete = (Chamelon.Tag.(delete id), Cstruct.create 0) in
        (* we need to make sure our deletion and new items go in the same block pair as
         * the originals did *)
        if (String.length data) > (t.Internal.block_size / 4) then begin
          Log.debug (fun m -> m "writing %s at id %d to ctz" filename id);
          write_in_ctz block t filename data [delete]
        end else begin
          Log.debug (fun m -> m "writing %s at id %d inline on blockpair %a" filename id pp_blockpair block);
          write_inline block t filename data [delete]
        end
      in
      if String.length filename < 1 then Lwt.return @@ Error (`Value_expected Mirage_kv.Key.empty)
      else begin
        Lwt_mutex.with_lock t.new_block_mutex @@ fun () ->
        Internal.Find.entries_of_name t block_pair filename >>= function
        | Error (`Not_found _ ) as e -> Lwt.return e
        | Ok [] | Ok ((_, [])::_) | Error (`No_id _) -> begin
            Log.debug (fun m -> m "writing new file %s, size %d to blocks %a" filename (String.length data) pp_blockpair block_pair);
            if (String.length data) > (t.block_size / 4) then
              write_in_ctz block_pair t filename data []
            else
              write_inline block_pair t filename data []
          end
        | Ok ((block, hd::_)::[]) ->
          (* wait, why do we only delete it and not write it? *)
          delete_entry block Chamelon.Tag.((fst hd).id)
        | Ok l ->
          Log.debug (fun m -> m "got multiple blocks (%a) when looking for %s" Fmt.(list ~sep:semi pp_blockpair) (List.map fst l) filename);
          Lwt_list.fold_left_s (fun acc (block, entries) ->
            (* really regretting using 'list' here, not gonna lie *)
            match entries with
            | [] -> Lwt.return acc (* this should never happen, but if it does, noop is correct *)
            | entry::_ ->
              (* since all entries found will have the same id, and all we care about is the id,
               * we don't care how many entries we got *)
              delete_entry block Chamelon.Tag.((fst entry).id)
          ) (Ok ()) l
      end
  end

  let remove t key =
    if Mirage_kv.Key.(equal empty key) then begin
      (* it's impossible to remove the root directory in littlefs, as it's
       * implicitly at the root pair *)
      Log.warn (fun m -> m "refusing to delete the root directory");
      Lwt.return @@ Error (`Not_found key)
    end else begin
      (* first, find the parent directory from which to delete (basename key) *)
      Internal.Find.find_first_blockpair_of_directory t Internal.root_pair (Mirage_kv.Key.(segments @@ parent key)) >>= function
      | `Final_dir_on pair ->
        Log.debug (fun f -> f "found %a in a directory starting at %a, will delete"
                      Mirage_kv.Key.pp key pp_blockpair pair);
        Internal.Delete.delete_in_directory pair t (Mirage_kv.Key.basename key)
      (* if we couldn't find (parent key), it's already pretty deleted *)
      | `No_id _ -> Lwt.return @@ Ok ()
      | `Not_directory k -> Lwt.return @@ Error (`Dictionary_expected (Mirage_kv.Key.v k))
    end

(* [block_size_device] is the block size used by the underlying block device which
 * we're trying to mount a filesystem from,
 * as opposed to the block size recorded in the filesystem's superblock *)
  let check_superblock ~program_block_size ~block_size_device cs =
    match Chamelon.Block.of_cstruct ~program_block_size cs with
    | Error (`Msg s) ->
      Log.err (fun f -> f "error parsing block when checking superblock: %s" s);
      Lwt.return @@ Error (`Not_found Mirage_kv.Key.empty)
    | Ok parsed_block ->
      (* does this block have the expected superblock entries? *)
      (* the order of entries is strictly specified: name, then the inline struct, then
       * any other entries in the superblock *)
      match Chamelon.Block.entries parsed_block with
      | maybe_name :: maybe_struct :: _ when
          Chamelon.Superblock.is_valid_name maybe_name &&
          Chamelon.Superblock.is_valid_superblock maybe_struct -> begin
        match Chamelon.Superblock.parse (snd maybe_struct) with
        | Error (`Msg s) ->
          Log.err (fun f -> f "error parsing block when checking superblock: %s" s);
          Lwt.return @@ Error (`Not_found Mirage_kv.Key.empty)
        | Ok sb ->
          if sb.version_major != fst Chamelon.Superblock.version then begin
            Log.err (fun f -> f "filesystem is an incompatible version");
            Lwt.return @@ Error (`Not_found Mirage_kv.Key.empty)
          end else if not @@ Int32.equal sb.block_size block_size_device then begin
            Log.err (fun f -> f "filesystem expects a block device with size %ld but the device block size is %ld" sb.block_size block_size_device);
            Lwt.return @@ Error (`Not_found Mirage_kv.Key.empty)
          end else Lwt.return @@ Ok (Chamelon.Block.revision_count parsed_block, sb)
      end
      | _ -> Log.err (fun f -> f "expected entries not found on parsed superblock");
        Lwt.return @@ Error (`Not_found Mirage_kv.Key.empty)

  (* `device` should be an already-connected block device *)
  let connect ~program_block_size ~block_size device : (t, error) result Lwt.t =
    This_Block.connect ~block_size device >>= fun block ->
    Log.debug (fun f -> f "initiating filesystem with block size %d (0x%x)" block_size block_size);
    let block0, block1 = Cstruct.create block_size, Cstruct.create block_size in
    Lwt_result.both
      (This_Block.read block 0L [block0])
      (This_Block.read block 1L [block1])
    >>= function
    | Error e ->
      Log.err (fun f -> f "first blockpair read failed: %a" This_Block.pp_error e);
      Lwt.return @@ Error (`Not_found Mirage_kv.Key.empty)
    | Ok ((), ()) ->
      (* make sure the block is parseable and block size matches *)
      Lwt_result.both
        (check_superblock ~program_block_size ~block_size_device:(Int32.of_int block_size) block0)
        (check_superblock ~program_block_size ~block_size_device:(Int32.of_int block_size) block1)
      >>= function
      | Error _ as e -> Lwt.return e
      | Ok ((rc0, sb0), (rc1, sb1)) ->
        let lookahead = Internal.(ref {offset = 0; blocks = []}) in
        let file_size_max, name_length_max =
          if rc1 > rc0 then sb1.file_size_max, sb1.name_length_max else sb0.file_size_max, sb0.name_length_max
        in
        let t = Internal.({block;
                 block_size;
                 program_block_size;
                 lookahead;
                 file_size_max;
                 name_length_max;
                 new_block_mutex = Lwt_mutex.create ()})
        in
        Log.debug (fun f -> f "mounting fs with file size max %ld, name length max %ld" file_size_max name_length_max);
        Lwt_mutex.lock t.new_block_mutex >>= fun () ->
        Internal.(Traverse.follow_links t [] (Chamelon.Entry.Metadata root_pair)) >>= function
        | Error _e ->
          Lwt_mutex.unlock t.new_block_mutex;
          Log.err (fun f -> f "couldn't get list of used blocks");
          Lwt.return @@ Error (`Not_found Mirage_kv.Key.empty)
        | Ok used_blocks ->
          Log.debug (fun f -> f "found %d used blocks on block-based key-value store: %a" (List.length used_blocks) Fmt.(list ~sep:sp int64) used_blocks);
          let open Internal.Allocate in
          match unused t used_blocks with
          | Ok () ->
            Lwt_mutex.unlock t.new_block_mutex;
            Log.debug (fun f -> f "populated lookahead allocator with %d blocks" (List.length !(t.lookahead).blocks));
            Lwt.return @@ Ok {t with block; block_size; program_block_size}
          | Error `No_space ->
            (* it's not great that there aren't free blocks,
             * but the user may well be connecting to try to do something about it,
             * so refusing to continue is the wrong move *)
            Log.err (fun f -> f "could not initialize lookahead allocator: no blocks available");
            Log.err (fun f -> f "writes to the filesystem are likely to fail");
            Lwt.return @@ Ok {t with block; block_size; program_block_size}

  let format ~program_block_size ~block_size (sectors : Sectors.t) :
    (unit, write_error) result Lwt.t =
    This_Block.connect ~block_size sectors >>= fun block ->
    let write_whole_block n b = This_Block.write block n
        [fst @@ Chamelon.Block.to_cstruct ~program_block_size ~block_size b]
    in
    let name = Chamelon.Superblock.name in
    let block_count = This_Block.block_count block in
    let superblock_inline_struct = Chamelon.Superblock.inline_struct (Int32.of_int block_size) (Int32.of_int block_count) in
    let block_0 = Chamelon.Block.of_entries ~revision_count:1l [name; superblock_inline_struct] in
    let block_1 = Chamelon.Block.of_entries ~revision_count:2l [name; superblock_inline_struct] in
    Lwt_result.both
    (write_whole_block (fst Internal.root_pair) block_0)
    (write_whole_block (snd Internal.root_pair) block_1) >>= function
    | Ok ((), ()) -> Lwt.return @@ Ok ()
    | _ -> Lwt.return @@ Error `No_space

  let is_directory t key =
    Internal.get_directory ~ensure_path:false t Internal.root_pair (Mirage_kv.Key.segments key) >|= function
    | Ok _ -> true
    | _ -> false

  let dump fmt t =
    let block_buffer = Cstruct.create t.Internal.block_size in
    let print_block n =
      This_Block.read t.block n [block_buffer] >>= function
      | Error e -> Lwt.return @@ Fmt.pf fmt "@[block %Ld (%Lx)@ block-level read error: %a@]" n n
        This_Block.pp_error e
      | Ok () ->
        let program_block_size = t.program_block_size in
        match Chamelon.Block.of_cstruct ~program_block_size block_buffer with
        | Error (`Msg s) -> Lwt.return @@ Fmt.pf fmt "@[block %Ld (%Lx)@ chamelon-level parse error: %s@]" n n s
        | Ok block ->
           if n < 2L then
             Fmt.pf fmt "@[superblock %Ld (%Lx)@ parsed contents: %a@ @]" n n Chamelon.Block.pp block
           else
             Fmt.pf fmt "@[block %Ld (%Lx)@ parsed contents: %a@ @]" n n Chamelon.Block.pp block;
           Lwt.return_unit
    in
    let block_count = This_Block.block_count t.block in
    let l = List.init block_count (fun n -> Int64.of_int n)  in
    Lwt_list.iter_s print_block l

end
