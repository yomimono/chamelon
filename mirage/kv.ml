open Lwt.Infix

let root_pair = (0L, 1L)

module Make(Sectors : Mirage_block.S)(Clock : Mirage_clock.PCLOCK) = struct
  module Fs = Fs.Make(Sectors)(Clock)
  
  type key = Mirage_kv.Key.t

  let log_src = Logs.Src.create "chamelon-kv" ~doc:"chamelon KV layer"
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

  let pp_error fmt = function
    | `Not_found key -> Format.fprintf fmt "key %a not found" Mirage_kv.Key.pp key
    | `Dictionary_expected key -> Format.fprintf fmt "%a was not a dictionary" Mirage_kv.Key.pp key
    | `Value_expected key -> Format.fprintf fmt "%a was not a value" Mirage_kv.Key.pp key

  let pp_write_error fmt = function
    | `No_space -> Format.fprintf fmt "no space left on device"
    | `Too_many_retries n -> Format.fprintf fmt "tried to write %d times and didn't succeed" n
    | #error as e -> pp_error fmt e

  type t = Fs.t

  let get = Fs.File_read.get

  (* [set] does a little work on top of the filesystem's set functions, because
   * we need to make directories if the key has >1 segment in it. *)
  (* Once we've either found or created the parent directory, we can ask the FS layer
   * to set the data appropriately there. *)
  let set t key data : (unit, write_error) result Lwt.t =
    let dir = Mirage_kv.Key.parent key in
    Fs.Find.find_first_blockpair_of_directory t root_pair
      (Mirage_kv.Key.segments dir) >>= function
    | `Basename_on block_pair ->
      Logs.debug (fun m -> m "found basename of path %a on block pair %Ld, %Ld"
                     Mirage_kv.Key.pp key
                     (fst block_pair) (snd block_pair));
      (* the directory already exists, so just write the file *)
      Fs.File_write.set_in_directory block_pair t (Mirage_kv.Key.basename key) data
    | `No_id path -> begin
        Logs.debug (fun m -> m "path component %s had no id; making it and its children" path);
        (* something along the path is missing, so make it. *)
        (* note we need to call mkdir with the whole path (save the basename),
         * so that we get all levels of directory we may need,
         * not just the first thing that was found missing. *)
      Fs.mkdir t root_pair (Mirage_kv.Key.segments dir) >>= function
      | Error (`Not_found _) -> Lwt.return @@ (Error (`Not_found (Mirage_kv.Key.v path)))
      | Error `No_space as e -> Lwt.return e
      | Ok block_pair ->
        Logs.debug (fun m -> m "made filesystem structure for %a, writing to blockpair %Ld, %Ld"
                       Mirage_kv.Key.pp dir (fst block_pair) (snd block_pair)
        );
        Fs.File_write.set_in_directory block_pair t (Mirage_kv.Key.basename key) data
      end
    | `No_entry ->
      Logs.err (fun m -> m "id was present but no matching entries");
      Lwt.return @@ Error (`Not_found key)
    | `No_structs ->
      Logs.err (fun m -> m "id was present but no matching structure");
      Lwt.return @@ Error (`Not_found key)

  let list t key : ((string * [`Dictionary | `Value]) list, error) result Lwt.t =
    let cmp (name1, _) (name2, _) = String.compare name1 name2 in
    let translate entries = List.filter_map Chamelon.Entry.info_of_entry entries |> List.sort cmp in
    let ls_in_dir dir_pair =
      Fs.Find.all_entries_in_dir t dir_pair >>= function
      | Error _ -> Lwt.return @@ Error (`Not_found key)
      | Ok entries_by_block ->
        (* we have to compact first, because IDs are unique per *block*, not directory.
         * If we compact after flattening the list, we might wrongly conflate multiple
         * entries in the same directory, but on different blocks. *)
        let compacted = List.map (fun (_block, entries) -> Chamelon.Entry.compact entries) entries_by_block in
        Lwt.return @@ Ok (translate @@ List.flatten compacted)
    in
    match (Mirage_kv.Key.segments key) with
    | [] -> ls_in_dir root_pair
    | segments ->
      (* descend into each segment until we run out, at which point we'll be in the
       * directory we want to list *)
      Fs.Find.find_first_blockpair_of_directory t root_pair segments >>= function
      | `No_id k -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v k))
      | `No_structs | `No_entry -> Lwt.return @@ Error (`Not_found key)
      | `Basename_on pair -> ls_in_dir pair

  let exists t key =
    list t (Mirage_kv.Key.parent key) >>= function
    | Error _ as e -> Lwt.return e
    | Ok l ->
      let lookup (name, dict_or_val) =
        if (String.compare name (Mirage_kv.Key.basename key)) = 0 then
          Some dict_or_val
        else None
      in
      Lwt.return @@ Ok (List.find_map lookup l)

  let remove t key =
    if Mirage_kv.Key.(equal empty key) then begin
    (* it's impossible to remove the root directory in littlefs, as it's
     * implicitly at the root pair *)
      Logs.warn (fun m -> m "refusing to delete the root directory");
      Lwt.return @@ Error (`Not_found key)
    end else
      Fs.Find.find_first_blockpair_of_directory t root_pair Mirage_kv.Key.(segments @@ parent key) >>= function
      | `Basename_on pair ->
        Logs.debug (fun f -> f "found %a in a directory starting at %a, will delete"
                       Mirage_kv.Key.pp key Fmt.(pair ~sep:comma int64 int64) 
                       pair);
        Fs.Delete.delete_in_directory pair t (Mirage_kv.Key.basename key)
      | `No_entry | `No_id _ | `No_structs -> Lwt.return @@ Ok ()

  let last_modified t key =
    (* easy case: `key` represents a value, not a dictionary. Find the associated
     * metadata for the timestamp at which it was last modified and return it. *)
    let last_modified_value t key =
      Fs.Find.find_first_blockpair_of_directory t root_pair Mirage_kv.Key.(segments @@ parent key) >>= function
      | `No_entry | `No_structs -> Lwt.return @@ Error (`Not_found key)
      | `No_id k -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v k))
      | `Basename_on block_pair ->
        Fs.Find.entries_of_name t block_pair @@ Mirage_kv.Key.basename key >>= function
        | Error (`No_id k) | Error (`Not_found k) -> Lwt.return @@ Error (`Not_found k)
        | Ok l ->
          (* we only care about the last block with entries, and don't care about its block number *)
          let l = snd @@ List.(hd @@ rev l) in
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
    in
    Fs.Find.find_first_blockpair_of_directory t root_pair (Mirage_kv.Key.segments key) >>= function
    | `No_id _ | `No_entry | `No_structs -> last_modified_value t key
    | `Basename_on _block_pair ->
      let open Lwt_result.Infix in
      (* we were asked to get the last_modified time of a directory :/ *)
      list t key >>= fun l ->
      (* luckily, the spec says we should only check last_modified dates to a depth of 1 *)
      (* unfortunately, the spec *doesn't* say what the last_modified time of an empty directory is :/ *)
      Lwt_list.fold_left_s (fun span entry ->
          match span with
          | Error _ as e -> Lwt.return e
          | Ok prev ->
            match entry with
            | _, `Dictionary -> Lwt.return (Ok prev)
            | (name, `Value) ->
              last_modified_value t Mirage_kv.Key.(key / name) >>= fun new_span ->
              match Ptime.Span.of_d_ps prev, Ptime.Span.of_d_ps new_span with
              | None, _ | _, None -> Lwt.return @@ Error (`Not_found key)
              | Some p, Some n ->
                match Ptime.of_span p, Ptime.of_span n with
                | None, _ | _, None -> Lwt.return @@ Error (`Not_found key)
                | Some p_ts, Some a_ts ->
                  if Ptime.is_later a_ts ~than:p_ts
                  then Lwt.return @@ Ok new_span
                  else Lwt.return @@ Ok prev
        )
        (Ok Ptime.Span.(zero |> to_d_ps)) l

  (* this is probably a surprising implementation for `batch`. Concurrent writes are not
   * supported by this implementation (there's a global write mutex) so we don't have
   * to do any work to make sure that writes don't get in each other's way. *)
  let batch t ?(retries=13) f =
    let _ = retries in f t

  let digest t key =
    let rec aux ctx t key =
      get t key >>= function
      | Ok v ->
        let digest = Digestif.SHA256.feed_string ctx v in
        Lwt.return @@ Ok digest
      | Error (`Value_expected _) -> begin
        (* let's see whether we can get a digest for the directory contents *)
        (* unfortunately we can't just run a digest of the block list,
         * because CTZs can change file contents without changing
         * metadata if the length remains the same, and also because
         * there are many differences possible in the entry list that map to the same
         * filesystem structure *)
        list t key >>= function
        | Error e ->
          Log.err (fun m -> m "error listing %a: %a\n%!" Mirage_kv.Key.pp key pp_error e);
          Lwt.return @@ Error (`Not_found key)
        | Ok l -> begin
          (* There's no explicit statement in the mli about whether
           * we should descend beyond 1 dictionary for `digest`,
           * but I'm not sure how we can meaningfully have a digest if we don't *)
          Lwt_list.fold_left_s (fun ctx_result (basename, _) ->
              match ctx_result with
              | Error _ as e -> Lwt.return e
              | Ok ctx ->
                let path = Mirage_kv.Key.add key basename in
                aux ctx t path
            ) (Ok ctx) l
        end
      end
      | Error _ as e -> Lwt.return e
    in
    let ctx = Digestif.SHA256.init () in
    Log.debug (fun f -> f "context for digest initiated");
    aux ctx t key >|= function
    | Error e -> Error e
    | Ok ctx -> Ok Digestif.SHA256.(to_raw_string @@ get ctx)

  let disconnect _ = Lwt.return_unit

  let connect ~program_block_size block =
    Sectors.get_info block >>= fun info ->
    let block_size = info.Mirage_block.sector_size in
    Fs.connect ~program_block_size ~block_size block

  let format ~program_block_size block =
    Sectors.get_info block >>= fun info ->
    let block_size = info.Mirage_block.sector_size in
    Fs.format ~program_block_size ~block_size block
end
