open Lwt.Infix

module Make(Sectors : Mirage_block.S) = struct
  module Fs = Fs.Make(Sectors)

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
    | `Already_present of key (** The key is already present. *)
    | `Rename_source_prefix of key * key (** The source is a prefix of destination in rename. *)
  ]

  let pp_error fmt = function
    | `Not_found key -> Format.fprintf fmt "key %a not found" Mirage_kv.Key.pp key
    | `Dictionary_expected key -> Format.fprintf fmt "%a was not a dictionary" Mirage_kv.Key.pp key
    | `Value_expected key -> Format.fprintf fmt "%a was not a value" Mirage_kv.Key.pp key

  let pp_write_error fmt = function
    | `No_space -> Format.fprintf fmt "no space left on device"
    | `Already_present key -> Format.fprintf fmt "key %a is already present" Mirage_kv.Key.pp key
    | `Rename_source_prefix (k1, k2) -> Format.fprintf fmt "rename %a is a prefix of %a" Mirage_kv.Key.pp k1 Mirage_kv.Key.pp k2
    | #error as e -> pp_error fmt e

  type t = Fs.t

  let get = Fs.File_read.get

  let get_partial t key ~offset ~length =
    Fs.File_read.get_partial t key ~offset:(Optint.Int63.to_int offset) ~length

  (* [set] does a little work on top of the filesystem's set functions, because
   * we need to make directories if the key has >1 segment in it. *)
  (* Once we've either found or created the parent directory, we can ask the FS layer
   * to set the data appropriately there. *)
  let set t key data : (unit, write_error) result Lwt.t =
    let name_length = String.length @@ Mirage_kv.Key.basename key in
    let name_length_max = Int32.to_int @@ Fs.name_length_max t in
    if name_length > name_length_max then begin
      Log.err (fun f -> f "key length %d exceeds max length %d - refusing to write" name_length name_length_max);
      Lwt.return @@ Error (`Not_found Mirage_kv.Key.empty)
    end else begin
      let dir = Mirage_kv.Key.parent key in
      Fs.mkdir t (Mirage_kv.Key.segments dir) >>= function
      | Ok block_pair ->
        Log.debug (fun m -> m "found dir for %a on block pair %a"
                      Mirage_kv.Key.pp key
                      Fs.pp_blockpair block_pair);
        Fs.File_write.set_in_directory block_pair t (Mirage_kv.Key.basename key) data
      | Error (`Already_present e) ->
        Log.err (fun f -> f "error making directory for write to %a: component %s already present and not a directory" Mirage_kv.Key.pp key e);
        Lwt.return @@ Error (`Dictionary_expected (Mirage_kv.Key.v e))
      | Error `No_space as e -> Lwt.return e
      | Error (`Not_found _k) as e -> Lwt.return e
    end

  let set_partial t key ~offset data =
    get t key >>= function
    | Error e -> Lwt.return (Error (e :> write_error))
    | Ok d ->
      let offset = Optint.Int63.to_int offset in
      Bytes.blit_string data 0 (Bytes.unsafe_of_string d) offset (String.length data);
      set t key d

  let allocate t key ?last_modified:_ size =
    (* [allocate t key ~last_modified size] should only allocate space
     * if there is nothing already present for this key *)
    get t key >>= function
    | Ok _ -> Lwt.return @@ Error (`Already_present key)
    | Error (`Not_found key) ->
      set t key (String.make (Optint.Int63.to_int size) '\000')
    | Error e -> Lwt.return @@ Error (e :> write_error)

  (** [list t key], where [key] is a reachable directory,
   * gives the files and directories (values and dictionaries) in [key].
   * It is not a recursive listing. *)
  let list t key : ((key * [`Dictionary | `Value]) list, error) result Lwt.t =
    Fs.ls t key

  (** [exists t key] returns true *only* for a file/value called (basename key) set in (dirname key).
   * A directory/dictionary doesn't cut it. *)
  let exists t key =
    list t (Mirage_kv.Key.parent key) >>= function
    | Error _ as e -> Lwt.return e
    | Ok l ->
      let lookup (name, dict_or_val) =
        if Mirage_kv.Key.equal name key then
          Some dict_or_val
        else None
      in
      Lwt.return @@ Ok (List.find_map lookup l)

  let size t key = Fs.Size.size t key

  let remove t key = Fs.remove t key

  let rename t ~source ~dest =
    get t source >>= function
    | Error e -> Lwt.return (Error (e :> write_error))
    | Ok data ->
      set t dest data >>= function
      | Ok () ->
        remove t src data
      | Error _ as e  -> Lwt.return e

  (* [last_modified t key] gives the timestamp metadata for a file/value,
   * or (for a directory) the most recently modified file/value within the directory.
   * We don't have to recurse, thankfully, so we only have to examine files. *)
  let last_modified t key =
    let ptimeify = function
      | Error _ as e -> e
      | Ok s ->
        let ts =
          let (let*) = Option.bind in
          let* span = Ptime.Span.of_d_ps s in
          let* ts = Ptime.of_span span in
          Some ts
        in
        match ts with
        | Some ts -> Ok ts
        | None -> Error (`Not_found key)
    in
    Fs.is_directory t key >>= function
    | false -> Fs.last_modified_value t key >|= ptimeify
    | true ->
      Fs.ls t key >>= function
      | Error _ as e -> Lwt.return e
      | Ok l ->
        (* luckily, the spec says we should only check last_modified dates to a depth of 1 *)
        (* unfortunately, the spec *doesn't* say what the last_modified time of an empty directory is :/ *)
        (* it's convenient for us to say it's the earliest possible time,
         * such that our fold can just use the latest time it's seen in the list as the accumulator *)
        Lwt_list.fold_left_s (fun ts entry ->
            match entry with
            | _, `Dictionary -> Lwt.return ts
            | (name, `Value) ->
              Fs.last_modified_value t name >>= fun new_span ->
              match ts, ptimeify new_span with
              | Error e, _ -> Lwt.return (Error e)
              | Ok _, Error e -> Lwt.return @@ Error e
              | Ok ts, Ok a_ts ->
                if Ptime.is_later a_ts ~than:ts
                then Lwt.return @@ Ok a_ts
                else Lwt.return @@ Ok ts
          )
          (Ok Ptime.epoch) l

  (** [digest t key] is the SHA256 sum of `key` if `key` is a value.
   * If [key] is a dictionary, it's a recursive digest of `key`'s contents. *)
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
              Lwt_list.fold_left_s (fun ctx_result (path, _) ->
                  match ctx_result with
                  | Error _ as e -> Lwt.return e
                  | Ok ctx -> aux ctx t path
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

  let dump fmt t = Fs.dump fmt t
end
