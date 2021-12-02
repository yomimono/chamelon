open Lwt.Infix

let root_pair = (0L, 1L)

module Make(Sectors : Mirage_block.S) = struct
  module Fs = Fs.Make(Sectors)
  
  type key = Mirage_kv.Key.t

  let log_src = Logs.Src.create "littlefs-kv" ~doc:"littlefs KV layer"
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

  let set t key data : (unit, write_error) result Lwt.t =
    let dir = Mirage_kv.Key.parent key in
    Fs.Find.find_directory t root_pair (Mirage_kv.Key.segments dir) >>= function
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
    let translate entries = List.filter_map Littlefs.Entry.info_of_entry entries in
    let ls_in_dir dir_pair =
      Fs.Find.all_entries_in_dir t dir_pair >>= function
      | Error _ -> Lwt.return @@ Error (`Not_found key)
      | Ok entries -> Lwt.return @@ Ok (translate @@ Littlefs.Entry.compact entries)
    in
    match (Mirage_kv.Key.segments key) with
    | [] -> ls_in_dir root_pair
    | segments ->
      Fs.Find.find_directory t root_pair segments >>= function
      | `No_id k -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v k))
      | `No_structs | `No_entry -> Lwt.return @@ Error (`Not_found key)
      | `Basename_on pair -> ls_in_dir pair

  let exists t key =
    (* TODO: just go look for the thing directly in the FS,
     * instead of getting a list and then having to sort through it *)
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
      Logs.warn (fun m -> m "can't delete %a" Mirage_kv.Key.pp key);
      Lwt.return @@ Error (`Not_found key)
    end else
      Fs.Find.find_directory t root_pair Mirage_kv.Key.(segments @@ parent key) >>= function
      | `Basename_on pair -> Fs.Delete.delete_in_directory pair t (Mirage_kv.Key.basename key)
      | `No_entry | `No_id _ | `No_structs -> Lwt.return @@ Ok ()

  let connect = Fs.connect

  let format = Fs.format
end
