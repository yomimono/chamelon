open Lwt.Infix

let root_pair = (0L, 1L)

module Make(Sectors : Mirage_block.S) = struct
  module Fs = Fs.Make(Sectors)
  
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

  type t = Fs.t

  let get = Fs.File_read.get

  let set t key data : (unit, write_error) result Lwt.t =
    let dir = Mirage_kv.Key.parent key in
    Fs.Find.find_directory t root_pair (Mirage_kv.Key.segments dir) >>= function
    | `Basename_on block_pair ->
      (* the directory already exists, so just write the file *)
      Fs.File_write.set_in_directory block_pair t (Mirage_kv.Key.basename key) data
    | `No_id path -> begin
        (* something along the path is missing, so make it. *)
        (* note we need to call mkdir with the whole path (save the basename),
         * so that we get all levels of directory we may need,
         * not just the first thing that was found missing. *)
      Fs.mkdir t root_pair (Mirage_kv.Key.segments dir) >>= function
      | Error _ -> Lwt.return @@ (Error (`Not_found (Mirage_kv.Key.v path)))
      | Ok block_pair ->
        Fs.File_write.set_in_directory block_pair t (Mirage_kv.Key.basename key) data
      end
    | _ -> Lwt.return @@ Error (`Not_found key)

  let list t key : ((string * [`Dictionary | `Value]) list, error) result Lwt.t =
    let translate entries = List.filter_map Littlefs.Entry.info_of_entry entries in
    let ls_in_dir dir_pair =
      Fs.Find.all_entries_in_dir t dir_pair >>= function
      | Error _ -> Lwt.return @@ Error (`Not_found key)
      | Ok entries -> Lwt.return @@ Ok (translate entries)
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

  let connect = Fs.connect

  let format = Fs.format
end
