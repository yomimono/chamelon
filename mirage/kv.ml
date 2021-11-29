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

  let get t key : (string, error) result Lwt.t =
    let map_errors = function
        | Ok (`Inline d) -> Lwt.return (Ok d)
        | Ok (`Ctz ctz) -> Fs.File_read.get_ctz t key ctz
        | Error _ -> Lwt.return @@ Error (`Not_found key)
    in
    match Mirage_kv.Key.segments key with
    | [] -> Lwt.return @@ Error (`Value_expected key)
    | basename::[] -> Fs.File_read.get_value t root_pair basename >>= map_errors
    | _ ->
      let dirname = Mirage_kv.Key.(parent key |> segments) in
      Fs.Find.find_directory t root_pair dirname >>= function
      | `Basename_on pair -> begin
        Fs.File_read.get_value t pair (Mirage_kv.Key.basename key) >>= map_errors
        end
      | _ -> Lwt.return @@ Error (`Not_found key)

  let set t key data : (unit, write_error) result Lwt.t =
    let dir = Mirage_kv.Key.parent key in
    Fs.Find.find_directory_block_pair t root_pair (Mirage_kv.Key.segments dir) >>= function
    | `Basename_on block_pair ->
      Fs.File_write.set_in_directory block_pair t (Mirage_kv.Key.basename key) data
    | `No_id path -> begin
      Fs.mkdir t root_pair (Mirage_kv.Key.segments dir) >>= function
      | Error _ -> Lwt.return @@ (Error (`Not_found (Mirage_kv.Key.v path)))
      | Ok block_pair ->
        Fs.File_write.set_in_directory block_pair t (Mirage_kv.Key.basename key) data
      end
    | _ -> Lwt.return @@ Error (`Not_found key)

  let list t key : ((string * [`Dictionary | `Value]) list, error) result Lwt.t =
    match (Mirage_kv.Key.segments key) with
    | [] -> begin
      Fs.Find.entries_following_hard_tail t root_pair >>= function
      | Error _ -> Lwt.return @@ Error (`Not_found key)
      | Ok entries -> Lwt.return @@ Ok (Fs.list_block entries)
    end
    | segments ->
      Fs.Find.find_directory t root_pair segments >>= function
      | `No_id k -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v k))
      | `No_structs | `No_entry | `Bad_pointer ->
        Lwt.return @@ Error (`Not_found key)
      | `Basename_on pair ->
        Fs.Find.entries_following_hard_tail t pair >>= function
        | Error _ -> Lwt.return @@ Error (`Not_found key)
        | Ok entries -> Lwt.return @@ Ok (Fs.list_block entries)

  let connect = Fs.connect

  let format = Fs.format
end
