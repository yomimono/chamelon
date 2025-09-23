module Mirage_block = Block (* disambiguate this from Chamelon.Block *)
module Chamelon = Kv.Make(Mirage_block)

let pp_ty fmt = function
  | `Value -> Stdlib.Format.fprintf fmt "%s" "file"
  | `Dictionary -> Stdlib.Format.fprintf fmt "%s" "directory"

let pp_time fmt = function
  | Error `No_last_modified -> Stdlib.Format.fprintf fmt "no last modified info"
  | Ok timestamp -> Ptime.pp fmt timestamp

let ls {Common_options.image; block_size; program_block_size} timestamp path =
  let open Lwt.Infix in
  let check_time t path =
    Chamelon.last_modified t path >|= function
    | Error _ -> Error `No_last_modified
    | Ok ts -> Ok ts
  in
  Lwt_main.run @@ (
  Mirage_block.connect ~prefered_sector_size:(Some block_size) image >>= fun block ->
  Chamelon.connect block ~program_block_size >>= function
  | Error _ -> Stdlib.Format.eprintf "Error doing the initial filesystem ls\n%!"; exit 1
  | Ok t ->
    let requested_path = Mirage_kv.Key.v path in
    Chamelon.list t requested_path >>= function
    | Error (`Value_expected key) -> Stdlib.Format.eprintf "A component of the path %s was not as expected: %a\n%!" path Mirage_kv.Key.pp key; exit 1
    | Error (`Not_found key) -> begin
      (* if the key is a value, we'd like to output its information *)
      Chamelon.exists t requested_path >>= function
      | Ok None ->
        Stdlib.Format.eprintf "key %a not found\n%!" Mirage_kv.Key.pp key; exit 1
      | Error _ ->
        Stdlib.Format.eprintf "error attempting to find %a\n%!" Mirage_kv.Key.pp key; exit 2
      | Ok (Some ty) ->
        if timestamp then begin
          check_time t requested_path >>= fun time ->
          Stdlib.Format.printf "%a : %a (%a)\n%!" Mirage_kv.Key.pp requested_path pp_ty ty pp_time time;
          Lwt.return_unit
        end else begin
          Stdlib.Format.printf "%a : %a\n%!" Mirage_kv.Key.pp requested_path pp_ty ty;
          Lwt.return_unit
        end
    end
    | Error _ -> Stdlib.Format.eprintf "filesystem was opened, but ls failed\n%!"; exit 2
    | Ok l ->
      let pp fmt (path, key_or_dict) =
        Stdlib.Format.fprintf fmt "%a : %a" Mirage_kv.Key.pp path pp_ty key_or_dict
      in
      let print (path, key_or_dict) =
        if timestamp then begin
          check_time t path >>= fun time ->
          Stdlib.Format.printf "%a (%a)\n%!" pp (path, key_or_dict) pp_time time;
          Lwt.return_unit
        end else begin
          Stdlib.Format.printf "%a\n%!" pp (path, key_or_dict);
          Lwt.return_unit
        end
      in
      Lwt_list.iter_p print l
   )
