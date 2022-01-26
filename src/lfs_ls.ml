module Mirage_block = Block (* disambiguate this from Chamelon.Block *)
module Chamelon = Kv.Make(Mirage_block)(Pclock)

let timestamp =
  let doc = "output last-modified information when available" in
  Cmdliner.Arg.(value & flag & info ~doc ~docv:"TIMESTAMP" ["t"; "timestamp"])

let image =
  let doc = "path to the filesystem image" in
  Cmdliner.Arg.(value & pos 0 string "./littlefs.img" & info ~doc ~docv:"IMAGE" [])

let block_size =
  let doc = "block size of the filesystem in bytes" in
  Cmdliner.Arg.(value & pos 1 int 4096 & info ~doc ~docv:"BLOCK_SIZE" [])

let path =
  Cmdliner.Arg.(value & pos 2 string "/" & info ~docv:"PATH" [])

let pp_ty fmt = function
  | `Value -> Stdlib.Format.fprintf fmt "%s" "file"
  | `Dictionary -> Stdlib.Format.fprintf fmt "%s" "directory"

let pp_time fmt = function
  | Error `No_last_modified -> Stdlib.Format.fprintf fmt "no last modified info"
  | Error `Unparseable_span -> Stdlib.Format.fprintf fmt "(last modified span unparseable)"
  | Ok (`Span span) -> Ptime.Span.pp fmt span
  | Ok (`Ptime timestamp) -> Ptime.pp fmt timestamp

let check_time t path =
  let open Lwt.Infix in
  Chamelon.last_modified t path >>= function
  | Error _ ->
    Stdlib.Format.eprintf "uh, %a doesn't seem to exist?\n%!" Mirage_kv.Key.pp path;
    Lwt.return @@ Error `No_last_modified
  | Ok (d, ps) ->
    match Ptime.Span.of_d_ps (d, ps) with
    | None ->
      Lwt.return @@ Error `Unparseable_span
    | Some span ->
      match Ptime.of_span span with
      | None ->
        Lwt.return @@ Ok (`Span span)
      | Some timestamp ->
        Lwt.return @@ Ok (`Ptime timestamp)

let ls timestamp image block_size path =
  let open Lwt.Infix in
  Lwt_main.run @@ (
  Mirage_block.connect image >>= fun block ->
  Chamelon.connect block ~program_block_size:16 ~block_size >>= function
  | Error _ -> Stdlib.Format.eprintf "Error doing the initial filesystem ls\n%!"; exit 1
  | Ok t ->
    Chamelon.list t (Mirage_kv.Key.v path) >>= function
    | Error (`Value_expected key) -> Stdlib.Format.eprintf "A component of the path %s was not as expected: %a\n%!" path Mirage_kv.Key.pp key; exit 1
    | Error (`Not_found key) -> begin
      (* if the key is a value, we'd like to output its information *)
      Chamelon.exists t (Mirage_kv.Key.v path) >>= function
      | Ok None -> 
        Stdlib.Format.eprintf "key %a not found\n%!" Mirage_kv.Key.pp key; exit 1
      | Error _ ->
        Stdlib.Format.eprintf "error attempting to find %a\n%!" Mirage_kv.Key.pp key; exit 2
      | Ok (Some ty) ->
        check_time t (Mirage_kv.Key.v path) >>= fun time ->
        Stdlib.Format.printf "%a: %s (%a)\n%!" pp_ty ty path pp_time time;
        Lwt.return_unit
    end
    | Error _ -> Stdlib.Format.eprintf "filesystem was opened, but ls failed\n%!"; exit 2
    | Ok l ->
      let pp fmt (name, key_or_dict) =
        Stdlib.Format.fprintf fmt "%s : %a" name pp_ty key_or_dict
      in
      let print (name, key_or_dict) =
        if timestamp then begin
          let fullpath = Mirage_kv.Key.(append (v path) (v name)) in
          check_time t fullpath >>= fun time ->
          Stdlib.Format.printf "%a (%a) \n%!" pp (name, key_or_dict) pp_time time;
          Lwt.return_unit
        end else begin
          Stdlib.Format.printf "%a\n%!" pp (name, key_or_dict);
          Lwt.return_unit
        end
      in
      Lwt_list.iter_p print l
   )
   
let () =
  let go = Cmdliner.Term.(const ls $ timestamp $ image $ block_size $ path) in
  Cmdliner.Term.(exit @@ eval (go, info "lfs_ls"))
