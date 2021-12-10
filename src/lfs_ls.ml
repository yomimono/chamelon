module Mirage_block = Block (* disambiguate this from Littlefs.Block *)
module Littlefs = Kv.Make(Mirage_block)(Pclock)

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

let ls image block_size path =
  let open Lwt.Infix in
  Lwt_main.run @@ (
  Mirage_block.connect image >>= fun block ->
  Littlefs.connect block ~program_block_size:16 ~block_size >>= function
  | Error _ -> Stdlib.Format.eprintf "Error doing the initial filesystem ls\n%!"; exit 1
  | Ok t ->
    Littlefs.list t (Mirage_kv.Key.v path) >>= function
    | Error (`Value_expected _key) -> Stdlib.Format.eprintf "%s isn't bound to a key\n%!" path; exit 1
    | Error (`Not_found key) -> begin
      Littlefs.exists t (Mirage_kv.Key.v path) >>= function
      | Ok None -> 
        Stdlib.Format.eprintf "key %a not found\n%!" Mirage_kv.Key.pp key; exit 1
      | Error _ ->
        Stdlib.Format.eprintf "error attempting to find %a\n%!" Mirage_kv.Key.pp key; exit 2
      | Ok (Some ty) ->
        Littlefs.last_modified t (Mirage_kv.Key.v path) >>= function
        | Error _ ->
          Stdlib.Format.printf "%a : %s\n%!" pp_ty ty path;
          Lwt.return_unit
        | Ok (d, ps) ->
          match Ptime.Span.of_d_ps (d, ps) with
          | None ->
            Stdlib.Format.printf "%a : %s\n%!" pp_ty ty path;
            Lwt.return_unit
          | Some span ->
            match Ptime.of_span span with
            | None ->
              Stdlib.Format.printf "%a: %s (%a)\n%!" pp_ty ty path Ptime.Span.pp span;
              Lwt.return_unit
            | Some timestamp ->
              Stdlib.Format.printf "%a: %s (%a)\n%!" pp_ty ty path Ptime.pp timestamp;
              Lwt.return_unit
    end
    | Error _ -> Stdlib.Format.eprintf "filesystem was opened, but ls failed\n%!"; exit 2
    | Ok l ->
      let pp fmt (name, key_or_dict) =
        Stdlib.Format.fprintf fmt "%s : %a" name pp_ty key_or_dict
      in
      List.iter (fun e -> Stdlib.Format.printf "%a\n%!" pp e) l
    ; Lwt.return_unit
)

let () =
  let go = Cmdliner.Term.(const ls $ image $ block_size $ path) in
  Cmdliner.Term.(exit @@ eval (go, info "lfs_ls"))
