module Mirage_block = Block (* disambiguate this from Littlefs.Block *)
module Littlefs = Kv.Make(Mirage_block)

let image =
  let doc = "path to the filesystem image" in
  Cmdliner.Arg.(value & pos 0 string "./littlefs.img" & info ~doc ~docv:"IMAGE" [])

let block_size =
  let doc = "block size of the filesystem in bytes" in
  Cmdliner.Arg.(value & pos 1 int 4096 & info ~doc ~docv:"BLOCK_SIZE" [])

let path =
  let doc = "path to list. Currently any missing hierarchy will not be created,
             and indeed no hierarchy is supported. In effect, this is just a filename." in
  Cmdliner.Arg.(value & pos 2 string "example" & info ~doc ~docv:"PATH" [])

let ls image block_size path =
  let open Lwt.Infix in
  Lwt_main.run @@ (
  Mirage_block.connect image >>= fun block ->
  Littlefs.connect block ~program_block_size:16 ~block_size >>= function
  | Error _ -> Stdlib.Format.eprintf "Error doing the initial filesystem ls\n%!"; exit 1
  | Ok t ->
    Littlefs.list t (Mirage_kv.Key.v path) >>= function
    | Error (`Not_found _key) -> Stdlib.Format.eprintf "key %s not found\n%!" path; exit 1
    | Error (`Value_expected _key) -> Stdlib.Format.eprintf "%s isn't bound to a key\n%!" path; exit 1
    | Error _ -> Stdlib.Format.eprintf "filesystem was opened, but ls failed\n%!"; exit 2
    | Ok l ->
      let pp fmt (name, key_or_dict) =
        let pp_ty fmt = function
          | `Value -> Stdlib.Format.fprintf fmt "%s" "file"
          | `Dictionary -> Stdlib.Format.fprintf fmt "%s" "directory"
        in
        Stdlib.Format.fprintf fmt "%s : %a" name pp_ty key_or_dict
      in
      List.iter (fun e -> Stdlib.Format.printf "%a\n%!" pp e) l
    ; Lwt.return_unit
)

let () =
  let go = Cmdliner.Term.(const ls $ image $ block_size $ path) in
  Cmdliner.Term.(exit @@ eval (go, info "lfs_ls"))
