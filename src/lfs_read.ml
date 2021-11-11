module Mirage_block = Block (* disambiguate this from Littlefs.Block *)
module Littlefs = Fs.Make(Mirage_block)

let image =
  let doc = "path to the filesystem image" in
  Cmdliner.Arg.(value & pos 0 string "./littlefs.img" & info ~doc ~docv:"IMAGE" [])

let block_size =
  let doc = "block size of the filesystem in bytes" in
  Cmdliner.Arg.(value & pos 1 int 4096 & info ~doc ~docv:"BLOCK_SIZE" [])

let path =
  let doc = "path to write. Currently any missing hierarchy will not be created,
             and indeed no hierarchy is supported. In effect, this is just a filename." in
  Cmdliner.Arg.(value & pos 2 string "example" & info ~doc ~docv:"PATH" [])

let read image block_size path =
  let open Lwt.Infix in
  Lwt_main.run @@ (
  Mirage_block.connect image >>= fun block ->
  Littlefs.connect block ~program_block_size:16 ~block_size >>= function
  | Error _ -> Stdlib.Format.eprintf "Error doing the initial filesystem read\n%!"; exit 1
  | Ok t ->
    Littlefs.get t (Mirage_kv.Key.v path) >>= function
    | Ok v -> Stdlib.Format.printf "%s%!" v; Lwt.return_unit
    | Error (`Not_found _key) -> Stdlib.Format.eprintf "key %s not found\n%!" path; exit 1
    | Error _ -> Stdlib.Format.eprintf "filesystem was opened, but read failed\n%!"; exit 2
)

let () =
  let go = Cmdliner.Term.(const read $ image $ block_size $ path) in
  Cmdliner.Term.(exit @@ eval (go, info "lfs_read"))
