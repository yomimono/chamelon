module Mirage_sectors = Block (* disambiguate this from Littlefs.Block *)
module Littlefs = Kv.Make(Mirage_sectors)

let program_block_size =
  let doc = "program block size in bytes" in
  Cmdliner.Arg.(value & opt int 16 & info ~doc ["p"; "program-block-size"])

let block_size =
  let doc = "block size in bytes" in
  Cmdliner.Arg.(value & opt int 4096 & info ~doc ["b"; "block-size"])

let file =
  let doc = "file to format as littlefs filesystem" in
  Cmdliner.Arg.(value & pos 0 string "littlefs.img" & info ~doc [])

let format program_block_size block_size file =
  let aux () =
    let open Lwt.Infix in
    Mirage_sectors.connect file >>= fun sectors ->
    Littlefs.connect ~program_block_size ~block_size sectors >>= function
    | Error _fs_error -> Stdlib.Format.eprintf "error connecting to the sector device\n%!"; exit 1
    | Ok fs ->

      Printf.printf "Formatting %s as a littlefs filesystem with block size %d\n%!" file block_size;
      Littlefs.format fs >|= function
      | Error _ -> Stdlib.Format.eprintf "error writing the filesystem to disk\n%!"; exit 1
      | r -> r
  in
  Lwt_main.run @@ aux ()

let () =
  let go = Cmdliner.Term.(const format $ program_block_size $ block_size $ file) in
  Cmdliner.Term.(exit @@ eval (go, info "format"))
