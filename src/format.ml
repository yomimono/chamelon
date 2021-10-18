module Mirage_block = Block (* disambiguate this from Littlefs.Block *)
module Littlefs = Fs.Make(Mirage_block)

let block_size =
  let doc = "block size in bytes" in
  Cmdliner.Arg.(value & opt int 4096 & info ~doc ["b"; "block-size"])

let file =
  let doc = "file to format as littlefs filesystem" in
  Cmdliner.Arg.(value & pos 0 string "littlefs.img" & info ~doc [])

let format block_size file =
  let aux () =
    let open Lwt.Infix in
    Mirage_block.connect file >>= fun block ->
    Littlefs.format block ~block_size:(Int32.of_int block_size) >|= function
    | Error (`Block_write e) as orig -> Format.eprintf "%a" Mirage_block.pp_write_error e; orig
    | r -> r
  in
  Lwt_main.run @@ aux ()

let () =
  let go = Cmdliner.Term.(const format $ block_size $ file) in
  Cmdliner.Term.(exit @@ eval (go, info "format"))
