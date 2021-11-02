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

let data =
  let doc = "data to write to the file. Currently this must be small enough to fit in an inline data structure. Multiple arguments can be given but will be straightforwardly concatenated with no separator." in
  Cmdliner.Arg.(value & pos_right 2 string [] & info ~doc ~docv:"DATA" [])

let write image block_size path data =
  let open Lwt.Infix in
  Lwt_main.run @@ (
  Mirage_block.connect image >>= fun block ->
  Littlefs.connect block ~program_block_size:16l ~block_size >>= function
  | Error _ -> Stdlib.Format.eprintf "Error doing the initial filesystem read\n%!"; exit 1
  | Ok t ->
    Littlefs.write t path @@ List.map Cstruct.of_string data >>= function
    | Ok () -> Stdlib.Format.printf "wrote some bytes\n%!"; Lwt.return_unit
    | Error _ -> Stdlib.Format.eprintf "Filesystem was opened, but write failed\n%!";
      exit 1
)

let () =
  let go = Cmdliner.Term.(const write $ image $ block_size $ path $ data) in
  Cmdliner.Term.(exit @@ eval (go, info "lfs_write"))
