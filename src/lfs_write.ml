module Mirage_block = Block (* disambiguate this from Littlefs.Block *)
module Littlefs = Kv.Make(Mirage_block)

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
  let doc = "data to write to the file. Provide - for stdin." in
  Cmdliner.Arg.(value & pos 3 string "-" & info ~doc ~docv:"DATA" [])

let setup_log style_renderer level =
  Fmt_tty.setup_std_outputs ?style_renderer ();
  Logs.set_level level;
  Logs.set_reporter (Logs_fmt.reporter ());
  ()

let setup_log =
  Cmdliner.Term.(const setup_log $ Fmt_cli.style_renderer () $ Logs_cli.level ())

let write image block_size path data _log_unit =
  let open Lwt.Infix in
  Lwt_main.run @@ (
  Mirage_block.connect image >>= fun block ->
  Littlefs.connect block ~program_block_size:16 ~block_size >>= function
  | Error _ -> Logs.err (fun m -> m "Error doing the initial filesystem read\n%!"); exit 1
  | Ok t -> let data =
    if String.equal data "-" then begin
        match Bos.OS.File.(read dash) with
        | Error _ -> Logs.err (fun m -> m "couldn't understand what I should write\n%!"); exit 1
        | Ok data -> data
      end else data
    in
    Littlefs.set t (Mirage_kv.Key.v path) data >>= function
    | Ok () -> Logs.debug (fun m -> m "successfully wrote to %s" path);
      Lwt.return_unit
    | Error e -> Logs.err (fun m -> m "%a" Littlefs.pp_write_error e);
      exit 1
)

let () =
  let go = Cmdliner.Term.(const write $ image $ block_size $ path $ data $ setup_log ) in
  Cmdliner.Term.(exit @@ eval (go, info "lfs_write"))
