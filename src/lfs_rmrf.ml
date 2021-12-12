module Mirage_block = Block (* disambiguate this from Littlefs.Block *)
module Littlefs = Kv.Make(Mirage_block)(Pclock)

let image =
  let doc = "path to the filesystem image" in
  Cmdliner.Arg.(value & pos 0 string "./littlefs.img" & info ~doc ~docv:"IMAGE" [])

let block_size =
  let doc = "block size of the filesystem in bytes" in
  Cmdliner.Arg.(value & pos 1 int 4096 & info ~doc ~docv:"BLOCK_SIZE" [])

let path =
  let doc = "path to remove. Note this is a recursive deletion." in
  Cmdliner.Arg.(value & pos 2 string "example" & info ~doc ~docv:"PATH" [])

let setup_log style_renderer level =
  Fmt_tty.setup_std_outputs ?style_renderer ();
  Logs.set_level level;
  Logs.set_reporter (Logs_fmt.reporter ());
  ()

let setup_log =
  Cmdliner.Term.(const setup_log $ Fmt_cli.style_renderer () $ Logs_cli.level ())

let rmrf image block_size path () =
  let open Lwt.Infix in
  Lwt_main.run @@ (
  Mirage_block.connect image >>= fun block ->
  Littlefs.connect block ~program_block_size:16 ~block_size >>= function
  | Error _ -> Stdlib.Format.eprintf "Error doing the initial filesystem connection\n%!"; exit 1
  | Ok t ->
    Littlefs.remove t (Mirage_kv.Key.v path) >>= function
    | Ok () ->
      Logs.debug (fun m -> m "successfully deleted %s" path);
      Lwt.return @@ Ok ()
    | Error (`Not_found k) ->
      Logs.debug (fun m -> m "%a wasn't found, so we won't delete %s"
                     Mirage_kv.Key.pp k path);
      Lwt.return @@ Ok ()
    | Error (`Dictionary_expected k) ->
      Stdlib.Format.eprintf "%a is not a dictionary and elements could not be removed from it\n%!" Mirage_kv.Key.pp k;
      exit 1
    | Error `No_space -> 
      Stdlib.Format.eprintf "no space available to execute deletion\n%!";
      exit 1
    | Error (`Value_expected k) ->
      (* it's unclear to me how we'd end up here, which means we definitely want a clear error message *)
      Stdlib.Format.eprintf "value expected for %a but there wasn't one\n%!" Mirage_kv.Key.pp k;
      exit 1
    | Error (`Too_many_retries i) ->
      Stdlib.Format.eprintf "couldn't execute deletion batch after %d tries\n%!" i;
      exit 1
    | Error _ -> Stdlib.Format.eprintf "oh no"; exit 1
)

let () =
  let go = Cmdliner.Term.(const rmrf $ image $ block_size $ path $ setup_log ) in
  Cmdliner.Term.(exit @@ eval (go, info "lfs_rmrf"))
