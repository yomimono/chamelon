module Mirage_sectors = Block (* disambiguate this from Littlefs.Block *)
module Littlefs = Kv.Make(Mirage_sectors) (* Pclock comes from mirage-ptime *)

let format {Common_options.program_block_size; block_size; image} =
  let aux () =
    let open Lwt.Infix in
    Mirage_sectors.connect ~prefered_sector_size:(Some block_size) image >>= fun sectors ->
    Mirage_sectors.get_info sectors >>= fun info ->
    let block_size = info.Mirage_block.sector_size in
    Printf.printf "Formatting %s as a littlefs filesystem with block size %d\n%!" image block_size;
    Littlefs.format ~program_block_size sectors >|= function
    | Error _ -> Format.eprintf "error writing the filesystem to disk\n%!"; exit 1
    | Ok r -> r
  in
  Lwt_main.run @@ aux ()

let mount {Common_options.image; block_size; program_block_size} =
  let open Lwt.Infix in
  Mirage_sectors.connect ~prefered_sector_size:(Some block_size) image >>= fun block ->
  Littlefs.connect block ~program_block_size >>= function
  | Error _ -> Format.eprintf "Error doing the initial filesystem read\n%!"; exit 1
  | Ok t -> Lwt.return t

let parse common_options =
  let open Lwt.Infix in
  Lwt_main.run @@ (
    mount common_options >>= fun t ->
    Littlefs.dump Format.std_formatter t
  )

let read common_options path =
  let open Lwt.Infix in
  Lwt_main.run @@ (
    mount common_options >>= fun t ->
    Littlefs.get t (Mirage_kv.Key.v path) >>= function
    | Ok v -> Format.printf "%s%!" v; Lwt.return_unit
    | Error (`Not_found _key) -> Format.eprintf "key %s not found\n%!" path; exit 1
    | Error (`Value_expected _key) -> Format.eprintf "%s isn't bound to a key\n%!" path; exit 1
    | Error _ -> Format.eprintf "filesystem was opened, but read failed\n%!"; exit 2
  )

let remove common_options path =
  let open Lwt.Infix in
  Lwt_main.run @@ (
    mount common_options >>= fun t ->
    Littlefs.remove t (Mirage_kv.Key.v path) >>= function
    | Ok () ->
      Logs.debug (fun m -> m "successfully deleted %s" path);
      Lwt.return_unit
    | Error (`Not_found k) ->
      Logs.debug (fun m -> m "%a wasn't found, so we won't delete %s"
                     Mirage_kv.Key.pp k path);
      Lwt.return_unit
    | Error (`Dictionary_expected k) ->
      Format.eprintf "%a is not a dictionary and elements could not be removed from it\n%!" Mirage_kv.Key.pp k;
      exit 1
    | Error `No_space ->
      Format.eprintf "no space available to execute deletion\n%!";
      exit 1
    | Error (`Value_expected k) ->
      (* it's unclear to me how we'd end up here, which means we definitely want a clear error message *)
      Format.eprintf "value expected for %a but there wasn't one\n%!" Mirage_kv.Key.pp k;
      exit 1
    | Error e -> Format.eprintf "error deleting: %a" Littlefs.pp_write_error e; exit 1
  )

let write common_options path data =
  let open Lwt.Infix in
  Lwt_main.run @@ (
    mount common_options >>= fun t ->
    let data =
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

let common_options_t =
  let block_size =
    let doc = "block size of the filesystem in bytes" in
    Cmdliner.Arg.(value & pos 1 int 4096 & info ~doc ~docv:"BLOCK_SIZE" [])
  in
  let image =
    let doc = "path to the filesystem image" in
    Cmdliner.Arg.(value & pos 0 string "./littlefs.img" & info ~doc ~docv:"IMAGE" [])
  in
  let program_block_size =
    let doc = "program block size in bytes" in
    Cmdliner.Arg.(value & opt int 16 & info ~doc ["p"; "program-block-size"])
  in
  let setup_log style_renderer level =
    Fmt_tty.setup_std_outputs ?style_renderer ();
    Logs.set_level level;
    Logs.set_reporter (Logs_fmt.reporter ());
    ()
  in
  let setup_log = Cmdliner.Term.(const setup_log $ Fmt_cli.style_renderer () $ Logs_cli.level ()) in
  Cmdliner.Term.(const Common_options.set_options $ block_size $ program_block_size $ image $ setup_log)

let path =
  Cmdliner.Arg.(value & pos 2 string "/example" & info ~doc:"path" ~docv:"PATH" [])

let format_command =
  let info = Cmdliner.Cmd.info "format" ~doc:"format a file as a chamelon filesystem" in
  Cmdliner.Cmd.v info Cmdliner.Term.(const format $ common_options_t)

let list_command =
  let timestamp =
    let doc = "output last-modified information when available" in
    Cmdliner.Arg.(value & flag & info ~doc ~docv:"TIMESTAMP" ["t"; "timestamp"])
  in
  let info = Cmdliner.Cmd.info "ls" ~doc:"list the contents of a path in a filesystem" in
  Cmdliner.Cmd.v info Cmdliner.Term.(const Lfs_ls.ls $ common_options_t $ timestamp $ path)

let read_command =
  let doc = "put file contents on stdout" in
  let info = Cmdliner.Cmd.info "read" ~doc in
  Cmdliner.Cmd.v info Cmdliner.Term.(const read $ common_options_t $ path)

let remove_command =
  let doc = "recursively remove a path and all items within it" in
  let info = Cmdliner.Cmd.info "rm" ~doc in
  Cmdliner.Cmd.v info Cmdliner.Term.(const remove $ common_options_t $ path)

let write_command =
  let data =
    let doc = "data to write to the file. Provide - for stdin." in
    Cmdliner.Arg.(value & pos 3 string "-" & info ~doc ~docv:"DATA" [])
  in
  let doc = "write a file" in
  let info = Cmdliner.Cmd.info "write" ~doc in
  Cmdliner.Cmd.v info Cmdliner.Term.(const write $ common_options_t $ path $ data)

let parse_command =
  let doc = "attempt to parse the filesystem and display all known data from doing so" in
  let info = Cmdliner.Cmd.info "parse" ~doc in
  Cmdliner.Cmd.v info Cmdliner.Term.(const parse $ common_options_t)

let main_cmd =
  let doc = "filesystem operations on chamelon images" in
  let info = Cmdliner.Cmd.info "chamelon" ~doc in
  let default = Cmdliner.Term.(ret (const
                                      (fun _ -> `Help (`Pager, None)) $ common_options_t)) in
  Cmdliner.Cmd.group info ~default [format_command;
                                    list_command;
                                    read_command;
                                    write_command;
                                    remove_command;
                                    parse_command;
                                   ]

let () = exit (Cmdliner.Cmd.eval main_cmd)
