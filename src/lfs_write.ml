let image =
  let doc = "path to the filesystem image" in
  Cmdliner.Arg.(value & pos 0 string "./littlefs.img" & info ~doc ~docv:"IMAGE" [])

let block_size =
  let doc = "block size for the filesystem" in
  Cmdliner.Arg.(value & pos 1 int 4096 & info ~doc ~docv:"BLOCK_SIZE" [])

let path =
  let doc = "path to write. Currently any missing hierarchy will not be created,
             and indeed no hierarchy is supported. In effect, this is just a filename." in
  Cmdliner.Arg.(value & pos 2 string "example" & info ~doc ~docv:"PATH" [])

let data =
  let doc = "data to write to the file. Currently this must be small enough to fit in an inline data structure. Multiple arguments can be given but will be straightforwardly concatenated with no separator." in
  Cmdliner.Arg.(value & pos_all string [] & info ~doc ~docv:"DATA" [])

let write _image _block_size _path _data =
  ()

let () =
  let go = Cmdliner.Term.(const write $ image $ block_size $ path $ data) in
  Cmdliner.Term.(exit @@ eval (go, info "lfs_write"))
