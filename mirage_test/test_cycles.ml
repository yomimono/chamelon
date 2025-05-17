module Chamelon_lib = Chamelon
module Chamelon = Kv.Make(Block)
open Lwt.Infix

let fail pp e = Lwt.fail_with (Format.asprintf "%a" pp e)

let fail_read = fail Chamelon.pp_error
let fail_write = fail Chamelon.pp_write_error

let testable_key = Alcotest.testable Mirage_kv.Key.pp Mirage_kv.Key.equal

let program_block_size = 16
let block_size = 512

let just_mount block =
  Chamelon.connect ~program_block_size block >>= function
  | Error e -> fail_read e
  | Ok fs -> Lwt.return fs

let format_and_mount block =
  Chamelon.format ~program_block_size block >>= function
  | Error e -> fail_write e
  | Ok () -> just_mount block

(* simple cycle: a directory with an entry referring to its own blockpair *)
let simple_cycle block _ () =
  let id = 1 in
  (* nothing fishy about this bit, just an id and name *)
  let label = Chamelon_lib.Dir.name "self" id in
  (* but we make a reference back to root, which shouldn't be allowed anywhere in the filesystem *)
  let rootref = Chamelon_lib.Dir.mkdir ~to_pair:(0L, 1L) id in
  let new_commit = [label; rootref] in
  (* get a fresh, empty filesystem *)
  format_and_mount block >>= fun fs ->
  let block_0 = Cstruct.create block_size in
  (* read the raw block contents of the root directory *)
  Block.read block 0L [Cstruct.create block_size] >>= function
  | Error _ -> Alcotest.fail "read failure on block 0"
  | Ok () ->
    (* parse the raw block contents into a LittleFS block *)
    match Chamelon_lib.Block.of_cstruct ~program_block_size block_0 with
    | Error (`Msg s) -> Alcotest.failf "parsing root directory before alteration: %s" s
    | Ok parsed_root_block ->
      (* add our cyclical reference commit to the root block *)
      let altered_block = Chamelon_lib.Block.add_commit parsed_root_block new_commit in
      (* properly serialize the block to a cstruct for writing to the block device *)
      match Chamelon_lib.Block.to_cstruct ~program_block_size ~block_size altered_block with
      | _, `Unwriteable -> Alcotest.fail "attempted write was unwriteable"
      | _, `Split | _, `Split_emergency -> Alcotest.failf "first write gave us a split"
      | new_block_zero, `Ok ->
        (* write the serialized block *)
        Block.write block 0L [new_block_zero] >>= function
        | Error _ -> Alcotest.fail "raw block write fail"
        | Ok () ->
          Block.write block 1L [new_block_zero] >>= function
          | Error _ -> Alcotest.fail "raw block write fail"
          | Ok () ->
            (* unmount the filesystem *)
            Chamelon.disconnect fs >>= fun () ->
            (* try remounting the filesystem; this should fail *)
            Chamelon.connect ~program_block_size block >>= function
            | Error _ -> Lwt.return_unit
            | Ok _fs ->
              Alcotest.fail "was able to mount a filesystem with a simple cycle in it"

let test img =
  Logs.set_level (Some Logs.Debug);
  Logs.set_reporter @@ Logs_fmt.reporter ();
  let open Alcotest_lwt in
  let open Lwt.Infix in
  Lwt_main.run @@ (
    Block.connect ~prefered_sector_size:(Some block_size) img >>= fun block ->
    run "cycles" [
      ("simple", [
          test_case "simple" `Quick (simple_cycle block);
        ])
    ])


let () = test "emptyfile"
