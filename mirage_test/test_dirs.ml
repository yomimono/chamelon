(* implementation of a subset of littlefs's tests/test_dirs.toml *)

module Chamelon = Kv.Make(Block)(Pclock)
open Lwt.Infix

let fail pp e = Lwt.fail_with (Format.asprintf "%a" pp e)

let fail_read = fail Chamelon.pp_error
let fail_write = fail Chamelon.pp_write_error

let testable_key = Alcotest.testable Mirage_kv.Key.pp Mirage_kv.Key.equal

let program_block_size = 16
(* let block_size = 512 *)

let rec do_many fs ~f = function
  | n when n <= 0 -> Lwt.return_unit
  | n -> f fs n >>= fun () -> do_many fs ~f (n-1)

let just_mount block =
  Chamelon.connect ~program_block_size block >>= function
  | Error e -> fail_read e
  | Ok fs -> Lwt.return fs

let format_and_mount block =
  Chamelon.format ~program_block_size block >>= function
  | Error e -> fail_write e
  | Ok () -> just_mount block

let assert_slash_empty fs =
  Chamelon.list fs Mirage_kv.Key.empty >>= function
  | Error e -> fail_read e
  | Ok l ->
    Format.printf "%a\n%!" Fmt.(list ~sep:comma string) (List.map fst l);
    Alcotest.(check int) "fs should be empty after deleting everything" 0 (List.length l); Lwt.return_unit

(* root *)
let test_root block _ () =
  let key = Mirage_kv.Key.empty in
  format_and_mount block >>= fun fs ->
  Chamelon.list fs key >>= function
  | Error _ -> Alcotest.fail "couldn't list /"
  | Ok _ ->
    (* much of this involves things like "directory open" or relative naming
     * of special directories like ".." and ".",
     * which are not concepts applicable to kv *)
    (* ergo, short test :) *)
    Lwt.return_unit

let write_in_dir fs n =
  let s = string_of_int n in
  let key = Mirage_kv.Key.(v s / s) in
  Chamelon.set fs key s >|= Result.get_ok

let write fs n =
  let s = string_of_int n in
  Chamelon.set fs Mirage_kv.Key.(v s) s >|= Result.get_ok

let delete_dir fs n =
  let s = string_of_int n in
  let key = Mirage_kv.Key.(v s) in
  Chamelon.remove fs key >|= Result.get_ok

let delete fs n =
  let s = string_of_int n in
  Chamelon.remove fs Mirage_kv.Key.(v s) >|= Result.get_ok

let list fs ~ty n =
  let s = string_of_int n in
  Chamelon.list fs Mirage_kv.Key.empty >|= Result.get_ok >>= fun l ->
  let matching = List.filter
      (fun (name, val_or_dict) -> String.equal name s && val_or_dict = ty)
      l in
  Alcotest.(check int) "each directory appears once in ls /" 1 (List.length matching);
  Lwt.return_unit

(* many directory creation *)
let test_many_dir_creation block _ () =
  format_and_mount block >>= fun fs ->
  (* empty directories aren't a thing in mirage-kv,
   * so instead of creating an empty directory we put keys
   * into their own dictionaries *)
  do_many fs ~f:write_in_dir 100 >>= fun () ->
  (* original test unmounts here; we don't have a disconnect,
   * but we can re-mount *)
  just_mount block >>= fun fs ->
  (* every directory should be in the list from / *)
  do_many fs ~f:(list ~ty:`Dictionary) 100 >>= fun () ->
  Lwt.return_unit

(* many directory removal *)
let test_many_dir_removal block _ () =
  format_and_mount block >>= fun fs ->
  (* as in creation, make a bunch of directories *)
  do_many fs ~f:write_in_dir 100 >>= fun () ->
  (* remount *)
  just_mount block >>= fun fs ->
  (* make sure those directories are listed from / *)
  do_many fs ~f:(list ~ty:`Dictionary) 100 >>= fun () ->
  (* now remount again *)
  just_mount block >>= fun fs ->
  (* delete all the directories *)
  do_many fs ~f:delete_dir 100 >>= fun () ->
  (* remount again *)
  just_mount block >>= fun fs ->
  (* check that ls / returns nothing *)
  assert_slash_empty fs

(* many directory rename *)
(* mirage-kv doesn't support a rename operation, so no test here *)

(* reentrant many directory creation/rename/removal *)
(* TODO we *should* test this to ensure that the global write lock is
 * working as intended, and all mutating paths are guarded by it *)

(* file creation *)
let test_file_creation block _ () =
  format_and_mount block >>= fun fs ->
  (* now we want 100 files in / *)
  do_many fs ~f:write 100 >>= fun () ->
  (* remount *)
  just_mount block >>= fun fs ->
  (* we should have Value entry in / for everything we just wrote *)
  do_many fs ~f:(list ~ty:`Value) 100 >>= fun () ->
  Lwt.return_unit

(* file removal *)
let test_file_removal block _ () =
  format_and_mount block >>= fun fs ->
  (* now we want 100 files in / *)
  do_many fs ~f:write 100 >>= fun () ->
  (* remount *)
  just_mount block >>= fun fs ->
  (* make sure everything's there *)
  do_many fs ~f:(list ~ty:`Value) 100 >>= fun () ->
  (* remount *)
  just_mount block >>= fun fs ->
  (* delete all those files *)
  do_many fs ~f:delete 100 >>= fun () ->
  (* remount *)
  just_mount block >>= fun fs ->
  (* now ls / should be empty *)
  assert_slash_empty fs

(* file rename *)
(* no rename in mirage-kv, so nothing to do here *)

(* reentrant file creation/rename/removal *)
(* TODO much like the directory case we should be sure this works *)

(* nested directories *)
(* recursive remove *)

(* other error cases *)

(* directory seek *)
(* root seek *)

let test img =
  Logs.set_level (Some Logs.Debug);
  Logs.set_reporter @@ Logs_fmt.reporter ();
  let open Alcotest_lwt in
  let open Lwt.Infix in
  Lwt_main.run @@ (
    Block.connect ~prefered_sector_size:(Some 512) img >>= fun block ->
    run "directories" [
      ("from_toml", [
          test_case "root" `Quick (test_root block);
          test_case "many directory creation" `Quick (test_many_dir_creation block);
          test_case "many directory removal" `Quick (test_many_dir_removal block);
          test_case "file creation" `Quick (test_file_creation block);
          test_case "file removal" `Quick (test_file_removal block);
        ])
    ])


let () = test "emptyfile"
