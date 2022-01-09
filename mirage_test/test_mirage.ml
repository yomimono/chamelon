module Chamelon = Kv.Make(Block)(Pclock)
open Lwt.Infix

let fail pp e = Lwt.fail_with (Format.asprintf "%a" pp e)

let fail_read = fail Chamelon.pp_error
let fail_write = fail Chamelon.pp_write_error

let testable_key = Alcotest.testable Mirage_kv.Key.pp Mirage_kv.Key.equal

let program_block_size = 16
let block_size = 4096

let format_and_mount block =
  Chamelon.format ~program_block_size ~block_size block >>= function
  | Error e -> fail_write e
  | Ok () ->
    Chamelon.connect ~program_block_size ~block_size block >>= function
    | Error e -> fail_read e
    | Ok fs -> Lwt.return fs

let test_format block _ () =
  Chamelon.format ~program_block_size ~block_size block >>= function
  | Error e -> fail_write e
  | Ok () ->
    Chamelon.connect ~program_block_size ~block_size block >>= function
    | Error e -> Alcotest.failf "couldn't mount the filesystem after formatting it: %a" Chamelon.pp_error e
    | Ok fs ->
      Chamelon.list fs (Mirage_kv.Key.v "/") >>= function
      | Error e -> fail_read e
      | Ok l ->
        Alcotest.(check int) "no entries in just-formatted filesystem" 0 (List.length l);
        Lwt.return_unit

let test_get_set_general path contents block _ () =
  format_and_mount block >>= fun fs ->
  Chamelon.set fs path contents >>= function | Error e -> fail_write e | Ok () ->
  Chamelon.get fs path >>= function | Error e -> fail_read e
  | Ok actual ->
    Alcotest.(check string) "get/set roundtrip for very small file in root" contents actual;
    Chamelon.list fs Mirage_kv.Key.empty >>= function | Error e -> fail_read e
    | Ok l ->
      Alcotest.(check int) "one entry in filesystem" 1 (List.length l);
      Lwt.return_unit

let test_get_set =
  let path = Mirage_kv.Key.v "get set to get wet" in
  let contents = "hell yeah let's do this!!!" in
  test_get_set_general path contents

let test_set_nonascii_data =
  let path = Mirage_kv.Key.v "camel emoji" in
  let contents = "🔋" in
  test_get_set_general path contents

let test_set_nonascii_key =
  let path = Mirage_kv.Key.v "🔋" in
  let contents = "camel" in
  test_get_set_general path contents

let test_set_deep block _ () =
  let slash = Mirage_kv.Key.v "/" in
  let key = Mirage_kv.Key.v "/set/deep/fs/filesystem" in
  let contents = "arglebarglefargle" in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs key contents >>= function | Error e -> fail_write e | Ok () ->
  Chamelon.get fs key >>= function | Error e -> fail_read e | Ok s ->
  Alcotest.(check string) "set it and get it, deep in the fs" contents s;
  Chamelon.list fs slash >>= function | Error e -> fail_read e | Ok l ->
    Alcotest.(check int) (Format.asprintf "size of ls / after setting %a" Mirage_kv.Key.pp key) 1 @@ List.length l;
    let e = List.hd l in
    Alcotest.(check string) "list entry name" "set" (fst e);
    match (snd e) with 
    | `Value -> Alcotest.fail "value where dictionary was expected"
    | `Dictionary ->
      Chamelon.list fs (Mirage_kv.Key.parent key) >>= function
      | Error e -> Lwt.fail_with @@ Format.asprintf "parent directory of %a listing failed: %a" Mirage_kv.Key.pp key Chamelon.pp_error e
      | Ok l ->
        let pp_key = Mirage_kv.Key.pp in
        Alcotest.(check int) (Format.asprintf "size of ls %a after setting %a" pp_key (Mirage_kv.Key.parent key) pp_key key) 1 @@ List.length l;
        let e = List.hd l in
        Alcotest.(check string) "list entry name" "filesystem" (fst e);
        Lwt.return_unit

let test_last_modified block _ () =
  let path = Mirage_kv.Key.v "get set to get wet" in
  let contents = "hell yeah let's do this!!!" in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs path contents >>= function | Error e -> fail_write e | Ok () ->
  Chamelon.get fs path >>= function | Error e -> fail_read e
  | Ok _contents ->
    Chamelon.last_modified fs path >>= function | Error e -> fail_read e | Ok first_write_time ->
      let first_timestamp = Ptime.unsafe_of_d_ps first_write_time in
      let now_timestamp = Pclock.(now_d_ps ()) |> Ptime.unsafe_of_d_ps in
      Alcotest.(check bool) "last modified time is before now" true (Ptime.is_later now_timestamp ~than:first_timestamp);
      Chamelon.set fs path "do it again!!" >>= function | Error e -> fail_write e
      | Ok () ->
        Chamelon.last_modified fs path >>= function | Error e -> fail_read e | Ok second_write_time ->
          let second_timestamp = Ptime.unsafe_of_d_ps second_write_time in
          Alcotest.(check bool) "after modifying, last modified time is later" true
            (Ptime.is_later second_timestamp ~than:now_timestamp);
          Lwt.return_unit

let test_digest_empty block _ () =
  let path1 = Mirage_kv.Key.v "trans" and path2 = Mirage_kv.Key.v "rights" in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs path1 "" >>= function | Error e -> fail_write e | Ok () ->
    Chamelon.digest fs path1 >>= function | Error e -> fail_read e | Ok digest1 ->
    Chamelon.set fs path2 "" >>= function | Error e -> fail_write e | Ok () ->
    Chamelon.digest fs path2 >>= function | Error e -> fail_read e | Ok digest2 ->
      Alcotest.(check string) "digests of two empty files with different keys are the same" digest1 digest2;
      Chamelon.digest fs path1 >>= function | Error e -> fail_read e | Ok digest1_redux ->
        Alcotest.(check string) "digest of a file is not different after unrelated writes" digest1_redux digest2;
        Lwt.return_unit

let test_digest_slash block _ () =
  let slash = Mirage_kv.Key.v "/" in
  let key = Mirage_kv.Key.v "digest" in
  format_and_mount block >>= fun fs ->
  Chamelon.digest fs slash >>= function | Error e -> fail_read e | Ok digest ->
  (* no special meaning to setting the digest, it's just a handy value *)
  Chamelon.set fs key digest >>= function | Error e -> fail_write e | Ok () ->
  Chamelon.digest fs slash >>= function | Error e -> fail_read e | Ok post_write_digest ->
  Alcotest.(check bool) "digest changes after write" false (String.equal post_write_digest digest);
  Lwt.return_unit

let test_digest_overwrite block _ () =
  let deep = Mirage_kv.Key.v "/digest/deep/dir/dictionary" in
  let deep_contents_initial = "arglebarglefargle" in
  let deep_contents_final = "morglemoop" in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs deep deep_contents_initial >>= function | Error e -> fail_write e | Ok () -> 
  Chamelon.digest fs deep >>= function
  | Error e -> Lwt.fail_with @@ Format.asprintf "digest of deep directory key failed on first write: %a" Chamelon.pp_error e
  | Ok digest_key_first_write ->
  Chamelon.set fs deep deep_contents_final >>= function | Error e -> fail_write e | Ok () -> 
  Chamelon.digest fs deep >>= function
  | Error e -> Lwt.fail_with @@ Format.asprintf "digest of deep directory key failed on second write: %a" Chamelon.pp_error e
  | Ok digest_key_second_write ->
    Alcotest.(check bool) "digest of a file should change after overwrite" false (String.equal digest_key_first_write digest_key_second_write);
  Chamelon.set fs deep deep_contents_initial >>= function | Error e -> fail_write e | Ok () -> 
  Chamelon.digest fs deep >>= function
  | Error e -> Lwt.fail_with @@ Format.asprintf "digest of deep directory key failed on third write: %a" Chamelon.pp_error e
  | Ok digest_key_third_write ->
    Alcotest.(check string) "digest of a file should match digest of file with the same contents" digest_key_first_write digest_key_third_write;
      Lwt.return_unit

let test_digest_deep_dictionary block _ () =
  let slash = Mirage_kv.Key.v "/" in
  let deep = Mirage_kv.Key.v "/digest/deep/dictionary" in
  let deep_contents = "arglebarglefargle" in
  let shallow = Mirage_kv.Key.v "/smorgleforg" in
  let shallow_contents = "clinglefrimp" in
  format_and_mount block >>= fun fs ->
  Chamelon.digest fs slash >>= function | Error e -> fail_read e | Ok digest_empty_slash -> 
  Chamelon.set fs deep deep_contents >>= function | Error e -> fail_write e | Ok () -> 
  Chamelon.digest fs slash >>= function | Error e -> fail_read e | Ok digest_slash_post_deep_write ->
  Chamelon.digest fs deep >>= function | Error e -> fail_read e | Ok digest_deep_pre_shallow_write ->
  Chamelon.set fs shallow shallow_contents >>= function | Error e -> fail_write e | Ok () -> 
  Chamelon.digest fs deep >>= function | Error e -> fail_read e | Ok digest_deep_post_shallow_write ->
  Alcotest.(check bool) "setting a key deep in the hierarchy affects the digest of /" false (String.equal digest_empty_slash digest_slash_post_deep_write);
  Alcotest.(check string) "digests of things in the fs hierarchy aren't sensitive to unrelated changes" digest_deep_pre_shallow_write digest_deep_post_shallow_write;
  Lwt.return_unit

let test_no_space block _ () =
  let blorp = String.init 4096 (fun _ -> 'a') in
  let k n = Mirage_kv.Key.v @@ string_of_int n in
  (* filesystem is 10 * 4K in size, so we should expect to write
   * 40K - 2*4K (initial metadata blocks) = 32K; if the files are 4K each,
   * our ninth write should fail *)
  format_and_mount block >>= fun fs ->
  let l = List.init 8 (fun n -> n) in
  Lwt_list.iter_p (fun i ->
    Chamelon.set fs (k i) blorp >>= function | Error e -> fail_write e | Ok () ->
    Lwt.return_unit
  ) l >>= fun () ->
  Chamelon.list fs (Mirage_kv.Key.empty) >>= function | Error e -> fail_read e | Ok l ->
  Alcotest.(check int) "all set items are present" 8 @@ List.length l;
  Chamelon.set fs (k 8) blorp >>= function
  | Error `No_space -> Lwt.return_unit
  | Ok _ -> Alcotest.fail "setting 9th key succeeded when we expected No_space"
  | Error e -> fail_write e

(* we should be able to overwrite dictionaries with values and vice versa *)
let test_overwrite_dictionary block _ () =
  let deep_key = Mirage_kv.Key.v "/fleep/dorp" in
  let key = Mirage_kv.Key.parent deep_key in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs deep_key "" >>= function | Error e -> fail_write e | Ok () -> 
  Chamelon.set fs key "" >>= function
  | Error e -> Alcotest.fail (Format.asprintf "failed to overwrite a dictionary with a value: %a" Chamelon.pp_write_error e)
  | Ok () ->
    Chamelon.set fs deep_key "" >>= function
    | Error e -> Alcotest.fail (Format.asprintf "failed to overwrite a value with a dictionary: %a" Chamelon.pp_write_error e)
    | Ok () -> Lwt.return_unit

let test_nonexistent_value block _ () =
  format_and_mount block >>= fun fs ->
  let key = Mirage_kv.Key.v "/snapglefring" in
  Chamelon.get fs key >>= function
  | Ok s -> Alcotest.fail (Format.asprintf "getting a nonexistent value succeeded and returned %S" s)
  | Error (`Not_found k) -> Alcotest.check testable_key "key returned from not_found is the correct one" key k;
    Lwt.return_unit
  | Error e -> Alcotest.fail (Format.asprintf "getting a nonexistent value failed with an unexpected error type: %a" Chamelon.pp_error e)

let test_get_dictionary block _ () =
  let key = Mirage_kv.Key.v "/hooplemorg/bleeplefroop" in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs key "" >>= function | Error e -> fail_write e | Ok () ->
  Chamelon.get fs (Mirage_kv.Key.parent key) >>= function
  | Error (`Value_expected k) -> Alcotest.check testable_key "getting a dictionary fails on the part that's a dictionary" k (Mirage_kv.Key.parent key);
    Lwt.return_unit
  | Ok s -> Alcotest.fail (Format.asprintf "getting a dictionary succeeded and returned %S" s)
  | Error e -> Alcotest.fail (Format.asprintf "getting a dictionary failed with an unexpected error type: %a" Chamelon.pp_error e)

let test img =
  Logs.set_level (Some Logs.Debug);
  let open Alcotest_lwt in
  let open Lwt.Infix in
  Lwt_main.run @@ (
    Block.connect img >>= fun block ->
    run "mirage-kv" [
      ("format",
       [ test_case "format" `Quick (test_format block) ;
       ]
      );
      ("set",
       [ test_case "get/set roundtrip" `Quick (test_get_set block);
         test_case "get/set roundtrip w/non-ascii data" `Quick (test_set_nonascii_data block);
         test_case "get/set roundtrip w/non-ascii key" `Quick (test_set_nonascii_key block);
         test_case "mkdir -p" `Quick (test_set_deep block);
         test_case "disk full" `Quick (test_no_space block);
         (* test_overwrite_dictionary is disabled for the moment; we need to confirm the "correct" behavior *)
         (* test_case "overwrite dictionary" `Quick (test_overwrite_dictionary block); *)
       ]
      );
      ("last modified",
       [ test_case "last modified increases on overwrite" `Quick (test_last_modified block);
       ]
      );
      ("get",
       [ test_case "get nonexistent value" `Quick (test_nonexistent_value block);
         test_case "get a dictionary" `Quick (test_get_dictionary block);
       ]
      );
      ("digest",
       [ test_case "digest of empty files w/different keys is identical" `Quick (test_digest_empty block);
         test_case "slash digest" `Quick (test_digest_slash block) ;
         test_case "file overwrite digest" `Quick (test_digest_overwrite block) ;
         test_case "dict digest" `Quick (test_digest_deep_dictionary block) ;
       ]
      )
    ]
  )
let () = test "emptyfile"
