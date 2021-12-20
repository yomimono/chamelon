module Littlefs = Kv.Make(Block)(Pclock)
open Lwt.Infix

let fail pp e = Lwt.fail_with (Format.asprintf "%a" pp e)

let fail_read = fail Littlefs.pp_error
let fail_write = fail Littlefs.pp_write_error

let test_format fs _ () =
  Littlefs.format fs >>= function
  | Error e -> fail_write e
  | Ok () ->
    Littlefs.list fs (Mirage_kv.Key.v "/") >>= function
    | Error e -> fail_read e
    | Ok l ->
      Alcotest.(check int) "no entries in just-formatted filesystem" 0 (List.length l);
      Lwt.return_unit

let test_get_set fs _ () =
  let path = Mirage_kv.Key.v "get set to get wet" in
  let contents = "hell yeah let's do this!!!" in
  Littlefs.format fs >>= function | Error e -> fail_write e | Ok () ->
  Littlefs.set fs path contents >>= function | Error e -> fail_write e | Ok () ->
  Littlefs.get fs path >>= function | Error e -> fail_read e
  | Ok actual ->
    Alcotest.(check string) "get/set roundtrip for very small file in root" contents actual;
    Littlefs.list fs Mirage_kv.Key.empty >>= function | Error e -> fail_read e
    | Ok l ->
      Alcotest.(check int) "one entry in filesystem" 1 (List.length l);
      Lwt.return_unit

let test_set_deep fs _ () =
  let slash = Mirage_kv.Key.v "/" in
  let key = Mirage_kv.Key.v "/set/deep/fs/way/down/in/the/filesystem" in
  let contents = "arglebarglefargle" in
  Littlefs.format fs >>= function | Error e -> fail_write e | Ok () ->
  Littlefs.set fs key contents >>= function | Error e -> fail_write e | Ok () ->
  Littlefs.get fs key >>= function | Error e -> fail_read e | Ok s ->
  Alcotest.(check string) "set it and get it, deep in the fs" contents s;
  Littlefs.list fs slash >>= function | Error e -> fail_read e | Ok l ->
    Alcotest.(check int) (Format.asprintf "size of ls / after setting %a" Mirage_kv.Key.pp key) 1 @@ List.length l;
    let e = List.hd l in
    Alcotest.(check string) "list entry name" "set" (fst e);
    match (snd e) with 
    | `Value -> Alcotest.fail "value where dictionary was expected"
    | `Dictionary ->
      Littlefs.list fs (Mirage_kv.Key.parent key) >>= function
      | Error e -> Lwt.fail_with @@ Format.asprintf "parent directory of %a listing failed: %a" Mirage_kv.Key.pp key Littlefs.pp_error e
      | Ok l ->
        let pp_key = Mirage_kv.Key.pp in
        Alcotest.(check int) (Format.asprintf "size of ls %a after setting %a" pp_key (Mirage_kv.Key.parent key) pp_key key) 1 @@ List.length l;
        let e = List.hd l in
        Alcotest.(check string) "list entry name" "filesystem" (fst e);
        Lwt.return_unit

let test_last_modified fs _ () =
  let path = Mirage_kv.Key.v "get set to get wet" in
  let contents = "hell yeah let's do this!!!" in
  Littlefs.format fs >>= function | Error e -> fail_write e | Ok () ->
  Littlefs.set fs path contents >>= function | Error e -> fail_write e | Ok () ->
  Littlefs.get fs path >>= function | Error e -> fail_read e
  | Ok _contents ->
    Littlefs.last_modified fs path >>= function | Error e -> fail_read e | Ok first_write_time ->
      let first_timestamp = Ptime.unsafe_of_d_ps first_write_time in
      let now_timestamp = Pclock.(now_d_ps ()) |> Ptime.unsafe_of_d_ps in
      Alcotest.(check bool) "last modified time is before now" true (Ptime.is_later now_timestamp ~than:first_timestamp);
      Littlefs.set fs path "do it again!!" >>= function | Error e -> fail_write e
      | Ok () ->
        Littlefs.last_modified fs path >>= function | Error e -> fail_read e | Ok second_write_time ->
          let second_timestamp = Ptime.unsafe_of_d_ps second_write_time in
          Alcotest.(check bool) "after modifying, last modified time is later" true
            (Ptime.is_later second_timestamp ~than:now_timestamp);
          Lwt.return_unit

let test_digest_empty fs _ () =
  let path1 = Mirage_kv.Key.v "trans" and path2 = Mirage_kv.Key.v "rights" in
  Littlefs.format fs >>= function | Error e -> fail_write e | Ok () ->
  Littlefs.set fs path1 "" >>= function | Error e -> fail_write e | Ok () ->
    Littlefs.digest fs path1 >>= function | Error e -> fail_read e | Ok digest1 ->
    Littlefs.set fs path2 "" >>= function | Error e -> fail_write e | Ok () ->
    Littlefs.digest fs path2 >>= function | Error e -> fail_read e | Ok digest2 ->
      Alcotest.(check string) "digests of two empty files with different keys are the same" digest1 digest2;
      Littlefs.digest fs path1 >>= function | Error e -> fail_read e | Ok digest1_redux ->
        Alcotest.(check string) "digest of a file is not different after unrelated writes" digest1_redux digest2;
        Lwt.return_unit

let test_digest_slash fs _ () =
  let slash = Mirage_kv.Key.v "/" in
  let key = Mirage_kv.Key.v "digest" in
  Littlefs.format fs >>= function | Error e -> fail_write e | Ok () ->
  Littlefs.digest fs slash >>= function | Error e -> fail_read e | Ok digest ->
  (* no special meaning to setting the digest, it's just a handy value *)
  Littlefs.set fs key digest >>= function | Error e -> fail_write e | Ok () ->
  Littlefs.digest fs slash >>= function | Error e -> fail_read e | Ok post_write_digest ->
  Alcotest.(check bool) "digest changes after write" false (String.equal post_write_digest digest);
  Lwt.return_unit

let test_digest_deep_key fs _ () =
  let slash = Mirage_kv.Key.v "/" in
  let deep = Mirage_kv.Key.v "/digest/deep/dictionary" in
  let deep_contents = "arglebarglefargle" in
  Littlefs.format fs >>= function | Error e -> fail_write e | Ok () ->
  Littlefs.set fs deep deep_contents >>= function | Error e -> fail_write e | Ok () -> 
  Littlefs.digest fs deep >>= function
  | Error e -> Lwt.fail_with @@ Format.asprintf "digest of deep directory key failed: %a" Littlefs.pp_error e
  | Ok digest_key_post_write ->
    Littlefs.digest fs slash >>= function
    | Error e -> Lwt.fail_with @@ Format.asprintf "digest of / failed: %a" Littlefs.pp_error e
    | Ok digest_slash_post_write ->
      Alcotest.(check string) "digest for fs should be digest of only file" digest_key_post_write digest_slash_post_write;
      Lwt.return_unit

let test_digest_deep_dictionary fs _ () =
  let slash = Mirage_kv.Key.v "/" in
  let deep = Mirage_kv.Key.v "/digest/deep/dictionary" in
  let deep_contents = "arglebarglefargle" in
  let shallow = Mirage_kv.Key.v "/smorgleforg" in
  let shallow_contents = "clinglefrimp" in
  Littlefs.format fs >>= function | Error e -> fail_write e | Ok () ->
  Littlefs.digest fs slash >>= function | Error e -> fail_read e | Ok digest_empty_slash -> 
  Littlefs.set fs deep deep_contents >>= function | Error e -> fail_write e | Ok () -> 
  Littlefs.digest fs slash >>= function | Error e -> fail_read e | Ok digest_slash_post_deep_write ->
  Littlefs.digest fs deep >>= function | Error e -> fail_read e | Ok digest_deep_pre_shallow_write ->
  Littlefs.set fs shallow shallow_contents >>= function | Error e -> fail_write e | Ok () -> 
  Littlefs.digest fs deep >>= function | Error e -> fail_read e | Ok digest_deep_post_shallow_write ->
  Alcotest.(check bool) "setting a key deep in the hierarchy affects the digest of /" false (String.equal digest_empty_slash digest_slash_post_deep_write);
  Alcotest.(check string) "digests of things in the fs hierarchy aren't sensitive to unrelated changes" digest_deep_pre_shallow_write digest_deep_post_shallow_write;
  Lwt.return_unit

let test img block_size =
  Logs.set_level (Some Logs.Debug);
  let open Alcotest_lwt in
  let open Lwt.Infix in
  Lwt_main.run @@ (
    Block.connect img >>= fun block ->
    Littlefs.connect block ~program_block_size:16 ~block_size >>= function
    | Error e -> Alcotest.fail (Format.asprintf "%a" Littlefs.pp_error e)
    | Ok fs ->
      run "mirage-kv" [
        ("format",
         [ test_case "format" `Quick (test_format fs) ;
         ]
        );
        ("set",
         [ test_case "get/set roundtrip" `Quick (test_get_set fs);
           test_case "mkdir -p" `Quick (test_set_deep fs);
         ]

        );
        ("last modified",
         [ test_case "last modified increases on overwrite" `Quick (test_last_modified fs);
         ]
        );
        ("digest",
         [ test_case "digest of empty files w/different keys is identical" `Quick (test_digest_empty fs);
           test_case "slash digest" `Quick (test_digest_slash fs) ;
           test_case "deep key digest" `Quick (test_digest_deep_key fs) ;
           test_case "dict digest" `Quick (test_digest_deep_dictionary fs) ;
         ]
        )
      ]
  )
let () = test "emptyfile" 4096
