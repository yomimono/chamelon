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

let test img block_size =
  let open Alcotest_lwt in
  let open Lwt.Infix in
  Lwt_main.run @@ (
    Block.connect img >>= fun block ->
    Littlefs.connect block ~program_block_size:16 ~block_size >>= function
    | Ok fs ->
      run "mirage-kv" [
        ("format",
         [ test_case "format" `Quick (test_format fs) ;
         ]
        );
        ("set",
         [ test_case "get/set roundtrip" `Quick (test_get_set fs);
         ]
        );
        ("last modified",
         [ test_case "last modified increases on overwrite" `Quick (test_last_modified fs);
         ]
        )
      ]
    | Error e -> Alcotest.fail (Format.asprintf "%a" Littlefs.pp_error e)
  )
let () = test "emptyfile" 4096
