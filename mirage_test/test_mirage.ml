module Chamelon = Kv.Make(Block)(Pclock)
open Lwt.Infix

let fail pp e = Lwt.fail_with (Format.asprintf "%a" pp e)

let fail_read = fail Chamelon.pp_error
let fail_write = fail Chamelon.pp_write_error

let testable_key = Alcotest.testable Mirage_kv.Key.pp Mirage_kv.Key.equal

let program_block_size = 16
let block_size = 512

let rec write_until_full ~write fs n =
  write n >>= function
  | false -> Lwt.return (n - 1)
  | true -> write_until_full ~write fs (n+1)

let format_and_mount block =
  Chamelon.format ~program_block_size block >>= function
  | Error e -> fail_write e
  | Ok () ->
    Chamelon.connect ~program_block_size block >>= function
    | Error e -> fail_read e
    | Ok fs -> Lwt.return fs

let test_format block _ () =
  Chamelon.format ~program_block_size block >>= function
  | Error e -> fail_write e
  | Ok () ->
    Chamelon.connect ~program_block_size block >>= function
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

let test_set_too_big_key block _ () =
  (* 500 x 'A' shouldn't be writable as a path on our 512-block fs *)
  let path = Mirage_kv.Key.v "/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" in
  let contents = ":D" in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs path contents >>= function
  | Ok () -> Alcotest.fail "allowed a write to way too big a key"
  | Error e -> Format.printf "%a" Chamelon.pp_write_error e; Lwt.return_unit

let test_set_partial block _ () =
  let key = Mirage_kv.Key.v "/original" in
  let contents = "I need less coffee" in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs key contents >>= function | Error e -> fail_write e | Ok () ->
  let offset = Optint.Int63.of_int 7 in
  Chamelon.set_partial fs key ~offset "more" >>= function | Error e -> fail_write e | Ok () ->
  Chamelon.get fs key >>= function | Error e -> fail_read e
  | Ok actual ->
    Alcotest.(check string) "set_partial" "I need more coffee" actual;
    Lwt.return_unit

let test_set_empty_key block _ () =
  let path = Mirage_kv.Key.v "" in
  let contents = "camel" in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs path contents >>= function
  | Ok () -> Alcotest.fail "allowed a write to empty path"
  | Error _ -> Lwt.return_unit

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
    Alcotest.(check string) "list entry name" "/set" (Mirage_kv.Key.to_string (fst e));
    match (snd e) with 
    | `Value -> Alcotest.fail "value where dictionary was expected"
    | `Dictionary ->
      Chamelon.list fs (Mirage_kv.Key.parent key) >>= function
      | Error e -> Lwt.fail_with @@ Format.asprintf "parent directory of %a listing failed: %a" Mirage_kv.Key.pp key Chamelon.pp_error e
      | Ok l ->
        let pp_key = Mirage_kv.Key.pp in
        Alcotest.(check int) (Format.asprintf "size of ls %a after setting %a" pp_key (Mirage_kv.Key.parent key) pp_key key) 1 @@ List.length l;
        let e = List.hd l in
        Alcotest.(check string) "list entry name" "/set/deep/fs/filesystem" (Mirage_kv.Key.to_string (fst e));
        Lwt.return_unit

let test_last_modified block _ () =
  let path = Mirage_kv.Key.v "get set to get wet" in
  let contents = "hell yeah let's do this!!!" in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs path contents >>= function | Error e -> fail_write e | Ok () ->
  Chamelon.get fs path >>= function | Error e -> fail_read e
  | Ok _contents ->
    Chamelon.last_modified fs path >>= function | Error e -> fail_read e | Ok first_timestamp ->
      let now_timestamp = Pclock.(now_d_ps ()) |> Ptime.unsafe_of_d_ps in
      Alcotest.(check bool) "last modified time is before now" true (Ptime.is_later now_timestamp ~than:first_timestamp);
      Chamelon.set fs path "do it again!!" >>= function | Error e -> fail_write e
      | Ok () ->
        Chamelon.last_modified fs path >>= function | Error e -> fail_read e | Ok second_timestamp ->
          Alcotest.(check bool) "after modifying, last modified time is later" true
            (Ptime.is_later second_timestamp ~than:now_timestamp);
          Lwt.return_unit

let test_last_modified_dir block _ () =
  let cheese1 = Mirage_kv.Key.v "/cheeses/edam" in
  let cheese2 = Mirage_kv.Key.v "/cheeses/cheddar" in
  let contents = "delicious" in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs cheese1 contents >>= function | Error e -> fail_write e | Ok () ->
  Chamelon.set fs cheese2 contents >>= function | Error e -> fail_write e | Ok () ->
  Chamelon.last_modified fs cheese2 >>= function | Error e -> fail_read e | Ok cheese2_modified ->
  Chamelon.last_modified fs @@ Mirage_kv.Key.parent cheese2 >>= function
  | Error e -> fail_read e
  | Ok cheeses_modified ->
    let cheese2_modified_d_ps = Ptime.Span.to_d_ps (Ptime.to_span cheese2_modified) in
    let cheeses_modified_d_ps = Ptime.Span.to_d_ps (Ptime.to_span cheeses_modified) in
    Alcotest.(check (pair int int64) "dir has same last modified time as the last thing modified" cheese2_modified_d_ps cheeses_modified_d_ps);
    Lwt.return_unit

let test_last_modified_depth block _ () =
  let key1 = Mirage_kv.Key.v "/calico/explanation" in
  let key2 = Mirage_kv.Key.v "/calico/shorthaired/sweetheart" in
  let contents1 = "cats with 3 colors, usually black and orange on white" in
  let contents2 = "curious georgia porgia, perfect princess housecat, first of her name, smallest and best" in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs key1 contents1 >>= function | Error e -> fail_write e | Ok () ->
  Chamelon.set fs key2 contents2 >>= function | Error e -> fail_write e | Ok () ->
  Chamelon.last_modified fs (Mirage_kv.Key.parent key1) >>= function | Error e -> fail_read e | Ok calico_modified ->
  Chamelon.last_modified fs key1 >>= function | Error e -> fail_read e | Ok key1_modified ->
  let key1_modified_d_ps = Ptime.Span.to_d_ps (Ptime.to_span key1_modified) in
  let calico_modified_d_ps = Ptime.Span.to_d_ps (Ptime.to_span calico_modified) in
  Alcotest.(check (pair int int64) "last modified for parent only goes down 1 level" key1_modified_d_ps calico_modified_d_ps);
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
  let blorp = String.init block_size (fun _ -> 'a') in
  let k n = Mirage_kv.Key.v @@ string_of_int n in
  let blocks = 4096 * 10 / block_size in
  let blocks_for_metadata = 18 in
  format_and_mount block >>= fun fs ->
  let l = List.init (blocks - blocks_for_metadata) (fun n -> n) in
  Logs.debug (fun f -> f "writing %d blocks of nonsense..." (blocks - blocks_for_metadata));
  Lwt_list.iter_s (fun i ->
    Chamelon.set fs (k i) blorp >>= function | Error e -> fail_write e | Ok () ->
      Logs.debug (fun f -> f "wrote a block's worth of 'a' to the key /%d" i);
      Lwt.return_unit
  ) l >>= fun () ->
  Chamelon.list fs (Mirage_kv.Key.empty) >>= function | Error e -> fail_read e | Ok l ->
  Logs.debug (fun f -> f "%d items in the list for /" (List.length l));
  Alcotest.(check int) "all set items are present" (blocks - blocks_for_metadata) @@ List.length l;
  Chamelon.set fs (k (blocks - blocks_for_metadata + 1)) blorp >>= function
  | Error `No_space -> Lwt.return_unit
  | Ok _ -> Alcotest.fail "setting last key succeeded when we expected No_space"
  | Error e -> Alcotest.failf "setting last key failed with %a instead of No_space" Chamelon.pp_write_error e

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

let test_get_big_value block _ () =
  let key = Mirage_kv.Key.v "/chonkerson" in
  format_and_mount block >>= fun fs ->
  let g = Mirage_crypto_rng.default_generator () in
  let value = Mirage_crypto_rng.generate ~g (block_size * 4) in
  Chamelon.set fs key (Cstruct.to_string value) >>= function
  | Error e -> Alcotest.fail (Format.asprintf "setting a large key failed: %a" Chamelon.pp_write_error e)
  | Ok () ->
    Chamelon.get fs key >>= function
    | Error e -> Alcotest.fail (Format.asprintf "getting a large key failed: %a" Chamelon.pp_error e)
    | Ok retrieved_value ->
      Alcotest.(check string "retrieved big file is the same as what we set" (Cstruct.to_string value) retrieved_value);
      Lwt.return_unit

let test_get_valid_partial block _ () =
  let negation = "on opposite day, "
  and content = "the sky is red"
  in
  let key = Mirage_kv.Key.v "/wisdom"
  and contents = negation ^ content
  in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs key contents >>= function
  | Error e -> fail_write e
  | Ok () ->
    Chamelon.get_partial fs key
      ~offset:(Optint.Int63.of_int (String.length negation))
      ~length:(String.length content) >>= function
    | Error e -> fail_read e
    | Ok partial -> Alcotest.(check string) "partial read" content partial;
      Lwt.return_unit

let test_get_partial_bad_offsets block _ () =
  let key = Mirage_kv.Key.v "/file" and content = "important stuff" in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs key content >>= function
  | Error e -> fail_write e
  | Ok () ->
    (* offset too big *)
    let offset = Optint.Int63.of_int (String.length content + 10) in
    Chamelon.get_partial fs key ~offset ~length:1 >>= function
    | Ok v -> Alcotest.failf "partial read off end of file succeeded, returning %S" v
    | Error _ ->
      (* offset too *small* *)
      let offset = Optint.Int63.of_int (-10) in
      Chamelon.get_partial fs key ~offset ~length:1 >>= function
      | Ok v -> Alcotest.failf "negative offset succeeded, returning %S" v
      | Error _ -> Lwt.return_unit

let test_get_partial_bad_length block _ () =
  let key = Mirage_kv.Key.v "/file" and content = "important stuff" in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs key content >>= function
  | Error e -> fail_write e
  | Ok () ->
    (* negative length *)
    let offset = Optint.Int63.of_int 0 in
    Chamelon.get_partial fs key ~offset ~length:(-10) >>= function
    | Ok v -> Alcotest.failf "partial read with negative length succeeded, returning %S" v
    | Error _ -> 
      Chamelon.get_partial fs key ~offset ~length:0 >>= function
      | Ok v -> Alcotest.failf "partial read with zero length succeeded, returning %S" v
      | Error _ ->
        Chamelon.get_partial fs key ~offset ~length:(2 * (String.length content)) >>= function
        | Error _ -> Alcotest.failf "should've gotten a short read, but got an outright failure"
        | Ok v -> Alcotest.(check string) "short reads" content v;
          Lwt.return_unit

let test_get_partial_bad_combos block _ () =
  let key = Mirage_kv.Key.v "/file" and content = "important stuff" in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs key content >>= function | Error e -> fail_write e | Ok () ->
  let offset = Optint.Int63.of_int 5 in
  Chamelon.get_partial fs key ~offset ~length:(String.length content) >>= function
  | Error e -> Alcotest.failf "should've gotten a short read, but got an outright failure: %a" Chamelon.pp_error e
  | Ok v -> Alcotest.(check string) "short reads" (String.sub content 5 ((String.length content) - 5)) v;
    Lwt.return_unit

let test_get_partial_in_dir block _ () =
  let key = Mirage_kv.Key.v "/files/more files/some more files/file"
  and content = "important stuff"
  in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs key content >>= function | Error e -> fail_write e | Ok () ->
  let offset = Optint.Int63.of_int 1 in
  Chamelon.get_partial fs key ~offset ~length:((String.length content) - 1) >>= function
  | Error e -> Alcotest.failf "error reading successfully set key: %a" Chamelon.pp_error e
  | Ok v -> Alcotest.(check string) "offset read of a file in a directory" (String.sub content 1 @@ (String.length content) - 1) v;
    Lwt.return_unit

let test_size_nonexistent block _ () =
  let key = Mirage_kv.Key.v "/thedeep/filenotfound" in
  format_and_mount block >>= fun fs ->
  Chamelon.size fs key >>= function
  | Error (`Not_found _) -> Lwt.return_unit
  | Error e -> Alcotest.fail (Format.asprintf "size on a nonexistent key failed with misleading error: %a" Chamelon.pp_error e)
  | Ok s ->
    let s = Optint.Int63.to_int s in
    Alcotest.fail (Format.asprintf "size on a nonexistent key succeeded, and gave us %d" s)

let test_size_small_file block _ () =
  let key = Mirage_kv.Key.v "/smallfile"
  and contents = "a string of known size"
  in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs key contents >>= function
  | Error e -> fail_write e
  | Ok () ->
    Chamelon.size fs key >>= function
    | Error e -> fail_read e
    | Ok s ->
      let s = Optint.Int63.to_int s in
      Alcotest.(check int) "size of small file is correct" (String.length contents) s;
      Lwt.return_unit

let test_size_empty block _ () =
  format_and_mount block >>= fun fs ->
  Chamelon.size fs Mirage_kv.Key.empty >>= function
  | Error e -> fail_read e
  | Ok n -> 
    let n = Optint.Int63.to_int n in
    Alcotest.(check int) "fresh filesystem size /" 0 n;
    Lwt.return_unit
  
let test_size_dir block _ () =
  let key1 = Mirage_kv.Key.v "/important do not lose/1"
  and key2 = Mirage_kv.Key.v "/important do not lose/2"
  and contents = "a string of known size"
  in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs key1 contents >>= function | Error e -> fail_write e | Ok () ->
  Chamelon.set fs key2 contents >>= function | Error e -> fail_write e | Ok () ->
  Chamelon.size fs Mirage_kv.Key.empty >>= function
  | Error e -> fail_read e
  | Ok n ->
    let n = Optint.Int63.to_int n in
    Alcotest.(check int) "directory file size" ((String.length contents) * 2) n;
    Lwt.return_unit

let test_nested_dir block _ () =
  let key1 = Mirage_kv.Key.v "/files/boring/taxes/2013/receipts/the good stuff"
  and key2 = Mirage_kv.Key.v "/files/boring/taxes/2014/receipts/the good stuff"
  and contents = "secrets"
  in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs key1 contents >>= function | Error e -> fail_write e | Ok () ->
  Chamelon.set fs key2 contents >>= function | Error e -> fail_write e | Ok () ->
  Chamelon.size fs @@ Mirage_kv.Key.v "/files/boring" >>= function
  | Error e -> fail_read e
  | Ok n ->
    let n = Optint.Int63.to_int n in
    Alcotest.(check int) "deeply nested directory file size" (2 * (String.length contents)) n;
    Lwt.return_unit

let test_many_files block _ () =
  format_and_mount block >>= fun fs ->
  let contents i = "number " ^ string_of_int i in
  let write i =
    Logs.debug (fun f -> f "setting key %d" i);
    Chamelon.set fs (Mirage_kv.Key.v @@ string_of_int i) (contents i) >>= function
    | Error `No_space -> Lwt.return false
    | Ok () -> Lwt.return true
    | Error e -> Alcotest.failf "unexpected error: %a" Chamelon.pp_write_error e
  in
  let readback i =
    Chamelon.get fs (Mirage_kv.Key.v @@ string_of_int i) >>= function
    | Error e -> Alcotest.failf "unexpected error fetching %d: %a" i Chamelon.pp_error e
    | Ok v ->
      Alcotest.(check string) "file contents match what's expected" (contents i) v;
      Lwt.return_unit
  in
  let rec read_all fs max n =
    readback n >>= fun () ->
    if (n + 1) > max then Lwt.return_unit else read_all fs max (n+1)
  in
  write_until_full ~write fs 0 >>= fun last_written ->
  Logs.debug (fun f -> f "last written file was %d" last_written);
  Alcotest.(check bool) "we managed to write at least one file" true (last_written > 0);
  Chamelon.list fs Mirage_kv.Key.empty >>= function | Error e -> fail_read e | Ok l ->
  let names = List.map (fun (n, _) -> Mirage_kv.Key.basename n) l |> List.fast_sort (fun a b -> Int.compare (int_of_string a) (int_of_string b)) in
  Logs.debug (fun f -> f "%a" Fmt.(list ~sep:sp string) names);
  Alcotest.(check int) "ls contains all written files" (last_written + 1) (List.length l);
  (* make sure each key has the correct corresponding value *)
  read_all fs last_written 0 >>= fun () ->
  Lwt.return_unit

let test_recursive_rm block _ () =
  let common_key = Mirage_kv.Key.v "state" in
  format_and_mount block >>= fun fs ->
  let write i =
    Chamelon.set fs (Mirage_kv.Key.((v @@ string_of_int i) // common_key)) "i'm a key :D" >>= function
    | Error `No_space -> Lwt.return false
    | Ok () -> Lwt.return true
    | Error e -> Alcotest.failf "unexpected error: %a" Chamelon.pp_write_error e
  in
  let isnt_there key =
    Chamelon.exists fs key >>= function
    | Ok (Some _) -> Alcotest.failf "exists said %a existed after it was deleted" Mirage_kv.Key.pp key
    | Ok None -> Lwt.return_unit
    | Error (`Not_found k) when Mirage_kv.Key.(equal k @@ parent key) -> Lwt.return_unit
    | Error e -> Alcotest.failf "unexpected error: %a" Chamelon.pp_error e
  in
  write_until_full ~write fs 0 >>= fun last_written ->
  Alcotest.(check bool) "wrote more than one entry" true (last_written > 0);
  Logs.debug (fun f -> f "wrote %d entries" last_written);
  let dir_key = Mirage_kv.Key.(v @@ string_of_int last_written) in
  Chamelon.list fs Mirage_kv.Key.empty >>= function | Error e -> fail_read e | Ok l ->
  let items = List.length l in
  Chamelon.remove fs dir_key >>= function
  | Error e -> fail_write e
  | Ok () ->
    Logs.debug (fun f -> f "deleting %a reported success" Mirage_kv.Key.pp dir_key);
    isnt_there dir_key >>= fun () ->
    isnt_there Mirage_kv.Key.(dir_key // common_key) >>= fun () ->
    (* we should have one fewer item in the list of / *)
    Chamelon.list fs Mirage_kv.Key.empty >>= function | Error e -> fail_read e | Ok l ->
    Alcotest.(check int) "removing an item means it doesn't show up in list" items ((+) 1 @@ List.length l);
    Lwt.return_unit

let test_rm_slash block _ () =
  format_and_mount block >>= fun fs ->
  Chamelon.remove fs Mirage_kv.Key.empty >>= function
  | Ok () -> Alcotest.fail "succeeded in removing the empty key (in other words, /)"
  | Error _ -> Lwt.return_unit

let test_rename block _ () =
  let key1 = Mirage_kv.Key.v "/original1" in
  let key2 = Mirage_kv.Key.v "/original2" in
  let contents = "A dummy file" in
  format_and_mount block >>= fun fs ->
  Chamelon.set fs key1 contents >>= function | Error e -> fail_write e | Ok () ->
  Chamelon.rename fs ~source:key1 ~dest:key2 >>= function
  | Error e -> Alcotest.failf "rename a key failed: %a" Chamelon.pp_write_error e
  | Ok () ->
    Chamelon.exists fs key2 >>= function
    | Error e -> Alcotest.failf "rename failed: %a" Chamelon.pp_error e
    | Ok None -> Alcotest.failf "the new name doesn't exists"
    | Ok (Some _) ->
      Chamelon.exists fs key1 >>= function
      | Error e -> Alcotest.failf "rename failed: %a" Chamelon.pp_error e
      | Ok (Some _) -> Alcotest.failf "the old name still exists"
      | Ok None -> Lwt.return_unit

let test img =
  Logs.set_level (Some Logs.Debug);
  Logs.set_reporter @@ Logs_fmt.reporter ();
  let open Alcotest_lwt in
  let open Lwt.Infix in
  Lwt_main.run @@ (
    Mirage_crypto_rng_lwt.initialize (module Mirage_crypto_rng.Fortuna);
    Block.connect ~prefered_sector_size:(Some 512) img >>= fun block ->
    run "mirage-kv" [
      ("format",
       [ test_case "format" `Quick (test_format block) ;
       ]
      );
      ("set",
       [ test_case "get/set roundtrip" `Quick (test_get_set block);
         test_case "get/set roundtrip w/non-ascii data" `Quick (test_set_nonascii_data block);
         test_case "get/set roundtrip w/non-ascii key" `Quick (test_set_nonascii_key block);
         test_case "get/set roundtrip w/empty key" `Quick (test_set_empty_key block);
         test_case "try to set too big a key" `Quick (test_set_too_big_key block);
         test_case "mkdir -p" `Quick (test_set_deep block);
         test_case "disk full" `Quick (test_no_space block);
         test_case "set_partial" `Quick (test_set_partial block);
       ]
      );
      ("last modified",
       [ test_case "last modified increases on overwrite" `Quick (test_last_modified block);
         test_case "last modified of a directory reflects a write in it" `Quick (test_last_modified_dir block);
         test_case "last modified only goes 1 level deep" `Quick (test_last_modified_depth block);
       ]
      );
      ("get",
       [ test_case "get nonexistent value" `Quick (test_nonexistent_value block);
         test_case "get a dictionary" `Quick (test_get_dictionary block);
         test_case "get a big value" `Quick (test_get_big_value block);
         test_case "get valid partial data" `Quick (test_get_valid_partial block);
         test_case "get partial data w/bad offset" `Quick (test_get_partial_bad_offsets block);
         test_case "get partial data w/bad length" `Quick (test_get_partial_bad_length block);
         test_case "get partial data w/bad offset+length" `Quick (test_get_partial_bad_combos block);
         test_case "get partial data in a file within a dir" `Quick (test_get_partial_in_dir block);
       ]
      );
      ("size",
      [ test_case "size of something missing" `Quick (test_size_nonexistent block);
        test_case "size of a small file" `Quick (test_size_small_file block);
        test_case "size of an empty fs" `Quick (test_size_empty block);
        test_case "size of a directory" `Quick (test_size_dir block);
        test_case "size of some nested stuff" `Quick (test_nested_dir block);
       ]
      );
      ("digest",
       [ test_case "digest of empty files w/different keys is identical" `Quick (test_digest_empty block);
         test_case "slash digest" `Quick (test_digest_slash block) ;
         test_case "file overwrite digest" `Quick (test_digest_overwrite block) ;
         test_case "dict digest" `Quick (test_digest_deep_dictionary block) ;
       ]
      );
      ("split",
       [
         test_case "we can write files until we run out of space" `Quick (test_many_files block);
       ]);
      ("rm",
       [
         test_case "removals are recursive" `Quick (test_recursive_rm block);
         test_case "can't remove /" `Quick (test_rm_slash block);
       ]);
      ("rename",
       [
         test_case "rename" `Quick (test_rename block);
       ]);
    ]
  )
let () = test "emptyfile"
