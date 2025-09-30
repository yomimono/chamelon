open Lwt.Infix
module Block = Mirage_block_combinators.Mem
module Chamelon = Kv.Make(Block)

(* we'll assume 512-sized blocks, since that's both
 * the maximum solo5-supported size currently and
 * the size used by the quick-and-dirty in-memory block device *)
let block_gen =
  let open Crowbar in
  (* Crowbar doesn't like to give us 512 bytes at a time, but it's fine with 256, so just ask for that twice *)
  map [bytes_fixed 256; bytes_fixed 256] (fun a b ->
      Cstruct.of_string @@ String.concat "" [a; b])

let write_fs fs blocks =
  let open Lwt.Infix in
  Lwt_list.iteri_p (fun block_number block_contents ->
      Block.write fs (Int64.of_int block_number) [block_contents] >>= function
      | Error e -> Crowbar.fail (Format.asprintf "error writing first block: %a" Block.pp_write_error e)
      | Ok () ->
        Lwt.return_unit
    ) blocks

let key = Crowbar.(map [bytes] Mirage_kv.Key.v)

let kv = Crowbar.(pair key bytes)

let start block blocks =
  write_fs block blocks >>= fun () ->
  Chamelon.connect block ~program_block_size:16 >>= function
  | Error _ ->
    (* "correctly" failing to connect is good *)
    Block.disconnect block >>= fun () ->
    Crowbar.bad_test ()
  | Ok fs ->
    Lwt.return fs

let size name blocks to_write =
  Lwt_main.run (
    Block.connect name >>= fun block ->
    start block blocks >>= fun fs ->
    (* our starting filesystem might have readable data; include that in the starting size *)
    Chamelon.size fs Mirage_kv.Key.empty >>= function
    | Error _ ->
      (* if we couldn't get a starting size, abort *)
      Block.disconnect block >>= fun () -> Crowbar.bad_test ()
    | Ok start_size ->
      Format.printf "fs mounted with size %a to start\n%!" Optint.Int63.pp start_size;
      Format.printf "%d ks and vs: %a\n%!" (List.length to_write) Fmt.(list ~sep:semi @@ pair ~sep:comma Mirage_kv.Key.pp Dump.string) to_write;
    (* write all the key/value pairs.
     * if they all succeeded,
     * the size reported for /
     * should be the sum of their values' sizes.
    *)
    Lwt_list.fold_left_s (
      fun sum (k, v) ->
        (* don't overwrite any previously set keys, and if we otherwise would,
         * don't include the value we would've set in the sum *)
        Chamelon.get fs k >>= function
        | Ok _ ->
          Format.printf "skipping %a as it's already set\n%!" Mirage_kv.Key.pp k;
          Lwt.return sum
        | Error _ ->
          Chamelon.set fs k v >>= function
          | Error _ ->
            Block.disconnect block >>= fun () ->
            Crowbar.bad_test ()
          | Ok () ->
            let v_size = String.length v in
            let sum = Optint.Int63.(add sum (of_int v_size)) in
            Format.printf "set key %a, value length %d, new sum %a\n%!" Mirage_kv.Key.pp k v_size Optint.Int63.pp sum;
            Lwt.return sum
    ) start_size to_write >>= fun sum ->
    Chamelon.size fs Mirage_kv.Key.empty >>= function
    | Error e -> Crowbar.failf "size failed on a filesystem where writes succeeded: %a" Chamelon.pp_error e
    | Ok size ->
      Crowbar.check_eq ~pp:Optint.Int63.pp size sum;
    Block.disconnect block
  )

let readback name blocks to_write =
  Lwt_main.run (
    Block.connect name >>= fun block ->
    start block blocks >>= fun fs ->
    Logs.debug (fun f -> f "%d ks and vs: %a\n%!" (List.length to_write) Fmt.(list ~sep:semi @@ pair ~sep:comma Mirage_kv.Key.pp Dump.string) to_write);
    (* for any key/value pair, if we can write it,
     * we should then be able to read it back *)
    Lwt_list.iter_p (
      fun (k, v) ->
        Logs.debug (fun f -> f "setting %S" @@ Mirage_kv.Key.to_string k);
        Chamelon.set fs k v >>= function
	| Error _ ->
	  Block.disconnect block >>= fun () ->
	  Crowbar.bad_test ()
	| Ok () ->
	  Chamelon.get fs k >>= function
	  | Error e ->
	    Block.disconnect block >>= fun () ->
	    Crowbar.failf "get of successfully set value: %a" Chamelon.pp_error e
	  | Ok readback ->
            Crowbar.check_eq ~pp:Fmt.Dump.string readback v;
            Chamelon.get_partial fs k ~offset:(Optint.Int63.of_int 0) ~length:(String.length v) >>= function
	    | Error e ->
	      Block.disconnect block >>= fun () ->
	      Crowbar.failf "get_partial of successfully set value: %a" Chamelon.pp_error e
            | Ok readback ->
              Crowbar.check_eq ~pp:Fmt.Dump.string readback v;
              Lwt.return_unit
    ) to_write
    >>= fun () ->
    Block.disconnect block
  )

let () =
  let open Crowbar in
  Logs.set_reporter @@ Logs_fmt.reporter ();
  Logs.set_level (Some Logs.Debug);
  add_test ~name:"read written info" [bytes; list block_gen; list kv] readback;
  add_test ~name:"size is sum of all written values" [bytes; list block_gen; list kv] size
