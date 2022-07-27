module Block = Mirage_block_combinators.Mem

module Chamelon = Kv.Make(Block)(Pclock)
module Log = (val Logs.src_log Logs.default : Logs.LOG)

let key = Mirage_kv.Key.v "/file"
let ascii_denom = 128
let comparator = String.init ascii_denom (fun f -> Char.chr f)
 
let mount_and_write =
  let open Lwt.Infix in
  Block.connect "mount chamelon" >>= fun block ->
  Block.get_info block >>= fun info ->
  Chamelon.format ~program_block_size:16 block >>= function
  | Error e -> Alcotest.failf "failed to format block device: %a" Chamelon.pp_write_error e
  | Ok () ->
    Chamelon.connect ~program_block_size:16 block >>= function
    | Error e -> Alcotest.failf "failed to mount fs: %a" Chamelon.pp_error e
    | Ok fs ->
      (* we want a real big buncha stuff *)
      Format.eprintf "block device has %Ld sectors of size %d\n%!" info.size_sectors info.sector_size;
      let space = (8192 - 64) * info.sector_size in
      (* get an even # of iterations of our chargen loop, for easier `tail` testing *)
      let content_size = (space / ascii_denom) * ascii_denom in
      let content = String.init content_size (fun f -> Char.chr (f mod ascii_denom)) in
      Format.eprintf "setting a key of size %d\n%!" content_size;
      Format.eprintf "starting key with %S\n%!" @@ String.sub content 0 128;
      Chamelon.set fs key content >>= function
      | Error e -> Alcotest.failf "error setting test key: %a" Chamelon.pp_write_error e
      | Ok () -> Lwt.return fs

(* worst-case for an inefficient partial read, and a common real-world case:
 * we want the *first* small number of bytes (e.g. `head`) *)
let head ~readfn n =
  Lwt_main.run @@ (
    let open Lwt.Infix in
    mount_and_write >>= fun fs ->
    let rec aux = function
      | n when n <= 0 -> Lwt.return_unit
      | n ->
        readfn fs key ~offset:0 ~length:128 >>= function
        | Error e ->
          Alcotest.failf "error reading test key: %a" Chamelon.pp_error e
        | Ok subset ->
          Alcotest.(check string) "head subset" comparator subset;
          aux (n-1)
    in
    aux n
  )

(* a partial read that doesn't stop after getting the data it needs
 * will perform very poorly in this test *)
let tail ~readfn n =
  Lwt_main.run @@ (
    let open Lwt.Infix in
    mount_and_write >>= fun fs ->
    Chamelon.size fs key >>= function
    | Error e -> Alcotest.failf "error getting size of test key: %a" Chamelon.pp_error e
    | Ok size ->
    let rec aux = function
      | n when n <= 0 -> Lwt.return_unit
      | n ->
        readfn fs key ~offset:(size - ascii_denom) ~length:ascii_denom >>= function
        | Error e -> Alcotest.failf "error reading test key: %a" Chamelon.pp_error e
        | Ok subset ->
          Alcotest.(check string) "tail subset" comparator subset;
          aux (n-1)
    in
    aux n
  )

let () =
  Logs.set_reporter @@ Logs_fmt.reporter ();
  Logs.set_level (Some Logs.Debug);
  head ~readfn:Chamelon.get_partial 1;
  tail ~readfn:Chamelon.get_partial 1;
  ()
