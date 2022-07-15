open Lwt.Infix
module Block = Mirage_block_combinators.Mem
module Chamelon = Kv.Make(Block)(Pclock)

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

let readback name blocks to_write =
  Lwt_main.run (
    Block.connect name >>= fun block ->
    (* on start, fill the in-memory block device *)
    write_fs block blocks >>= fun () ->
    Chamelon.connect block ~program_block_size:16 >>= function
    | Error _ ->
      (* "correctly" failing to connect is good *)
      Block.disconnect block >>= fun () ->
      Crowbar.bad_test ()
    | Ok fs ->
      (* for any key/value pair, if we can write it,
       * we should then be able to read it back *)
      Lwt_list.iter_p (
        fun (k, v) -> Chamelon.set fs k v >>= function
          | Error _ ->
            Block.disconnect block >>= fun () ->
            Crowbar.bad_test ()
          | Ok () ->
            Chamelon.get fs k >>= function
            | Error e ->
              Block.disconnect block >>= fun () ->
              Crowbar.failf "%a" Chamelon.pp_error e
            | Ok readback ->
              Lwt.return @@ Crowbar.check_eq readback v
      ) to_write
      >>= fun () ->
      Block.disconnect block
  )

let () =
  let open Crowbar in
  Logs.set_level (Some Logs.Debug);
  add_test ~name:"read written info" [bytes; list block_gen; list kv] readback
