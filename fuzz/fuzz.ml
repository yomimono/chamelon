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

let init blocks to_write =
  let open Lwt.Infix in
  Lwt_main.run (
    Block.connect "fuzz chamelon" >>= fun block ->
    (* on start, fill the in-memory block device *)
    write_fs block blocks >>= fun () ->
    (* for now, our assertion is just that we'll either connect or return an error *)
    Chamelon.connect block ~program_block_size:16 ~block_size:512  >>= function
    | Error _ -> Crowbar.bad_test ()
    | Ok fs ->
      (* for any key/value pair we've been given, we should be able to
       * write it and then get it back *)
      Lwt_list.iter_p (
        fun (k, v) -> Chamelon.set fs k v >>= function
          | Error _ -> Crowbar.bad_test ()
          | Ok () ->
            Chamelon.get fs k >>= function
            | Error e -> Crowbar.failf "%a" Chamelon.pp_error e
            | Ok readback ->
              Lwt.return @@ Crowbar.check_eq readback v
      ) to_write
  )

let () =
  let open Crowbar in
  Logs.set_level (Some Logs.Debug);
  add_test ~name:"initialize" [list block_gen; list kv] init
