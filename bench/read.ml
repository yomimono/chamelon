module Block = Mirage_block_combinators.Mem

module Chamelon = Kv.Make(Block)(Pclock)

open Bechamel
open Toolkit

let key = Mirage_kv.Key.v "/file"
let subset_length = 128
let comparator = String.init subset_length (fun f -> Char.chr (f / 128))
 
let mount_and_write =
  let open Lwt.Infix in
  Block.connect "mount chamelon" >>= fun block ->
  Block.get_info block >>= fun info ->
  Chamelon.format ~program_block_size:16 block >>= function
  | Error _ -> assert false
  | Ok () ->
    Chamelon.connect ~program_block_size:16 block >>= function
    | Error _ -> assert false
    | Ok fs ->
      (* we want a real big buncha stuff *)
      let content_size = (Int64.to_int info.size_sectors) - 2 in
      let content = String.init content_size (fun f -> Char.chr (f / 128)) in
      Chamelon.set fs key content >>= function
      | Error e -> Format.eprintf "error setting test key: %a" Chamelon.pp_write_error e; assert false
      | Ok () -> Lwt.return fs

let read_real ~readfn n =
  Lwt_main.run @@ (
    let open Lwt.Infix in
    mount_and_write >>= fun fs ->
    (* worst-case for an inefficient partial read, and a common real-world case:
     * we want the *first* small number of bytes (e.g. `head`) *)
    let rec aux = function
      | n when n <= 0 -> Lwt.return_unit
      | n ->
        readfn fs key ~offset:0 ~length:128 >>= function
        | Error e -> Format.eprintf "error reading test key: %a" Chamelon.pp_error e;
          assert false
        | Ok subset ->
          (* not interested in benchmarking incorrect implementations *)
          assert (String.equal subset comparator);
          aux (n-1)
    in
    aux n
  )

let benchmark instance =
  let n = 100 in
  let ols =
    Analyze.ols ~bootstrap:0 ~r_square:true ~predictors:Measure.[| run |]
  in
  let cfg = Benchmark.cfg ~stabilize:true () in
  let instances = instance::[] in
  let test_naive = Test.make ~name:"naive reads" (Staged.stage @@ fun () -> read_real ~readfn:Chamelon.simple_get_partial n) in
  let test_nonnaive = Test.make ~name:"less naive reads" (Staged.stage @@ fun () -> read_real ~readfn:Chamelon.get_partial n) in
  let test = Test.make_grouped ~name:"baseline" [test_naive; test_nonnaive] in
  let raw_results = Benchmark.all cfg instances test in
  let results =
    List.map (fun instance -> Analyze.all ols instance raw_results) instances
  in
  let results = Analyze.merge ols instances results in
  (results, raw_results)

let evaluate instance =
  let nothing _ = Ok () in
  let open Bechamel_js in
  emit ~dst:(Channel stdout) nothing ~compare ~x_label:Measure.run ~y_label:(Measure.label instance) @@ benchmark instance |> function
  | Error (`Msg s) -> Format.eprintf "%s\n%!" s; exit 1
  | Ok _ -> ()

let () =
  evaluate Instance.major_allocated
