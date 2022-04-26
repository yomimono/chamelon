module Block = Mirage_block_combinators.Mem

module Chamelon = Kv.Make(Block)(Pclock)

open Bechamel
open Toolkit
 
let mount =
  let open Lwt.Infix in
  Block.connect "mount chamelon" >>= fun block ->
  Chamelon.format ~program_block_size:16 block >>= function
  | Error _ -> assert false
  | Ok () ->
    Chamelon.connect ~program_block_size:16 block >>= function
    | Error _ -> assert false
    | Ok fs -> Lwt.return fs

let write n =
  Staged.stage @@ fun () ->
  Lwt_main.run @@ (
    let open Lwt.Infix in
    mount >>= fun kv ->
    let rec write_one = function
      | n when n < 0 -> Lwt.return_unit
      | n -> Chamelon.set kv (Mirage_kv.Key.v (string_of_int n)) (string_of_int n) >>= function
        | Error _ -> assert false
        | Ok () -> write_one (n - 1)
    in
    write_one n
  )

let benchmark () =
  let ols =
    Analyze.ols ~bootstrap:0 ~r_square:true ~predictors:Measure.[| run |]
  in
  let cfg = Benchmark.cfg ~stabilize:true () in
  let instances = Instance.[monotonic_clock] in
  let args = List.init 5 (fun n -> n * 10) in
  let test_writes = Test.make_indexed ~name:"root directory writes" ~args write in
  let test = Test.make_grouped ~name:"baseline" [test_writes] in
  let raw_results = Benchmark.all cfg instances test in
  let results =
    List.map (fun instance -> Analyze.all ols instance raw_results) instances
  in
  let results = Analyze.merge ols instances results in
  (results, raw_results)

let () =
  let results = benchmark () in
  let results_results =
    let nothing _ = Ok () in
    let open Bechamel_js in
    emit ~dst:(Channel stdout) nothing ~compare ~x_label:Measure.run
      ~y_label:(Measure.label Instance.monotonic_clock)
      results
  in
  match results_results with Ok () -> () | Error (`Msg err) -> invalid_arg err
