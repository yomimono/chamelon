module Block = Mirage_block_combinators.Mem

module Chamelon = Kv.Make(Block)(Pclock)

open Bechamel
open Toolkit
 
let lwt_test =
  Staged.stage @@ fun () ->
  Lwt_main.run @@ Lwt.return_unit

let block_connect_test =
  Staged.stage @@ fun () ->
  Lwt_main.run @@ (
    let open Lwt.Infix in
    Block.connect "bench chamelon" >>= fun _block ->
    Lwt.return_unit
  )

let format_test =
  Staged.stage @@ fun () ->
  Lwt_main.run @@ (
    let open Lwt.Infix in
    Block.connect "bench chamelon" >>= fun block ->
    Chamelon.format ~program_block_size:16 ~block_size:512 block >>= function
    | Error _ -> assert false
    | Ok _fs -> Lwt.return_unit
  )

let benchmark () =
  let ols =
    Analyze.ols ~bootstrap:0 ~r_square:true ~predictors:Measure.[| run |]
  in
  let cfg = Benchmark.cfg ~stabilize:true () in
  let instances = Instance.[monotonic_clock] in
  let test0 = Test.make ~name:"lwt" lwt_test in
  let test1 = Test.make ~name:"block connection" block_connect_test in 
  let test2 = Test.make ~name:"format blocks" format_test in 
  let test = Test.make_grouped ~name:"return unit" [test0; test1; test2] in
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
