module Block = Mirage_block_combinators.Mem

module Chamelon = Kv.Make(Block)

open Bechamel
open Toolkit

let key = Mirage_kv.Key.v "/file"
let ascii_denom = 128
let comparator = String.init ascii_denom (fun f -> Char.chr f)
 
let simple_get_partial t key ~offset ~length =
  if Optint.Int63.(offset < zero) then begin
    Logs.err (fun f -> f "read requested with negative offset");
    Lwt.return @@ Error (`Not_found key)
  end else if length <= 0 then begin
    Logs.err (fun f -> f "read requested with length <= 0");
    Lwt.return @@ Error (`Not_found key)
  end else begin
    let open Lwt.Infix in
    Chamelon.get t key >|= function
    | Error _ as e -> e
    | Ok v ->
      let offset = Optint.Int63.to_int offset in
      try Ok (String.sub v offset length)
      with Invalid_argument _ ->
        Logs.err (fun f -> f "partial read request cannot be fulfilled: %d < %d" (String.length v) (offset + length));
        Error (`Not_found key)
  end

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
      let space = ((Int64.to_int info.size_sectors) / 2) * info.sector_size in
      (* get an even # of iterations of our chargen loop, for easier `tail` testing *)
      let content_size = (space / ascii_denom) * ascii_denom in
      let content = String.init content_size (fun f -> Char.chr (f mod ascii_denom)) in
      Chamelon.set fs key content >>= function
      | Error e -> Format.eprintf "error setting test key: %a" Chamelon.pp_write_error e; assert false
      | Ok () -> Lwt.return fs

let head ~readfn n =
  Lwt_main.run @@ (
    let open Lwt.Infix in
    mount_and_write >>= fun fs ->
    (* worst-case for an inefficient partial read, and a common real-world case:
     * we want the *first* small number of bytes (e.g. `head`) *)
    let rec aux = function
      | n when n <= 0 -> Lwt.return_unit
      | n ->
        readfn fs key ~offset:Optint.Int63.zero ~length:ascii_denom >>= function
        | Error e -> Format.eprintf "error reading test key: %a" Chamelon.pp_error e;
          assert false
        | Ok subset ->
          assert (String.equal subset comparator);
          aux (n-1)
    in
    aux n
  )

let tail ~readfn n =
  Lwt_main.run @@ (
    let open Lwt.Infix in
    mount_and_write >>= fun fs ->
    Chamelon.size fs key >>= function
    | Error _ -> assert false
    | Ok size ->
      (* worst-case for an inefficient partial read, and a common real-world case:
       * we want the *last* small number of bytes (e.g. `tail`) *)
      let rec aux = function
        | n when n <= 0 -> Lwt.return_unit
        | n ->
          let offset = Optint.Int63.(sub size (of_int ascii_denom)) in
          readfn fs key ~offset ~length:ascii_denom >>= function
          | Error e -> Format.eprintf "error reading test key: %a" Chamelon.pp_error e;
            assert false
          | Ok subset ->
            assert (String.equal subset comparator);
            aux (n-1)
      in
      aux n
  )

let benchmark instance =
  let n = 10 in
  let ols =
    Analyze.ols ~bootstrap:0 ~r_square:false ~predictors:Measure.[| run |]
  in
  let cfg = Benchmark.cfg ~stabilize:true () in
  let instances = instance::[] in
  let _test_naive_head = Test.make ~name:"naive head reads" (Staged.stage @@ fun () -> head ~readfn:simple_get_partial n) in
  let test_nonnaive_head = Test.make ~name:"less naive head reads" (Staged.stage @@ fun () -> head ~readfn:Chamelon.get_partial n) in
  let _test_naive_tail = Test.make ~name:"naive tail reads" (Staged.stage @@ fun () -> tail ~readfn:simple_get_partial n) in
  let test_nonnaive_tail = Test.make ~name:"less naive tail reads" (Staged.stage @@ fun () -> tail ~readfn:Chamelon.get_partial n) in
  let heads = Test.make_grouped ~name:"head" [(* test_naive_head ; *) test_nonnaive_head] in
  let tails = Test.make_grouped ~name:"tail" [(* test_naive_tail ; *) test_nonnaive_tail] in
  let test = Test.make_grouped ~name:"all" [heads; tails] in
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
  evaluate Instance.monotonic_clock
