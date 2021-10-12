module Tag = struct
  let test_zero () =
    let n = 0 in
    let t = Littlefs.Tag.parse n |> Result.get_ok in
    Alcotest.(check bool) "valid bit" false t.valid
  
  let test_ones () =
    (* each field is 1, but abstract_type 1 is invalid *)
    let repr = 0x8000_0000 +
               0x1000_0000 +
               0x0010_0000 +
               0x0000_0400 +
               0x0000_0001
    in
    match Littlefs.Tag.parse repr with
    | Ok _ -> Alcotest.fail "abstract type 1 was accepted"
    | Error _ -> ()
  
  let test_maxint () =
    let valid = true
    and abstract_type = Littlefs.Tag.LFS_TYPE_GSTATE
    and chunk = 0xff
    and id = 0x3ff
    and length = 0x3ff
    in
    let repr = 0xffffffff in
    let is_gstate t = 
      Littlefs.Tag.(compare_abstract_type abstract_type t)
    in
    let t = Littlefs.Tag.parse repr |> Result.get_ok in
    Alcotest.(check bool) "valid bit" valid t.valid;
    Alcotest.(check int) "abstract type = 7" 0
      (is_gstate (fst t.type3));
    Alcotest.(check int) "chunk" chunk (snd t.type3);
    Alcotest.(check int) "id" id t.id;
    Alcotest.(check int) "length" length t.length;
    ()

end

module Superblock = struct
  let test_zero () =
    let cs = Cstruct.(create @@ Littlefs.Superblock.sizeof_superblock) in
    let sb = Littlefs.Superblock.parse cs in
    Alcotest.(check int32) "version" Int32.zero sb.version;
    Alcotest.(check int32) "block size" Int32.zero sb.block_size;
    Alcotest.(check int32) "block count" Int32.zero sb.block_count;
    Alcotest.(check int32) "name length maximum" Int32.zero sb.name_length_max;
    Alcotest.(check int32) "file size maximum" Int32.zero sb.file_size_max;
    Alcotest.(check int32) "file attributes size maximum" Int32.zero sb.file_attribute_size_max


end

let () =
  let tc = Alcotest.test_case in
  Alcotest.run "littlefs" [
    ( "tags", [
          tc "all bits are zero" `Quick Tag.test_zero;
          tc "all fields are 1" `Quick Tag.test_ones;
          tc "all bits are 1" `Quick Tag.test_maxint;
        ]);
    ( "superblock", [
          tc "all bits are zero" `Quick Superblock.test_zero;
      ]);
  ]
