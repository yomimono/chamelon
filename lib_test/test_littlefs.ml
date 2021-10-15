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
  
  let read_maxint () =
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

  let write_maxint () =
    let valid = true
    and abstract_type = Littlefs.Tag.LFS_TYPE_GSTATE
    and chunk = 0xff
    and id = 0x3ff
    and length = 0x3ff
    in
    let t = Littlefs.Tag.{ valid; type3 = (abstract_type, chunk); id; length } in
    let cs = Cstruct.create 4 in
    Cstruct.BE.set_uint32 cs 0 Int32.minus_one;
    Alcotest.(check @@ of_pp Cstruct.hexdump_pp) "tag writing: maxint" cs (Littlefs.Tag.to_cstruct ~xor_tag_with:0xffffffffl t)

end

module Superblock = struct
  let test_zero () =
    let cs = Cstruct.(create @@ Littlefs.Superblock.sizeof_superblock) in
    let sb = Littlefs.Superblock.parse cs in
    Alcotest.(check int) "major version" 0 sb.version_major;
    Alcotest.(check int) "minor version" 0 sb.version_minor;
    Alcotest.(check int32) "block size" Int32.zero sb.block_size;
    Alcotest.(check int32) "block count" Int32.zero sb.block_count;
    Alcotest.(check int32) "name length maximum" Int32.zero sb.name_length_max;
    Alcotest.(check int32) "file size maximum" Int32.zero sb.file_size_max;
    Alcotest.(check int32) "file attributes size maximum" Int32.zero sb.file_attribute_size_max


end

module Block = struct
  module Block = Littlefs.Block

  (* what's a reasonable block size? let's assume 4Kib *)
  let block_size = 4096l

  let commit_empty_list () =
    let block = Block.empty in
    let entries = [] in
    let block = Block.commit block_size block entries in
    let commit = List.hd block.commits in
    Alcotest.(check int) "padding should be the whole block less revision count and crc" (4096 - 8) commit.padding;
    Alcotest.(check int) "crc should be CRC of 0xffffffff and 0x00000000" 558161692 (Optint.to_int commit.crc)
end

let () =
  let tc = Alcotest.test_case in
  Alcotest.run "littlefs" [
    ( "tags", [
          tc "read: all bits are zero" `Quick Tag.test_zero;
          tc "read: all fields are 1" `Quick Tag.test_ones;
          tc "read: all bits are 1" `Quick Tag.read_maxint;
          tc "write: all bits are 1" `Quick Tag.write_maxint;
        ]);
    ( "superblock", [
          tc "all bits are zero" `Quick Superblock.test_zero;
      ]);
    ( "block", [
          tc "we can construct a block" `Quick Block.commit_empty_list;
      ]);
  ]
