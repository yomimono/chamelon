let cstruct = Alcotest.testable Cstruct.hexdump_pp Cstruct.equal

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
    (* It may be surprising that the expected case here is zero. The tag itself is set to all 1s, but it needs
     * to be XOR'd with the default value, which is also all 1s, so we end up with all 0s. *)
    Cstruct.BE.set_uint32 cs 0 Int32.zero;
    Alcotest.(check cstruct) "tag writing: maxint" cs (Littlefs.Tag.to_cstruct ~xor_tag_with:0xffffffffl t)

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
    let block = Block.commit ~program_block_size:block_size block entries in
    let commit = List.hd block.commits in
    Alcotest.(check int) "crc should be CRC of 0xffffffff and 0x00000000" 558161692 (Optint.to_int commit.preceding_crc)

  (* mimic the minimal superblock commit made by `mklittlefs` when run on an empty directory, and assert that they match what's expected *)
  let commit_superblock () =
    let revision_count = 1l in
    let block_count = 16 in
    let name = Littlefs.Superblock.name in
    let superblock_inline_struct = Littlefs.Superblock.inline_struct block_size @@ Int32.of_int block_count in
    let start_block = {Littlefs.Block.empty with revision_count;} in
    let block = Littlefs.Block.commit ~program_block_size:16l start_block [
        name;
        superblock_inline_struct;
      ] in
    let cs = Littlefs.Block.to_cstruct ~block_size block in
    let expected_length = 
        4 (* revision count *)
      + 4 (* superblock name tag *)
      + 8 (* "littlefs" *)
      + 4 (* inlinestruct tag *)
      + 24 (* six 4-byte-long int32s *)
      + 4 (* crc tag *)
      + 4 (* crc *)
      + 12 (* padding if the block size is 16l *)
    in
    let not_data_length = (Int32.to_int block_size) - expected_length in
    let data, not_data = Cstruct.split cs expected_length in

    let expected_inline_struct_tag = Cstruct.of_string "\x2f\xe0\x00" in
    let expected_crc = Cstruct.of_string "\x20\x5c\x2b\x6e" in
    (* Cstruct promises that buffers made with `create` are zeroed, so a new one
     * of the right length should be good to test against *)
    let zilch = Cstruct.create not_data_length in
    (* the hard, important bits: the correct XORing of the tags, the CRC *)
    (* there should be *one* commit here, meaning one CRC tag *)
    (* but the cstruct should be block-sized *)
    Alcotest.(check int) "block to_cstruct returns a block-size cstruct" (Int32.to_int block_size) (Cstruct.length cs);
    Alcotest.(check int) "zilch buffer and not_data have the same length" (Cstruct.length zilch) (Cstruct.length not_data);
    Alcotest.(check cstruct) "all zeroes in the non-data zone" zilch not_data;
    Alcotest.(check cstruct) "second tag got xor'd" expected_inline_struct_tag (Cstruct.sub data 0x10 3);
    Alcotest.(check cstruct) "crc matches what's expected" expected_crc (Cstruct.sub data 0x30 4)

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
          tc "write one commit to a block" `Quick Block.commit_superblock;
      ]);
  ]
