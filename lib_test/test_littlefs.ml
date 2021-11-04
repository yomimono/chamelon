let cstruct = Alcotest.testable Cstruct.hexdump_pp Cstruct.equal

let default_xor = Cstruct.of_string "\xff\xff\xff\xff"
let dont_xor = Cstruct.of_string "\x00\x00\x00\x00"

module Tag = struct
  let test_zero () =
    (* set the least significant bit because 0x0l is
     * explicitly an invalid tag *)
    let cs = Cstruct.of_string "\x00\x00\x00\x01" in
    let t = Littlefs.Tag.of_cstruct ~xor_tag_with:dont_xor cs |> Result.get_ok in
    Alcotest.(check bool) "valid bit" true t.valid
  
  let test_ones () =
    (* each field is 1, but abstract_type 1 is invalid *)
    let repr = Int32.(
        add 0x8000_0000l @@
        add 0x1000_0000l @@
        add 0x0010_0000l @@
        add 0x0000_0400l @@
            0x0000_0001l)
    in
    let tag = Cstruct.create 4 in
    Cstruct.BE.set_uint32 tag 0 repr;
    match Littlefs.Tag.of_cstruct ~xor_tag_with:dont_xor tag with
    | Ok _ -> Alcotest.fail "abstract type 1 was accepted"
    | Error _ -> ()
  
  let read_almost_maxint () =
    let valid = false
    and abstract_type = Littlefs.Tag.LFS_TYPE_GSTATE
    and chunk = 0xff
    and id = 0x3ff
    and length = 0x3ef
    in
    let repr = Cstruct.of_string "\xff\xff\xff\xef" in
    let is_gstate t = 
      Littlefs.Tag.(compare_abstract_type abstract_type t)
    in
    let t = Littlefs.Tag.of_cstruct ~xor_tag_with:dont_xor repr |> Result.get_ok in
    Alcotest.(check bool) "valid bit" valid t.valid;
    Alcotest.(check int) "abstract type = 7" 0
      (is_gstate (fst t.type3));
    Alcotest.(check int) "chunk" chunk (snd t.type3);
    Alcotest.(check int) "id" id t.id;
    Alcotest.(check int) "length" length t.length;
    ()

  let write_maxint () =
    let valid = false
    and abstract_type = Littlefs.Tag.LFS_TYPE_GSTATE
    and chunk = 0xff
    and id = 0x3ff
    and length = 0x3ff
    in
    let t = Littlefs.Tag.{ valid; type3 = (abstract_type, chunk); id; length } in
    (* It may be surprising that the expected case here is zero. The tag itself is set to all 1s, but it needs
     * to be XOR'd with the default value, which is also all 1s, so we end up with all 0s. *)
    let cs = Cstruct.of_string "\x00\x00\x00\x00" in
    Alcotest.(check cstruct) "tag writing: 0xffffffff" cs (Littlefs.Tag.to_cstruct ~xor_tag_with:(Cstruct.of_string "\xff\xff\xff\xff") t)

  let roundtrip () =
    let tag = Cstruct.of_string "\xf0\x0f\xff\xf7" in
    match Littlefs.Tag.of_cstruct ~xor_tag_with:default_xor tag with
    | Error (`Msg e) -> Alcotest.fail e
    | Ok parsed ->
      Alcotest.(check int) "tag has 8 bytes of data" 8 parsed.Littlefs.Tag.length;
      ()

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
  let block_size = 4096
  let program_block_size = 16

  let superblock =
    let revision_count = 1 in
    let block_count = 16 in
    let name = Littlefs.Superblock.name in
    let bs = Int32.of_int block_size in
    let superblock_inline_struct = Littlefs.Superblock.inline_struct bs @@ Int32.of_int block_count in
    Littlefs.Block.of_entries ~revision_count [name; superblock_inline_struct]

  (* mimic the minimal superblock commit made by `mklittlefs` when run on an empty directory, and assert that they match what's expected *)
  let commit_superblock () =
    let block = superblock in
    Alcotest.(check int) "in-memory block structure has 1 commit with 2 entries" 2
      (List.length @@ Littlefs.Commit.entries @@ List.hd @@ Littlefs.Block.commits block);
    let cs = Littlefs.Block.to_cstruct ~program_block_size ~block_size block in
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
    let not_data_length = block_size - expected_length in
    let data, not_data = Cstruct.split cs expected_length in

    Format.printf "data region: %a\n%!" Cstruct.hexdump_pp data;
    let nondata_limit = 32 in
    Format.printf "first %d bytes of non-data region: %a\n%!" nondata_limit
      Cstruct.hexdump_pp (Cstruct.sub not_data 0 nondata_limit);

    let expected_inline_struct_tag = Cstruct.of_string "\x2f\xe0\x00" in
    let expected_crc = Cstruct.of_string "\x50\xff\x0d\x72" in
    (* Cstruct promises that buffers made with `create` are zeroed, so a new one
     * of the right length should be good to test against *)
    let zilch = Cstruct.create not_data_length in
    (* the hard, important bits: the correct XORing of the tags, the CRC *)
    (* there should be *one* commit here, meaning one CRC tag *)
    (* but the cstruct should be block-sized *)
    Alcotest.(check int) "block to_cstruct returns a block-size cstruct" block_size (Cstruct.length cs);
    Alcotest.(check int) "zilch buffer and not_data have the same length" (Cstruct.length zilch) (Cstruct.length not_data);
    Alcotest.(check cstruct) "all zeroes in the non-data zone" zilch not_data;
    Alcotest.(check cstruct) "second tag got xor'd" expected_inline_struct_tag (Cstruct.sub data 0x10 3);
    Alcotest.(check cstruct) "crc matches what's expected" expected_crc (Cstruct.sub data 0x30 4)

  let roundtrip () =
    let block = superblock in
    let written_block = Block.to_cstruct ~program_block_size ~block_size block in
    let read_block = Block.of_cstruct ~program_block_size written_block |> Result.get_ok in
    let commits = Littlefs.Block.commits read_block in
    Alcotest.(check int) "read-back block has a commit" 1 (List.length commits);
    let commit = List.hd commits in
    (* read-back block should have 1 commit with 3 entries in it: the original 2 entries from the superblock, and the CRC tag from the commit *)
    let entries = Littlefs.Commit.entries commit in
    Alcotest.(check int) "read-back commit has 2 entries" 2 (List.length entries);

end

module Entry = struct
  let roundtrip () =
    let block = Block.superblock in
    let commit = List.hd @@ Littlefs.Block.commits block in
    let entries = Littlefs.Commit.entries commit in
    let default_tag = Cstruct.of_string "\xff\xff\xff\xff" in
    let (_last_tag, serialized) = Littlefs.Entry.to_cstructv ~starting_xor_tag:default_tag entries in
    Stdlib.Format.printf "serialized entry list: %a\n" Cstruct.hexdump_pp serialized;
    let (parsed, _last_tag, _s) = Littlefs.Entry.of_cstructv ~starting_xor_tag:default_tag serialized in
    Alcotest.(check int) "parsed entry list is same length as original" (List.length entries) (List.length parsed);
    let first_parsed = List.hd parsed in
    (* look it's, polymorphic eq (polymorphic eq), the fn who found, a way to crash stuff *)
    Alcotest.(check string) "first parsed entry has 'littlefs' data" "littlefs" (Cstruct.to_string @@ snd first_parsed);
    let snd_parsed = List.nth parsed 1 in
    Alcotest.(check string) "first parsed entry has 'littlefs' data" "littlefs" (Cstruct.to_string @@ snd first_parsed);
    Alcotest.(check string) "second parsed entry has the right version" "\x00\x00\x02\x00" Cstruct.(to_string @@ sub (snd snd_parsed) 0 4)

end

let () =
  let tc = Alcotest.test_case in
  Alcotest.run "littlefs" [
    ( "tags", [
          tc "read: valid bit" `Quick Tag.test_zero;
          tc "read: all fields are 1" `Quick Tag.test_ones;
          tc "read: almost all bits are 1" `Quick Tag.read_almost_maxint;
          tc "write: all bits are 1" `Quick Tag.write_maxint;
          tc "roundtrip print/parse for a data-bearing tag" `Quick Tag.roundtrip;
        ]);
    ( "superblock", [
          tc "all bits are zero" `Quick Superblock.test_zero;
      ]);
    ( "block", [
          tc "write a superblock commit to a block" `Quick Block.commit_superblock;
      ]);
    ( "entry", [
          tc "entry roundtrip" `Quick Entry.roundtrip;
        ]);
    ( "roundtrip", [
          tc "you got a parser and printer, you know what to do" `Quick Block.roundtrip;
        ]);
  ]
