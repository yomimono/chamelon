let cstruct = Alcotest.testable Cstruct.hexdump_pp Cstruct.equal

let default_xor = Cstruct.of_string "\xff\xff\xff\xff"
let dont_xor = Cstruct.of_string "\x00\x00\x00\x00"

module Tag = struct
  let test_zero () =
    (* set the least significant bit because 0x0l is
     * explicitly an invalid tag *)
    let cs = Cstruct.of_string "\x00\x00\x00\x01" in
    let t = Chamelon.Tag.of_cstruct ~xor_tag_with:dont_xor cs |> Result.get_ok in
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
    match Chamelon.Tag.of_cstruct ~xor_tag_with:dont_xor tag with
    | Ok _ -> Alcotest.fail "abstract type 1 was accepted"
    | Error _ -> ()
  
  let read_almost_maxint () =
    let valid = false
    and abstract_type = Chamelon.Tag.LFS_TYPE_GSTATE
    and chunk = 0xff
    and id = 0x3ff
    and length = 0x3ef
    in
    let repr = Cstruct.of_string "\xff\xff\xff\xef" in
    let is_gstate t = 
      Chamelon.Tag.(compare_abstract_type abstract_type t)
    in
    let t = Chamelon.Tag.of_cstruct ~xor_tag_with:dont_xor repr |> Result.get_ok in
    Alcotest.(check bool) "valid bit" valid t.valid;
    Alcotest.(check int) "abstract type = 7" 0
      (is_gstate (fst t.type3));
    Alcotest.(check int) "chunk" chunk (snd t.type3);
    Alcotest.(check int) "id" id t.id;
    Alcotest.(check int) "length" length t.length;
    ()

  let write_maxint () =
    let valid = false
    and abstract_type = Chamelon.Tag.LFS_TYPE_GSTATE
    and chunk = 0xff
    and id = 0x3ff
    and length = 0x3ff
    in
    let t = Chamelon.Tag.{ valid; type3 = (abstract_type, chunk); id; length } in
    (* It may be surprising that the expected case here is zero. The tag itself is set to all 1s, but it needs
     * to be XOR'd with the default value, which is also all 1s, so we end up with all 0s. *)
    let cs = Cstruct.of_string "\x00\x00\x00\x00" in
    Alcotest.(check cstruct) "tag writing: 0xffffffff" cs (Chamelon.Tag.to_cstruct ~xor_tag_with:(Cstruct.of_string "\xff\xff\xff\xff") t)

  let roundtrip () =
    let tag = Cstruct.of_string "\xf0\x0f\xff\xf7" in
    match Chamelon.Tag.of_cstruct ~xor_tag_with:default_xor tag with
    | Error (`Msg e) -> Alcotest.fail e
    | Ok parsed ->
      Alcotest.(check int) "tag has 8 bytes of data" 8 parsed.Chamelon.Tag.length;
      ()

end

module Block = struct
  module Block = Chamelon.Block

  (* what's a reasonable block size? let's assume 4Kib *)
  let block_size = 4096
  let program_block_size = 16

  let superblock =
    let revision_count = 1 in
    let block_count = 16 in
    let name = Chamelon.Superblock.name in
    let bs = Int32.of_int block_size in
    let superblock_inline_struct = Chamelon.Superblock.inline_struct bs @@ Int32.of_int block_count in
    Chamelon.Block.of_entries ~revision_count [name; superblock_inline_struct]

  let example_block =
    let entries = [
      Chamelon.File.name "one" 1;
      Chamelon.File.create_ctz 1 ~pointer:2l ~file_size:1024l;
      Chamelon.File.name "two" 2;
      Chamelon.File.create_inline 2 (Cstruct.create 8), Cstruct.create 8;
      Chamelon.File.name "three" 3;
      Chamelon.Dir.mkdir ~to_pair:(2L, 3L) 3;
    ] in
    Chamelon.Block.add_commit superblock entries

  (* mimic the minimal superblock commit made by `mklittlefs` when run on an empty directory, and assert that they match what's expected *)
  let commit_superblock () =
    let block = superblock in
    Alcotest.(check int) "in-memory block structure has 1 commit with 2 entries" 2
      (List.length @@ Chamelon.Commit.entries @@ List.hd @@ Chamelon.Block.commits block);
    let cs, _res = Chamelon.Block.to_cstruct ~program_block_size ~block_size block in
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
    let written_block, _res = Block.to_cstruct ~program_block_size ~block_size block in
    let read_block = Block.of_cstruct ~program_block_size written_block |> Result.get_ok in
    let commits = Chamelon.Block.commits read_block in
    Alcotest.(check int) "read-back block has a commit" 1 (List.length commits);
    let commit = List.hd commits in
    (* read-back block should have 1 commit with 3 entries in it: the original 2 entries from the superblock, and the CRC tag from the commit *)
    let entries = Chamelon.Commit.entries commit in
    Alcotest.(check int) "read-back commit has 2 entries" 2 (List.length entries);
    ()

  let revision_count_matters () =
    let block = superblock in
    let commits = Chamelon.Block.commits block in
    let revision_count = Chamelon.Block.revision_count block in
    let new_rev_count = revision_count + 1 in
    let revised_block = Chamelon.Block.of_commits ~hardtail:None ~revision_count:new_rev_count commits in
    let original_block_serialized, _ = Chamelon.Block.to_cstruct ~program_block_size ~block_size block in
    let incremented_block_serialized, _ = Chamelon.Block.to_cstruct ~program_block_size ~block_size revised_block in
    let orig_crc = Cstruct.(to_string @@ sub original_block_serialized 0x30 4) in
    let incremented_crc = Cstruct.(to_string @@ sub incremented_block_serialized 0x30 4) in
    Alcotest.(check bool) "crcs shouldn't be equal after revision count change" false (String.equal orig_crc incremented_crc)

  let compact_not_lossy () =
    let pp_entry fmt (tag, cs) =
      Format.fprintf fmt "%a: data length %d" Chamelon.Tag.pp tag (Cstruct.length cs) in
    let eq entry1 entry2 =
      let cs1, cs2 = Cstruct.(create block_size, create block_size) in
      let _ = Chamelon.Entry.into_cstructv ~starting_xor_tag:default_xor cs1 [entry1] in
      let _ = Chamelon.Entry.into_cstructv ~starting_xor_tag:default_xor cs2 [entry2] in
      Cstruct.equal cs1 cs2
    in
    let pre_split = example_block in
    let entry = Alcotest.testable pp_entry eq in
    Alcotest.(check @@ list entry) "compact should have no effect on the entry list in a block with no deletes" (Chamelon.Block.entries pre_split) Chamelon.Block.(entries @@ compact pre_split);
    ()

  let compact_removes_entries () =
    let sans_delete = example_block in
    let with_delete = Chamelon.Block.add_commit sans_delete @@ [(Chamelon.Tag.delete 3), Cstruct.empty] in
    let compacted = Chamelon.Block.compact with_delete in
    let compacted_is_smaller than = List.length (Chamelon.Block.entries compacted) < List.length (Chamelon.Block.entries than) in
    Alcotest.(check @@ bool) "compacted should have fewer entries" true @@ compacted_is_smaller with_delete;
    Alcotest.(check @@ bool) "compacted should have fewer entries" true @@ compacted_is_smaller sans_delete;
    ()

  let split_splits () =
    let pp_entry fmt (tag, cs) =
      Format.fprintf fmt "%a: data length %d" Chamelon.Tag.pp tag (Cstruct.length cs) in
    let pre_split = example_block in
    let new_block_address = (4L, 5L) in
    Format.printf "entries in the pre-split block: %a\n" Fmt.(list pp_entry) (Chamelon.Block.entries pre_split);

    let old_block, new_block = Chamelon.Block.split pre_split new_block_address in
    Format.printf "entries in old block: %a\n" Fmt.(list pp_entry) (Chamelon.Block.entries old_block);
    Alcotest.(check int) "old block should have its existing entries" 8 (List.length @@ Chamelon.Block.entries old_block);
    Alcotest.(check @@ option @@ pair int64 int64) "old block should now have a correct hardtail" (Some new_block_address) (Chamelon.Block.hardtail old_block);
    Format.printf "entries in new block: %a\n" Fmt.(list pp_entry) (Chamelon.Block.entries new_block);
    Alcotest.(check int) "new block should have its first CRC" 0 (List.length @@ Chamelon.Block.entries new_block);
    Alcotest.(check @@ option @@ pair int64 int64) "new block shouldn't have a hardtail" None (Chamelon.Block.hardtail new_block)

  let roundtrip_hardtail () =
    let pre_split = example_block in
    let new_block_address = (4L, 5L) in
    let old_block, _new_block = Chamelon.Block.split pre_split new_block_address in
    let cs = Cstruct.create block_size in
    match Chamelon.Block.into_cstruct ~program_block_size cs old_block with
    | `Unwriteable -> Alcotest.fail "this block should be extremely writeable"
    | `Split -> Alcotest.fail "it's not split time now"
    | `Split_emergency -> Alcotest.fail "it's not emergency split time now"
    | `Ok ->
      match Chamelon.Block.of_cstruct ~program_block_size cs with
      | Error _ -> Alcotest.fail "error rereading the block we wrote"
      | Ok reread_old_block ->
        Alcotest.(check @@ option @@ pair int64 int64) "old block should have a correct hardtail when we reread it" (Some new_block_address) (Chamelon.Block.hardtail reread_old_block)


end

module Entry = struct

  (* I can think of a *lot* of good properties for `compact` --
   * you should never get more entries than you started with;
   * no list returned should have >1 name entry for the same id
   * no list returned should have deletion entries *)

  let roundtrip () =
    let block = Block.superblock in
    let commit = List.hd @@ Chamelon.Block.commits block in
    let entries = Chamelon.Commit.entries commit in
    let default_tag = Cstruct.of_string "\xff\xff\xff\xff" in
    let (_last_tag, serialized) = Chamelon.Entry.to_cstructv ~starting_xor_tag:default_tag entries in
    Stdlib.Format.printf "serialized entry list: %a\n" Cstruct.hexdump_pp serialized;
    let (parsed, _last_tag, _s) = Chamelon.Entry.of_cstructv ~starting_xor_tag:default_tag serialized in
    Alcotest.(check int) "parsed entry list is same length as original" (List.length entries) (List.length parsed);
    let first_parsed = List.hd parsed in
    (* look it's, polymorphic eq (polymorphic eq), the fn who found, a way to crash stuff *)
    Alcotest.(check string) "first parsed entry has 'littlefs' data" "littlefs" (Cstruct.to_string @@ snd first_parsed);
    let snd_parsed = List.nth parsed 1 in
    Alcotest.(check string) "first parsed entry has 'littlefs' data" "littlefs" (Cstruct.to_string @@ snd first_parsed);
    Alcotest.(check string) "second parsed entry has the right version" "\x00\x00\x02\x00" Cstruct.(to_string @@ sub (snd snd_parsed) 0 4)

end

module File = struct

  let last_block () =
    let block_size = 512 in
    Alcotest.(check int) "tiny file" 0 @@ Chamelon.File.last_block_index ~file_size:1 ~block_size;
    Alcotest.(check int) "one-block file" 0 @@ Chamelon.File.last_block_index ~file_size:block_size ~block_size;
    Alcotest.(check int) "one block and change" 1 @@ Chamelon.File.last_block_index ~file_size:(block_size + block_size / 2) ~block_size;
    Alcotest.(check int) "maximal two-block file" 1 @@ Chamelon.File.last_block_index
      ~file_size:(block_size + (block_size - 4)) ~block_size;
    Alcotest.(check int) "minimal three-block file" 2 @@ Chamelon.File.last_block_index
      ~file_size:(block_size + (block_size - 4) + 1) ~block_size

  let size () =
    let not_a_file = Chamelon.Dir.mkdir ~to_pair:(2L, 3L) 1 in
    match Chamelon.Content.size not_a_file with
    | `File _ -> Alcotest.fail "gave a file size for a directory"
    | `Skip -> Alcotest.fail "inappropriately skipped a directory"
    | `Dir p ->
      Alcotest.(check @@ pair int64 int64) "dirpair to recurse into for size" (2L, 3L) p;
      let ctz = Chamelon.File.create_ctz 2 ~pointer:4l ~file_size:10l in
      match Chamelon.Content.size ctz with
      | `Dir _ | `Skip -> Alcotest.fail "didn't get file size for a ctz"
      | `File n ->
        Alcotest.(check int) "file size for a ctz" 10 n;
        let v = Cstruct.of_string "pies" in
        let inline = Chamelon.File.create_inline 3 v in
        match Chamelon.Content.size (inline, v) with
        | `Dir _ | `Skip -> Alcotest.fail "didn't get filesize for inlien file"
        | `File n -> Alcotest.(check int) "file size for an inline file" (Cstruct.length v) n

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
    ( "block", [
          tc "write a superblock commit to a block" `Quick Block.commit_superblock;
          tc "writing a block with different revision count gives different CRC" `Quick Block.revision_count_matters;
          tc "compacting a block with no deletes has no effect on contents" `Quick Block.compact_not_lossy;
          tc "compacting a block with deletes gives fewer entries" `Quick Block.compact_removes_entries;
          tc "splitting a block gets the hardtail right" `Quick Block.split_splits;
          tc "roundtrip hardtail conservation" `Quick Block.roundtrip_hardtail;
      ]);
    ( "entry", [
          tc "entry roundtrip" `Quick Entry.roundtrip;
        ]);
    ( "file", [
          tc "tricky last block index values" `Quick File.last_block;
          tc "sizes of entries" `Quick File.size;
        ]);
    ( "roundtrip", [
          tc "you got a parser and printer, you know what to do" `Quick Block.roundtrip;
        ]);
  ]
