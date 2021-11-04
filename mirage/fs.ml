let program_block_size = 16
(* fairly arbitrary. probably should be specifiable in keys, but y'know *)

let pbs_int32 = Int32.of_int program_block_size

type error = [
  | `Block of Mirage_block.error
  | `KV of Mirage_kv.error
]

type littlefs_write_error = [
    `Too_long (* path exceeds the allowable file name size *)
]

module Make(This_Block: Mirage_block.S) = struct
  type t = {
    block : This_Block.t;
    block_size : int;
    program_block_size : int;
    sector_size : int;
  }

  let last_id = ref 1

  type write_error = [
    | `Block_write of This_Block.write_error
    | `KV_write of Mirage_kv.write_error
    | `Littlefs_write of littlefs_write_error
  ]

  module Allocator = struct
    (* TODO: uh, eventually we'll need a real allocator :sweat_smile: *)
    let next _ = (2l, 3l)
  end

  let block_write_wrap = function
    | Error e -> Error (`Block_write e)
    | Ok _ as o -> o

  let sector_of_block ~block_size ~sector_size n =
    let byte_of_n = Int64.(mul n @@ of_int block_size) in
    Int64.(div byte_of_n @@ of_int sector_size)

  (* a and b are *block* counts *)
  let write_blocks ~block_size ~sector_size ~next_rev_count device a b write_me =
    let open Lwt.Infix in
    let sector_of_block = sector_of_block ~sector_size ~block_size in
    let (a, b) = sector_of_block a, sector_of_block b in
    let block_cs = Littlefs.Block.to_cstruct ~program_block_size ~block_size write_me in
    let revd_block = Littlefs.Block.(of_commits ~revision_count:next_rev_count (commits write_me)) in
    let revd_block_cs = Littlefs.Block.to_cstruct ~program_block_size ~block_size revd_block in

    This_Block.write device a [block_cs] >|= block_write_wrap >>= function
    | Ok () ->
      This_Block.write device b [revd_block_cs] >|= block_write_wrap
    | e -> Lwt.return e

  let add_entries {block_size; program_block_size; block; sector_size} block_number entries =
    let open Lwt.Infix in
    let raw_block = Cstruct.create block_size in
    let sector_number = sector_of_block ~block_size ~sector_size block_number in
    This_Block.read block sector_number [raw_block] >>= function
    | Error _ -> (* TODO: just bailing here is clearly the wrong thing to do;
                    we'd like to return an Error (`Block_write e) *)
      exit 1
    | Ok () ->
      match Littlefs.Block.of_cstruct ~program_block_size raw_block with
      | Error _ -> exit 1
      | Ok old_block ->
        let old_commits = Littlefs.Block.commits old_block in
        let last_commit = List.hd @@ List.rev old_commits in
        let old_revision_count = Littlefs.Block.revision_count old_block in
        let commit = Littlefs.Commit.commit_after last_commit entries in
        let new_block = Littlefs.Block.of_commits ~revision_count:(old_revision_count + 1)
            (old_commits @ [commit])
        in
        Littlefs.Block.into_cstruct ~program_block_size raw_block new_block;
        This_Block.write block block_number [raw_block] >>= function
        | Error e -> Lwt.return @@ Error (`Block_write e)
        | Ok () -> Lwt.return @@ Ok ()

  (* this is *very* deficient -- at the very least,
   * we need to see whether there are existing entries
   * for this name to overwrite,
   * find which metadata blocks are applicable,
   * and we need to figure out whether the write *can* be inline *)
  let write t path data =
    let open Lwt.Infix in
    let data = Cstruct.concat data in
    last_id := !last_id + 1;
    let file = Littlefs.File.write path !last_id data in
    add_entries t 1L file >>= fun _ ->
    add_entries t 0L file

  let connect device ~program_block_size ~block_size : (t, error) result Lwt.t =
    let open Lwt.Infix in
    let block_0, block_1 = Cstruct.(create block_size, create block_size) in
    This_Block.read device 0L [block_0] >>= fun _ ->
    This_Block.read device 1L [block_1] >>= fun _ ->
    match
      Littlefs.Block.of_cstruct ~program_block_size block_0,
      Littlefs.Block.of_cstruct ~program_block_size block_1
    with
    | _b1, _b2 -> begin
        (* TODO: for now, everything we would care about
         * from reading the FS is either hardcoded in
         * the implementation, or needs to be provided
         * in order to read the filesystem. If we can
         * make some meaning out of the blocks, call
         * that good enough. *)
        This_Block.get_info device >>= fun info ->
        let sector_size = info.sector_size in
        Lwt.return (Ok {block = device; sector_size; block_size; program_block_size})
      end

  let format device ~(block_size : int) : (unit, write_error) result Lwt.t =
    let open Lwt.Infix in
    This_Block.get_info device >>= fun {sector_size; size_sectors; _} ->
    (* TODO: there are some error cases here, like a block size that is larger
     * than the available disk, nonsensical block sizes, etc; we should return error variants for those *)
    let block_count =
      let size_in_bytes = Int64.(mul size_sectors @@ of_int sector_size) in
      Int64.(div size_in_bytes @@ of_int block_size |> to_int32)
    in
    let name = Littlefs.Superblock.name in
    let superblock_inline_struct = Littlefs.Superblock.inline_struct (Int32.of_int block_size) block_count in

    let block = Littlefs.Block.of_entries ~revision_count:2 [name; superblock_inline_struct] in
    write_blocks ~block_size ~sector_size ~next_rev_count:3 device 0L 1L block

end
