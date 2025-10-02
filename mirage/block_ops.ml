(* unfortunately, "block" is a really overloaded term in this codebase :/ *)

(* the module providing Mirage_block.S expects operations to be specified in
 * terms of *sectors*, which is not the level on which littlefs wants to
 * operate natively - we want to address blocks.
 * Provide an intermediate interface that implements block operations on top of
 * the sector-based API provided by a Mirage_block.S implementor. *)

module type Block_shim = sig
  include Mirage_block.S
  val block_count : t -> int
end

module Make(Sectors : Mirage_block.S) : sig
  include Block_shim
  val connect : block_size:int -> Sectors.t -> t Lwt.t
end = struct
  type t = { sectors : Sectors.t;
             sector_size : int; (* how big is each sector? *)
             size_sectors : int64; (* how many sectors is the device? *)
             block_size : int; (* how big is each block? *)
           }

  type error = Sectors.error
  type write_error = Sectors.write_error

  let pp_error = Sectors.pp_error
  let pp_write_error = Sectors.pp_write_error

  let log_src = Logs.Src.create "chamelon-shim" ~doc:"chamelon block-to-sector shim"
  module Log = (val Logs.src_log log_src : Logs.LOG)

  let sector_of_block t n =
    let byte_of_n = Int64.(mul n @@ of_int t.block_size) in
    Int64.(div byte_of_n @@ of_int t.sector_size)

  let block_count t =
    (Int64.to_int t.size_sectors) * t.sector_size / t.block_size

  let connect ~block_size sectors =
    let open Lwt.Infix in
    Sectors.get_info sectors >>= fun info ->
    let sector_size = info.sector_size in
    let size_sectors = info.size_sectors in
    Log.debug (fun f -> f "got info from a device with sector size %d (0x%x) and %Ld (0x%Lx) sectors available"
                   info.sector_size info.sector_size
                   info.size_sectors info.size_sectors);
    Lwt.return @@ 
    {block_size;
     sector_size;
     size_sectors;
     sectors;
    }

  let disconnect t = Sectors.disconnect t.sectors

  let get_info t = Sectors.get_info t.sectors

  let write t block_number = Sectors.write t.sectors (sector_of_block t block_number)
  let read t block_number = Sectors.read t.sectors (sector_of_block t block_number)

end
