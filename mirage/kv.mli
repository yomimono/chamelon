(** Kv.Make provides the module fulfilling Mirage_kv.RW, plus a few bonus calls.
 * Many functions contain calls to the Fs module, which provides lower-level operations
 * dealing directly with on-disk structures. *)

module Make(Sectors: Mirage_block.S) : sig

  include Mirage_kv.RW

  val format : program_block_size:int -> Sectors.t -> (unit, write_error) result Lwt.t
  val connect : program_block_size:int -> Sectors.t -> (t, error) result Lwt.t
  val size : t -> key -> (int, error) result Lwt.t

end
