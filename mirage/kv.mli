module Make(Sectors: Mirage_block.S)(Clock : Mirage_clock.PCLOCK) : sig

  include Mirage_kv.RW

  val format : program_block_size:int -> Sectors.t -> (unit, write_error) result Lwt.t
  val connect : program_block_size:int -> Sectors.t -> (t, error) result Lwt.t
  val size : t -> key -> (int, error) result Lwt.t

  (** [get_partial t k ~offset ~length] gives errors for length 0, negative offsets and lengths, and partial reads off the end of the file.  [get_partial t k ~offset ~length] should always give a result of (Ok v) where String.length v = length, or an error. *)
  val get_partial : t -> key -> offset:int -> length:int -> (string, error) result Lwt.t

end
