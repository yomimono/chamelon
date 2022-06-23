module Make(Sectors: Mirage_block.S)(Clock : Mirage_clock.PCLOCK) : sig

  include Mirage_kv.RW

  val format : program_block_size:int -> Sectors.t -> (unit, write_error) result Lwt.t
  val connect : program_block_size:int -> Sectors.t -> (t, error) result Lwt.t
  val size: t -> key -> (int, error) result Lwt.t

end
