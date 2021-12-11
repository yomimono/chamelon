module Make(Sectors: Mirage_block.S)(Clock : Mirage_clock.PCLOCK) : sig

  include Mirage_kv.RW

  val format : t -> (unit, write_error) result Lwt.t
  val connect : Sectors.t -> program_block_size:int -> block_size:int -> (t, error) result Lwt.t

end
