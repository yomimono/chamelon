module Make(Sectors: Mirage_block.S)(Clock : Mirage_clock.PCLOCK) : sig

  type t
  type key = Mirage_kv.Key.t
  type error = [
    | `Not_found           of key (** key not found *)
    | `Dictionary_expected of key (** key does not refer to a dictionary. *)
    | `Value_expected      of key (** key does not refer to a value. *)
  ]
  type write_error = [
    | error
    | `No_space                (** No space left on the device. *)
    | `Too_many_retries of int (** {!batch} has been trying to commit [n] times
                                   without success. *)
  ]

  val pp_error : error Fmt.t
  val pp_write_error : write_error Fmt.t

  val format : t -> (unit, write_error) result Lwt.t
  val connect : Sectors.t -> program_block_size:int -> block_size:int -> (t, error) result Lwt.t

  val exists : t -> key -> ([`Value | `Dictionary] option, error) result Lwt.t
  val get : t -> key -> (string, error) result Lwt.t
  val list : t -> key -> ((string * [`Value | `Dictionary]) list, error) result Lwt.t
  val last_modified : t -> key -> (int * int64, error) result Lwt.t

  val set: t -> key -> string -> (unit, write_error) result Lwt.t
  val remove : t -> key -> (unit, write_error) result Lwt.t

end
