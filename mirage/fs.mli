module Make(Sectors : Mirage_block.S) : sig
  type t

  type write_result = [ `No_space | `Split | `Split_emergency ]

  type key = Mirage_kv.Key.t
  type error = Mirage_kv.error
  type write_error = Mirage_kv.write_error

  val name_length_max : t -> int32

  val last_modified_value : t -> key -> ((int * int64) , error) result Lwt.t

  val connect : program_block_size:int -> block_size:int -> Sectors.t -> (t, error) result Lwt.t
  val format : program_block_size:int -> block_size:int -> Sectors.t -> (unit, write_error) result Lwt.t

  val mkdir : t -> (int64 * int64) -> string list -> ((int64 * int64), [`No_space | `Not_found of string] ) result Lwt.t

  val dump : Format.formatter -> t -> unit Lwt.t

  module Delete : sig
    val delete_in_directory : (int64 * int64) -> t -> string -> (unit, write_error) result Lwt.t

  end

  module File_read : sig
    val get : t -> Mirage_kv.Key.t -> (string, error) result Lwt.t
    val get_partial : t -> Mirage_kv.Key.t -> offset:int -> length:int ->
       (string, error) result Lwt.t
  end

  module File_write : sig
    (** [set_in_directory directory_head t filename data] creates entries in
     * [directory] for [filename] pointing to [data] *)
    val set_in_directory : (int64 * int64) -> t -> string -> string ->
      (unit, write_error) result Lwt.t
  end

  module Find : sig
    type blockwise_entry_list = (int64 * int64) * (Chamelon.Entry.t list)

    (** [find_first_blockpair_of_directory t head l] finds and enters
     *  the segments in [l] recursively until none remain.
     *  It returns `No_id if an entry is not present and `No_structs if an entry
     *  is present, but does not represent a valid directory. *)
    val find_first_blockpair_of_directory : t -> (int64 * int64) -> string list ->
      [`Final_dir_on of (int64 * int64) | `No_id of string | `No_structs] Lwt.t

    (** [all_entries_in_dir t head] gives an *uncompacted* list of all
     * entries in the directory starting at [head].
     * the returned entries in the directory are split up by block,
     * so the caller can distinguish between re-uses of the same ID number
     * when the directory spans multiple block numbers *)
    val all_entries_in_dir : t -> (int64 * int64) ->
      (blockwise_entry_list list, error) result Lwt.t
  end

  module Size : sig
    val size : t -> key -> (Optint.Int63.t, error) result Lwt.t
  end


end
