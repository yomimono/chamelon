type t

(* information *)
val seed_tag : t -> Cstruct.t
val last_tag : t -> Cstruct.t
val entries : t -> Entry.t list

(** [of_entries xor_first_tag_with crc l] creates a new commit [t] representing the entries [l].  CRC entries are ommitted. *)
val of_entries_filter_crc : Cstruct.t -> Optint.t -> Entry.t list -> t

(** [commit_after last_commit entries] makes a new commit containing [entries]
 * with its parameters correctly initialized from the information in [last_commit].
 *)
val commit_after : t -> Entry.t list -> t

(* serialization and deserialization *)
val into_cstruct : starting_offset:int -> program_block_size:int -> starting_xor_tag:Cstruct.t -> next_commit_valid:bool -> Cstruct.t -> t -> (int * Cstruct.t)
val of_cstructv : starting_offset:int -> program_block_size:int -> starting_xor_tag:Cstruct.t -> preceding_crc:Optint.t -> Cstruct.t -> t list
