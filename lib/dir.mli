(** [hard_tail_links t] returns the blockpair [t] points at
 * if [t] is a hard tail entry *)
val hard_tail_links : Tag.t * Cstruct.t -> (int64 * int64) option

val hard_tail_at : (int64 * int64) -> Entry.t

(** [of_entry t] returns the blockpair on which the directory contents start
 * if [t] is a directory structure entry *)
val of_entry : Entry.t -> (int64 * int64) option

(** [name str id] makes an entry linking [id] with [name]. *)
val name : string -> int -> Entry.t

(** [mkdir ~to_pair id] makes a directory structure entry. *)
val mkdir : to_pair:(int64 * int64) -> int -> Entry.t

val dirstruct_of_cstruct : Cstruct.t -> (int64 * int64) option
