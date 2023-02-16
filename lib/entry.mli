type t = Entry0.t
type link = | Metadata of (int64 * int64)
            | Data of (int32 * int32)

val links : t -> link option
val info_of_entry : t -> (string * [ `Value | `Dictionary]) option

(** [compact l] checks to see whether [l] contains any deletion entries;
 * if so, it removes the entries to be deleted and the corresponding deletion entry,
 * and returns the resulting list.
 *
 * See littlefs documentation on compaction for more information. *)
val compact : t list -> t list

(** [lenv_less_hardtail l] gives the number of bytes necessary to store [l],
 * not including any hardtail entries present in [l]. *)
val lenv_less_hardtail : t list -> int

(** [ctime id d,ps] returns a creation time entry for [id] at [d,ps] *)
val ctime : int -> int * int64 -> t

val ctime_of_cstruct : Cstruct.t -> (int * int64) option

val into_cstructv : starting_xor_tag:Cstruct.t -> Cstruct.t -> t list -> int * Cstruct.t
val to_cstructv : starting_xor_tag:Cstruct.t -> t list -> Cstruct.t * Cstruct.t

val of_cstructv : starting_xor_tag:Cstruct.t -> Cstruct.t -> t list * Cstruct.t * int
