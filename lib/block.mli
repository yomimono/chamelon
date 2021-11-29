type t

module IdSet : Set.S with type elt := int

(* [entries t] is equivalent to mapping `Commit.entries` over all commits in `t`,
 * and is provided for convenience *)
val entries : t -> Entry.t list
val commits : t -> Commit.t list
val revision_count : t -> int

val split : t -> int64 * int64 -> t * t
val linked_blocks : t -> Entry.link list

(* given a list of commits, assemble a block *)
val of_commits : revision_count:int -> Commit.t list -> t

(* start a new block with one commit containing these entries *)
val of_entries : revision_count:int -> Entry.t list -> t

(* remove entries that have been deleted, and compact all commits into one *)
val compact : t -> t

(* add a list of entries to an already-existing block *)
val add_commit : t -> Entry.t list -> t

(* which IDs are already used? *)
val ids : t -> IdSet.t

type write_result = [ `Ok | `Split | `Split_emergency ]

val into_cstruct : program_block_size:int -> Cstruct.t -> t -> write_result
val to_cstruct : program_block_size:int -> block_size:int -> t ->
  Cstruct.t * write_result
val of_cstruct : program_block_size:int -> Cstruct.t -> (t, [`Msg of string]) result
