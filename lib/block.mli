type t

module IdSet : Set.S with type elt := int

val commits : t -> Commit.t list
val revision_count : t -> int

(* given a list of commits, assemble a block *)
val of_commits : revision_count:int -> Commit.t list -> t

(* start a new block with one commit containing these entries *)
val of_entries : revision_count:int -> Entry.t list -> t

(* add a list of entries to an already-existing block *)
val add_commit : t -> Entry.t list -> t

(* which IDs are already used? *)
val ids : t -> IdSet.t

type write_result = [ `Ok | `Split | `Split_emergency ]

val into_cstruct : program_block_size:int -> Cstruct.t -> t -> write_result
val to_cstruct : program_block_size:int -> block_size:int -> t ->
  Cstruct.t * write_result
val of_cstruct : program_block_size:int -> Cstruct.t -> (t, [`Msg of string]) result
