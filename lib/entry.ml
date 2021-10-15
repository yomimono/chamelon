type t = Tag.t * Cstruct.t

let into_cstruct t cs =
  Tag.into_cstruct cs (fst t);
  Cstruct.blit (snd t) 0 cs Tag.size (Cstruct.len @@ snd t)

let to_cstruct t =y
  let cs = Cstruct.create @@ (Cstruct.len @@ snd t) + Tag.size in
  into_cstruct t cs;
  cs

let into_cstructv l cs =
  (* currently this takes a `t list`, and therefore is a straightforward List.iter .
   * This function exists so we can do better once `t list` is more complicated *)
