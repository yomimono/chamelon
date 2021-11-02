type t = Tag.t * Cstruct.t

let sizeof t =
  Cstruct.length (snd t) + Tag.size

let into_cstruct ~xor_tag_with cs t =
  Tag.into_cstruct ~xor_tag_with cs @@ fst t;
  Cstruct.blit (snd t) 0 cs Tag.size (Cstruct.length @@ snd t)

let to_cstruct ~xor_tag_with t =
  let cs = Cstruct.create @@ sizeof t in
  into_cstruct ~xor_tag_with cs t;
  cs

let lenv l =
  List.fold_left (fun sum t -> sum + sizeof t) 0 l

let into_cstructv ~starting_xor_tag cs l =
  (* currently this takes a `t list`, and therefore is pretty straightforward.
   * This function exists so we can do better once `t list` is replaced with more complicated *)
  List.fold_left (fun (pointer, prev_tag) t ->
      into_cstruct ~xor_tag_with:prev_tag (Cstruct.shift cs pointer) t;
      let tag = Tag.to_cstruct_raw (fst t) in
      (pointer + (sizeof t), tag)
    ) (0, starting_xor_tag) l

let to_cstructv ~starting_xor_tag l =
  let cs = Cstruct.create @@ lenv l in
  let _ = into_cstructv ~starting_xor_tag cs l in
  cs
