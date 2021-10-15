type t = Tag.t * Cstruct.t

let sizeof t =
  Cstruct.length (snd t) + Tag.size

let into_cstruct ~xor_tag_with cs t =
  Tag.into_cstruct ~xor_tag_with cs (fst t);
  Cstruct.blit (snd t) 0 cs Tag.size (Cstruct.length @@ snd t)

let to_cstruct ~xor_tag_with t =
  let cs = Cstruct.create @@ sizeof t in
  into_cstruct ~xor_tag_with cs t;
  cs

let lenv l =
  List.fold_left (fun sum t -> sum + sizeof t) 0 l

let into_cstructv l cs =
  (* currently this takes a `t list`, and therefore is pretty straightforward.
   * This function exists so we can do better once `t list` is replaced with more complicated *)
  List.fold_left (fun (pointer, prev_tag) t ->
      into_cstruct ~xor_tag_with:prev_tag (Cstruct.shift cs pointer) t;
      let tag = Tag.to_int32 (fst t) in
      (pointer + (sizeof t), tag)
    ) (0, 0xffffffffl) l

(* TODO: it's unclear to my tired mind whether we're supposed
 * to CRC the whole entry, or just the tag *)
let crc start_crc t =
  Checkseum.Crc32.digest_bigstring (Cstruct.to_bigarray @@ snd t) 0 (Cstruct.length @@ snd t) start_crc
