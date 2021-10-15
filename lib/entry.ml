type t = Tag.t * Cstruct.t

let sizeof t =
  Cstruct.length (snd t) + Tag.size

let into_cstruct cs t =
  Tag.into_cstruct cs (fst t);
  Cstruct.blit (snd t) 0 cs Tag.size (Cstruct.length @@ snd t)

let to_cstruct t =
  let cs = Cstruct.create @@ sizeof t in
  into_cstruct cs t;
  cs

let lenv l =
  List.fold_left (fun sum t -> sum + sizeof t) 0 l

let into_cstructv l cs =
  (* currently this takes a `t list`, and therefore is pretty straightforward.
   * This function exists so we can do better once `t list` is replaced with more complicated *)
  List.fold_left (fun pointer t ->
      into_cstruct (Cstruct.shift cs pointer) t;
      pointer + (sizeof t)
    ) 0 l

(* TODO: it's unclear to my tired mind whether we're supposed
 * to CRC the whole entry, or just the tag *)
let crc start_crc t =
  let tag_crc = Tag.crc start_crc (fst t) in
  Checkseum.Crc32.digest_bigstring (Cstruct.to_bigarray @@ snd t) 0 (Cstruct.length @@ snd t) tag_crc
