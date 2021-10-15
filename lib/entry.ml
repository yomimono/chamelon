type t = Tag.t * Cstruct.t

let sizeof t =
  Cstruct.length (snd t) + Tag.size

let into_cstruct ~prev_tag cs t =
  Tag.into_cstruct ~prev_tag cs (fst t);
  Cstruct.blit (snd t) 0 cs Tag.size (Cstruct.length @@ snd t)

let to_cstruct ~prev_tag t =
  let cs = Cstruct.create @@ sizeof t in
  into_cstruct ~prev_tag cs t;
  cs

let lenv l =
  List.fold_left (fun sum t -> sum + sizeof t) 0 l

let into_cstructv l cs =
  (* currently this takes a `t list`, and therefore is pretty straightforward.
   * This function exists so we can do better once `t list` is replaced with more complicated *)
  List.fold_left (fun (pointer, prev_tag) t ->
      into_cstruct ~prev_tag (Cstruct.shift cs pointer) t;
      (pointer + (sizeof t), (Cstruct.sub (Cstruct.shift cs pointer) 0 4))
    ) (0, Cstruct.of_string "\xff\xff\xff\xff") l

(* TODO: it's unclear to my tired mind whether we're supposed
 * to CRC the whole entry, or just the tag *)
let crc ~prev_tag start_crc t =
  let tag = Tag.to_cstruct ~prev_tag @@ fst t in
  let tag_crc = Tag.crc start_crc tag in
  Checkseum.Crc32.digest_bigstring (Cstruct.to_bigarray @@ snd t) 0 (Cstruct.length @@ snd t) tag_crc
