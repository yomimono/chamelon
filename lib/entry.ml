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
  List.fold_left (fun sum t ->
      Printf.printf "entry of size %d added to the sum\n%!" (sizeof t);
      sum + sizeof t) 0 l

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
  let (_, last_tag) = into_cstructv ~starting_xor_tag cs l in
  last_tag, cs

(** [of_cstructv cs] returns [(l, t, s)] where [l] is the list of (tag, entry) pairs discovered
 * preceding the next CRC entry.
 * [t] the last tag (un-xor'd) for use in seeding future reads or writes,
 * [s] the number of bytes read from [cs], including (if present and read) the CRC tag,
 * data, and any padding. *)
let of_cstructv ~starting_xor_tag cs =
  let tag ~xor_tag_with cs =
    if Cstruct.length cs < Tag.size then None
    else begin
      match Tag.of_cstruct ~xor_tag_with (Cstruct.sub cs 0 Tag.size) with
      | Error _ -> None
      | Ok tag ->
        let total_length = Tag.size + tag.length in
        if total_length <= Cstruct.length cs
        then Some (tag, Cstruct.sub cs Tag.size tag.length)
        else None
    end
  in
  let rec gather (l, last_tag, s) cs =
    match tag ~xor_tag_with:last_tag cs with
    | None -> (List.rev l, last_tag, s)
    | Some (tag, data) ->
      match tag.Tag.type3 with
      | Tag.LFS_TYPE_CRC, _chunk ->
        (* omit the CRC tag from the results, but make sure to return the amount
         * of data we read including it *)
        (List.rev l, Cstruct.sub cs 0 Tag.size,
         (s + Tag.size + (Cstruct.length data)))
      | _ ->

        gather ((tag, data) :: l,
                Cstruct.sub cs 0 Tag.size,
                s + Tag.size + Cstruct.length data
               )
          (Cstruct.shift cs (Tag.size + tag.Tag.length ))
  in
  gather ([], starting_xor_tag, 0) cs
