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

let lenv_less_padding l =
  List.fold_left (fun sum t ->
      match (fst t).Tag.type3 |> fst with
      | Tag.LFS_TYPE_CRC -> sum + Tag.size + 4
      | _ -> sum + sizeof t)
    0 l

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

(** [of_cstructv cs] returns [(l, t)] where [l] is a list of (tag, entry) pairs discovered, and [t] the last tag (un-xor'd) for use in seeding future reads or writes. *)
let of_cstructv ~starting_xor_tag cs =
  let tag ~xor_tag_with cs =
    if Cstruct.length cs < Tag.size then None
    else begin
      match Tag.of_cstruct ~xor_tag_with (Cstruct.sub cs 0 Tag.size) with
      | Error _ -> None
      | Ok tag ->
        let unpadded_length =
          match tag.Tag.type3 |> fst with
          (* special case here: the tag will
           * have the padding included in its length.
           * luckily the real length of the CRC is constant *)
          | Tag.LFS_TYPE_CRC -> 4
          | _ -> tag.length
        in
        if unpadded_length + Tag.size <= Cstruct.length cs
        then Some (tag, Cstruct.sub cs Tag.size unpadded_length)
        else None
    end
  in
  let rec gather (l, last_tag) cs =
    match tag ~xor_tag_with:last_tag cs with
    | None -> (List.rev l, last_tag)
    | Some (tag, data) ->
      gather ((tag, data) :: l, Cstruct.sub cs 0 Tag.size)
      (Cstruct.shift cs (Tag.size + tag.Tag.length ))
  in
  gather ([], starting_xor_tag) cs
