type t = Tag.t * Cstruct.t
type link = | Metadata of (int64 * int64)
            | Data of (int32 * int32)

let pp_link fmt = function
  | Metadata m -> Fmt.pf fmt "metadata %a" Fmt.(pair int64 int64) m
  | Data d -> Fmt.pf fmt "data %a" Fmt.(pair int32 int32) d

let sizeof t =
  Cstruct.length (snd t) + Tag.size

let info_of_entry (tag, data) =
  match tag.Tag.type3 with
  | (LFS_TYPE_NAME, 0x01) ->
    Some (Cstruct.to_string data, `Value)
  | (LFS_TYPE_NAME, 0x02) ->
    Some (Cstruct.to_string data, `Dictionary)
  | _ -> None

let ctime id (d, ps) =
  let cs = Cstruct.create @@ 4 + 8 in
  Cstruct.LE.set_uint32 cs 0 (Int32.of_int d);
  Cstruct.LE.set_uint64 cs 4 ps;
  Tag.({
      valid = true;
      type3 = (LFS_TYPE_USERATTR, 0x74);
      length = 4 + 8;
      id;
    }), cs

let ctime_of_cstruct cs =
  if Cstruct.length cs < 4 + 8 then None
  else begin
    let d = Cstruct.LE.get_uint32 cs 0 |> Int32.to_int in
    let ps = Cstruct.LE.get_uint64 cs 4 in
    Some (d, ps)
  end

let into_cstruct ~xor_tag_with cs t =
  Tag.into_cstruct ~xor_tag_with cs @@ fst t;
  Cstruct.blit (snd t) 0 cs Tag.size (Cstruct.length @@ snd t)

let links (tag, data) =
  if Tag.is_file_struct tag then begin
    match (snd tag.Tag.type3) with
    | 0x00 -> begin
      match Dir.dirstruct_of_cstruct data with
      | None -> None 
      | Some s -> Some (Metadata s)
    end
    | 0x02 -> begin
        match File.ctz_of_cstruct data with
        | None -> None
        | Some s -> Some (Data s)
      end
    | _ -> None
  end else if Tag.is_hardtail tag then begin
    match Dir.hard_tail_links (tag, data) with
    | None -> None
    | Some (next_metadata) -> Some (Metadata next_metadata)
  end else None

let pp fmt (tag, data) =
  match links (tag, data) with
  | None ->
    Fmt.pf fmt "@[entry: @[tag: %a@]@ @[contents:@ %a@]@]" Tag.pp tag Cstruct.hexdump_pp data
  | Some link ->
    Fmt.pf fmt "@[entry: @[tag: %a@]@ @[contents:@ @[(parsed as %a)@]@ %a@]@]"
      Tag.pp tag
      pp_link link
      Cstruct.hexdump_pp data

let compact entries =
  let remove_entries_matching id l =
    List.filter_map (fun e ->
        if 0 = (Int.compare Tag.((fst e).id) id) then None
        else Some e
      ) l
  in
  List.fold_left (fun new_list e ->
      match Tag.((fst e).type3) with
      | Tag.LFS_TYPE_SPLICE, 0xff -> remove_entries_matching Tag.((fst e).id) new_list
      | _ -> e :: new_list
    ) [] entries |> List.rev

let lenv_with_hardtail l =
  List.fold_left (fun sum t ->
      sum + sizeof t
      ) 0 l

let lenv_less_hardtail l =
  List.fold_left (fun sum t ->
      if (not @@ Tag.is_hardtail @@ fst t) then
      sum + sizeof t
      else sum) 0 l

let into_cstructv ~starting_xor_tag cs l =
  (* currently this takes a `t list`, and therefore is pretty straightforward.
   * This function exists so we can do better once `t list` is replaced with more complicated *)
  List.fold_left (fun (pointer, prev_tag) t ->
      into_cstruct ~xor_tag_with:prev_tag (Cstruct.shift cs pointer) t;
      let tag = Tag.to_cstruct_raw (fst t) in
      (pointer + (sizeof t), tag)
    ) (0, starting_xor_tag) l

let to_cstructv ~starting_xor_tag l =
  (* TODO: this is also not quite right; in cases where we filter out a
   * hardtail, we'll have a gap at the end of the cstruct *)
  let cs = Cstruct.create @@ lenv_with_hardtail l in
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
