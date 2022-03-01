let dirstruct id = Tag.({
    valid = true;
    type3 = (Tag.LFS_TYPE_STRUCT, 0x00);
    id;
    length = 4 * 2 ;(* content is pointers to the metadata pair, two 32-bit values *)
  })

let soft_tail = Tag.({
    valid = true;
    type3 = Tag.LFS_TYPE_TAIL, 0x00;
    id = 0x3ff;
    length = 4 * 2;
  })

let hard_tail = {soft_tail with type3 = Tag.LFS_TYPE_TAIL, 0x01}

let hard_tail_at (a, b) =
  let data = Cstruct.create 8 in
  Cstruct.LE.set_uint32 data 0 (Int64.to_int32 a);
  Cstruct.LE.set_uint32 data 4 (Int64.to_int32 b);
  (hard_tail, data)

let name n id = Tag.({
    valid = true;
    type3 = (Tag.LFS_TYPE_NAME, 0x02);
    id;
    length = String.length n;
  }, Cstruct.of_string n)

let mkdir ~to_pair id =
  let data = Cstruct.create 8 in
  Cstruct.LE.set_uint32 data 0 (Int64.to_int32 @@ fst to_pair);
  Cstruct.LE.set_uint32 data 4 (Int64.to_int32 @@ snd to_pair);
  (dirstruct id, data)

let dirstruct_of_cstruct cs =
  if Cstruct.length cs < (4 + 4) then None
  else Some (Cstruct.LE.(get_uint32 cs 0 |> Int64.of_int32,
                         get_uint32 cs 4 |> Int64.of_int32))


let hard_tail_links (tag, data) =
  match tag.Tag.type3 with
  | Tag.LFS_TYPE_TAIL, 0x01 ->
    (* both hard_tail and dirstruct have a two-pointer metadata structure *)
    dirstruct_of_cstruct data
  | _ -> None

let of_entry (tag, data) =
  let open Tag in
  match tag.type3 with
  | LFS_TYPE_STRUCT, 0x00 -> dirstruct_of_cstruct data
  | _, _ -> None
