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

let blocks (block1, block2) =
  let cs = Cstruct.create @@ 32 * 2 in
  Cstruct.LE.set_uint32 cs 0 block1;
  Cstruct.LE.set_uint32 cs 4 block2;
  cs

let dirstruct_of_cstruct cs =
  if Cstruct.length cs < (4 + 4) then None
  else Some (Cstruct.LE.(get_uint32 cs 0 |> Int64.of_int32,
                         get_uint32 cs 4 |> Int64.of_int32))

let of_entry (tag, data) =
  let open Tag in
  match tag.type3 with
  | LFS_TYPE_STRUCT, 0x00 -> dirstruct_of_cstruct data
  | _, _ -> None
