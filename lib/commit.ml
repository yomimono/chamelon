type t = {
  entries : Entry.t list;
  crc : Optint.t;
  padding : int; (* make the commit aligned with the program block size *)
}

let sizeof t =
  (Entry.lenv t.entries) + 4 + t.padding

let into_cstruct cs t =
  let _ = Entry.into_cstructv t.entries cs in
  let crc_pointer = Entry.lenv t.entries in
  Cstruct.LE.set_uint32 (Cstruct.shift cs crc_pointer) 0 (Optint.to_int32 t.crc);
  Cstruct.memset (Cstruct.sub cs (crc_pointer + 4) t.padding) 0
