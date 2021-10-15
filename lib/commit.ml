type t = {
  entries : Entry.t list;
  crc : Optint.t;
  padding : int; (* make the commit 32-bit word-aligned *)
}

let sizeof t =
  (Entry.lenv t.entries) + 4 + t.padding

let into_cstruct cs t =
  let (crc_pointer, _last_tag) = List.fold_left (fun (pointer, prev_tag) entry ->
      Entry.into_cstruct ~prev_tag (Cstruct.shift cs pointer) entry;
      (pointer + Entry.sizeof entry, Cstruct.sub (Cstruct.shift cs pointer) 0 4)
    ) (0, Cstruct.of_string "\xff\xff\xff\xff") t.entries in
  Cstruct.LE.set_uint32 (Cstruct.shift cs crc_pointer) 0 (Optint.to_int32 t.crc);
  Cstruct.memset (Cstruct.sub cs (crc_pointer + 4) t.padding) 0
