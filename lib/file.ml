let sizeof_pointer = 4

let inline_struct_chunk = 0x01
let file_chunk = 0x01
let ctz_chunk = 0x02

let name n id = Tag.({
    valid = true;
    type3 = (Tag.LFS_TYPE_NAME, file_chunk);
    id;
    length = String.length n;
  }, Cstruct.of_string n)

let create_inline id contents = Tag.({
    valid = true;
    type3 = (Tag.LFS_TYPE_STRUCT, inline_struct_chunk);
    id;
    length = (Cstruct.length contents);
  })

let create_ctz id ~pointer ~file_size =
  let cs = Cstruct.create (4 * 2) in
  Cstruct.LE.set_uint32 cs 0 pointer;
  Cstruct.LE.set_uint32 cs 4 file_size;
  Tag.({
    valid = true;
    type3 = (Tag.LFS_TYPE_STRUCT, ctz_chunk);
    id;
    length = (Cstruct.length cs);
  }, cs)

let ctz_of_cstruct cs =
  if Cstruct.length cs < 8 then None
  else Some Cstruct.LE.(get_uint32 cs 0, get_uint32 cs 4)

let write_inline n id contents =
  [name n id; (create_inline id contents), contents; ]

let n_pointers = function
  | 0 -> 0
  | 1 -> 1
  | index -> (Bitwise.ctz (Int32.of_int index) |> Int32.to_int) + 1

let of_block index cs =
  let sizeof_pointer = 4 in
  let pointer_count = n_pointers index in
  let pointers = List.init pointer_count (fun n ->
      Cstruct.LE.get_uint32 cs (sizeof_pointer * n)
    ) in
  let sizeof_data = (Cstruct.length cs) - (sizeof_pointer * pointer_count) in
  (pointers, Cstruct.sub cs (pointer_count * sizeof_pointer) sizeof_data)

let last_block_index ~file_size ~block_size =
  let popcount_arg = (file_size / (block_size - (2 * sizeof_pointer))) - 1 |> Int32.of_int in
  let numerator = file_size - (sizeof_pointer * (Bitwise.popcount popcount_arg |> Int32.to_int)) + 2 in
  numerator / (block_size - 2 * sizeof_pointer)
