type common_options = {
  block_size : int;
  program_block_size : int;
  image : string;
}

let set_options block_size program_block_size image () = {block_size; program_block_size; image}
