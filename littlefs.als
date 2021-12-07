
// a block pair is two metadata blocks such that
// one block has a higher revision count than the other
sig Block_pair {
  fst : Metadata_block,
  snd : Metadata_block
}

sig Metadata_block {
  revision_count : one Int,
  commits : set Commit
}

fact {
  all pair : Block_pair |
              (pair.fst.revision_count > pair.snd.revision_count ||
               pair.snd.revision_count > pair.fst.revision_count)
}

sig Commit {
  entries : set Entry,
  crc : one Entry
  //while we don't model correctness of the CRC,
  //ignoring it entirely would lead us to a one-level
  //representation here (i.e., blocks contain a set of entries),
  //which might lead us to an incorrect conclusion about
  //the real implementation.
  //so let's retain the "has a crc" property of commits
}

abstract sig Entry { }

abstract sig Struct {}

sig Directory extends Struct {
  block_pair : Block_pair
}

sig Inline_file extends Struct {
}

sig Ctz_file {
  block : Int
}

sig Name_mapping extends Entry {
  name : one String,
  id : one Int
}

sig Struct_mapping extends Entry {
  id : one Int,
  struct : one Struct
}

fact init {
  one Block_pair
}
