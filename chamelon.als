sig Metadata_block {
  revision_count : one Int,
  var commits : disj set Commit
}

fact {
  all block : Metadata_block | block.revision_count > 0
}

// a block pair is two metadata blocks such that
// one block has a higher revision count than the other
sig Block_pair {
  fst : disj one Metadata_block,
  snd : disj one Metadata_block
}

fact distinguishable_pair {
  all pair : Block_pair |
              (pair.fst.revision_count > pair.snd.revision_count ||
               pair.snd.revision_count > pair.fst.revision_count)
}

one sig Filesystem {
  root : disj one Block_pair
}

fact {
  always one Filesystem
}

sig Commit {
  entries : disj set Entry,
  crc : disj one Crc
  //while we don't model correctness of the CRC,
  //ignoring it entirely would lead us to a one-level
  //representation here (i.e., blocks contain a set of entries),
  //which might lead us to an incorrect conclusion about
  //the real implementation.
  //so let's retain the "has a crc" property of commits
}

// in this analysis, we gain nothing by saying that a Crc is an entry
// since it's not in any way interchangeable with the other entries
// and nothing it shares in common (tag structure) is modeled
sig Crc {}

abstract sig Entry, Struct {}

sig Directory extends Struct {
  block_pair : disj one Block_pair
}

sig Inline_file, Ctz_file extends Struct {}

sig Name { }

sig Name_mapping extends Entry {
  name : one Name,
  id : one Int
}

sig Struct_mapping extends Entry {
  id : one Int,
  struct : one Struct
}

sig Delete extends Entry {
  id : one Int
}

pred read {
  Filesystem' = Filesystem
}

fact all_block_in_bp {
  always all block : Metadata_block, bp : Block_pair | bp.fst = block || bp.snd = block
}

fact all_commits_in_block {
  always all commit : Commit, block : Metadata_block | commit in block.commits
}

fact all_entries_in_commits {
  always all entry : Entry, commit : Commit | entry in commit.entries
}

run example {}
