# v0.1.0 (2022-07-28, all changes by @yomimono)

* new features: expose and implement `Kv.size t key` and `Kv.get_partial t key ~offset ~length`.
* bugfix: large files could be misread under certain circumstances because the final block index wasn't correctly calculated. remove `bitwise` module and the Base `popcount` it referenced, and instead calculate the block index with a recursive function.
* bugfix: detect unwriteable blocks instead of endlessly splitting to try to accommodate them.
* bugfix: keep track of allocated but not-yet-written blocks, and don't hand them out twice.
* bugfix: check the maximum name length against the block size when mounting a filesystem.

# v0.0.10

* detect simple cycles in the metadata tree at connect time (@yomimono)
* check block size in the superblock at connect time, and fail if it doesn't match block device (@yomimono)


# v0.0.9.1 (2022-06-23)

* fix 32-bit compilation, for real this time (@yomimono)

# v0.0.9 (unreleased)

* be consistent in the use of Logs vs Log module (@palainp)
* implement a lookahead block allocator more similar to the littlefs one (@yomimono)
* bring back fuzz tests and improve them (@yomimono)
* test for correct block index detection in CTZ files, and fix an edge case (@yomimono)

# v0.0.8 (2022-04-29)

* use the sector_size given by the block device via its info (@dinosaure)

# v0.0.7 (2022-03-04)

* initial opam release

# v0.0.6 (2022-03-01)

* unix binaries are now all subcommands of the `chamelon` command
* refactor `split` and add many more tests
* fix a bug where writing a block that is over half full and already has a hardtail would never cause compaction

# v0.0.5 (2022-02-09)

* fix several problems with `remove`
* fix problem where making new directories could get exciting unexpected contents

# v0.0.4 (2022-01-26)

* always start processing a directory at its first blockpair, not its last
* add timestamp output to lfs_ls

# v0.0.3 (2022-01-22)

* follow hardtails when making new directories

# v0.0.2 (2022-01-18)

* used block detection now correctly finds the last block in a CTZ list
* entries in a directory are no longer sometimes lost after splitting when block size is small
* the allocator now has a separate function for allocating metadata block pairs, so the user doesn't have to retrieve two blocks individually

# v0.0.1 (2022-01-10)

Initial release.
