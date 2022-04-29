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
