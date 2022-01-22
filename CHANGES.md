# v0.0.3 (2022-01-22)

* follow hardtails when making new directories

# v0.0.2 (2022-01-18)

* used block detection now correctly finds the last block in a CTZ list
* entries in a directory are no longer sometimes lost after splitting when block size is small
* the allocator now has a separate function for allocating metadata block pairs, so the user doesn't have to retrieve two blocks individually

# v0.0.1 (2022-01-10)

Initial release.
