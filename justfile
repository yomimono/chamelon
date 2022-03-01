image := "_build/default/src/test.img"
HOME := env_var("HOME")

block_size := "4096"
program_block_size := "16"

test_img:
	dune build @default
	dd if=/dev/zero of={{image}} bs=64K count=1
	_build/default/src/chamelon.exe format {{image}}

read: test_img mount
	sudo mkdir /mnt/lib
	sudo cp lib/block.ml /mnt/lib
	_build/default/src/chamelon.exe read {{image}} {{block_size}} lib/block.ml

hardtail: test_img mount
	#!/bin/bash
	for i in `seq 1 10`; do
		sudo dd if=/dev/zero of=/mnt/$i bs=500 count=1
	done
	sudo umount /mnt
	_build/default/src/chamelon.exe ls {{image}} {{block_size}} /
	_build/default/src/chamelon.exe read {{image}} {{block_size}} 10
	for i in `seq 1 20`; do
		_build/default/src/chamelon.exe write {{image}} {{block_size}} /moar$i "moar data yey $i"
	done
	echo ""
	_build/default/src/chamelon.exe ls {{image}} {{block_size}} /
	_build/default/src/chamelon.exe read {{image}} {{block_size}} /moar2

test: read hardtail
	dune runtest -f

readmdir BLOCK:
	readmdir.py -a --log {{image}} {{block_size}} {{BLOCK}}

readtree:
	readtree.py -a --log {{image}} {{block_size}} 0 1

umount:
	sudo umount -q /mnt || true
	sudo losetup -d /dev/loop0 || true
	sudo rm -r /mnt/* || true

mount: umount
	sudo losetup /dev/loop0 {{image}}
	sudo chmod a+rw /dev/loop0
	sudo {{HOME}}/fuse-littlefs/lfs --block_size={{block_size}} -s /dev/loop0 /mnt
	# nb: `ls /mnt` will fail if there are no files at all in the filesystem.

fuse-format:
	dd if=/dev/zero of={{image}} bs=64K count=1
	sudo umount -q /mnt || true
	sudo losetup -d /dev/loop0 || true
	sudo losetup /dev/loop0 {{image}}
	sudo {{HOME}}/fuse-littlefs/lfs --block_size={{block_size}} --format /dev/loop0

hexdump:
	xxd {{image}} | less
