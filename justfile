image := "_build/default/src/test.img"
bigmage := "/tmp/bigfile.img"
HOME := env_var("HOME")

block_size := "4096"

big_img:
	dd if=/dev/zero of={{bigmage}} bs=1M count=64
	mklittlefs -c . -b 4096 -s $((1024 * 1024 * 64)) {{bigmage}}

test_img:
	dune build @default
	dd if=/dev/zero of={{image}} bs=64K count=1
	_build/default/src/format.exe {{image}}

debug_format:
	dune build @default
	dd if=/dev/zero of={{image}} bs=64K count=1
	gdb --args _build/default/src/format.exe {{image}}

read:
	dune build @default
	_build/default/src/lfs_read.exe {{bigmage}} 4096 lib/block.ml

readmdir BLOCK:
	readmdir.py -a --log {{image}} {{block_size}} {{BLOCK}}

readtree:
	readtree.py -a --log {{image}} {{block_size}} 0 1

mklittlefs-read:
	mklittlefs -d 5 -b {{block_size}} -l {{image}}

umount:
	sudo umount -q /mnt || true
	sudo losetup -d /dev/loop0 || true

mount: umount
	sudo losetup /dev/loop0 {{image}}
	sudo chmod a+rw /dev/loop0
	sudo {{HOME}}/fuse-littlefs/lfs --block_size={{block_size}} -d /dev/loop0 /mnt &
	# nb: `ls /mnt` will fail if there are no files at all in the filesystem.

fuse-format:
	dd if=/dev/zero of={{image}} bs=64K count=1
	sudo umount -q /mnt || true
	sudo losetup -d /dev/loop0 || true
	sudo losetup /dev/loop0 {{image}}
	sudo {{HOME}} fuse-littlefs/lfs --block_size={{block_size}} --format /dev/loop0

hexdump:
	xxd {{image}} | less
