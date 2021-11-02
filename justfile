image := "_build/default/src/test.img"

block_size := "4096"

test_img:
	dune build @default
	dd if=/dev/zero of={{image}} bs=64K count=1
	_build/default/src/format.exe {{image}}

debug_format:
	dune build @default
	dd if=/dev/zero of={{image}} bs=64K count=1
	gdb --args _build/default/src/format.exe {{image}}

readmdir:
	readmdir.py -a --log {{image}} {{block_size}} 0 1

readtree:
	readtree.py -a --log {{image}} {{block_size}} 0 1

mklittlefs-read:
	mklittlefs -d 5 -b {{block_size}} -l {{image}}

mount: test_img
	sudo umount -q /mnt || true
	sudo losetup -d /dev/loop0 || true
	sudo losetup /dev/loop0 {{image}}
	sudo chmod a+rw /dev/loop0
	sudo /home/yomimono/fuse-littlefs/lfs --block_size={{block_size}} -d /dev/loop0 /mnt &
	# nb: `ls /mnt` will fail if there are no files at all in the filesystem.

fuse-format:
	dd if=/dev/zero of={{image}} bs=64K count=1
	sudo umount -q /mnt || true
	sudo losetup -d /dev/loop0 || true
	sudo losetup /dev/loop0 {{image}}
	/home/yomimono/fuse-littlefs/lfs --block_size={{block_size}} --format /dev/loop0

hexdump:
	xxd {{image}} | less
