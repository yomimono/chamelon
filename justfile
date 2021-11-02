image := "_build/default/src/test.img"

test_img:
	dune build @default
	dd if=/dev/zero of={{image}} bs=64K count=1
	_build/default/src/format.exe {{image}}

debug:
	dune build @default
	dd if=/dev/zero of={{image}} bs=64K count=1
	gdb --args _build/default/src/format.exe {{image}}

readmdir: test_img
	readmdir.py -a --log {{image}} 4096 0 1

readtree: test_img
	readtree.py -a --log {{image}} 4096 0 1

mklittlefs-read: test_img
	mklittlefs -d 5 -b 4096 -l {{image}}

mount: test_img
	sudo umount -q /mnt || true
	sudo losetup -d /dev/loop0 || true
	sudo losetup /dev/loop0 {{image}}
	/home/yomimono/fuse-littlefs/lfs -d /dev/loop0 /mnt
	ls /mnt
