debug:
	dune build @default
	dd if=/dev/zero of=_build/default/src/test.img bs=64K count=1
	gdb --args _build/default/src/format.exe _build/default/src/test.img

mount:
	dd if=/dev/zero of=_build/default/src/test.img bs=64K count=1
	_build/default/src/format.exe _build/default/src/test.img
	sudo umount -q /mnt || true
	sudo losetup -d /dev/loop0 || true
	sudo losetup /dev/loop0 _build/default/src/test.img
	/home/yomimono/fuse-littlefs/lfs /dev/loop0 /mnt
	ls /mnt
