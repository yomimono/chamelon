debug:
	dune build @default
	dd if=/dev/zero of=_build/default/src/test.img bs=64K count=1
	gdb --args _build/default/src/format.exe _build/default/src/test.img
