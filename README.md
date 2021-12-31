# what is this?

An implementation of the [mirage-kv](https://github.com/mirage/mirage-kv) module type offering persistence via [mirage-block](https://github.com/mirage/mirage-block), suitable for use in MirageOS unikernels.  It is inspired by [littlefs](https://github.com/littlefs-project/littlefs).

# what isn't this?

Performant. Wear-alert. Making big promises. Well-tested. Backed by Big Camel or suitable for Big Data.

# why though?

Sometimes you just gotta store some stuff. You don't have to store much stuff and you don't have to do it very often but people are gonna get real mad if you don't do it at least a little.

# can I use it?

Sure, if you want. `littlefs` is released under the ISC license (like many MirageOS libraries and its core tooling). Prospective users are encouraged to remember that THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE.

# why would I use it over other MirageOS filesystem implementations?

Good question. I'm using it because I didn't want to end up using an unmaintained filesystem implementation or maintaining someone else's filesystem implementation. Obviously that's not going to apply to you (or if it does, you're unlikely to decide this software is the right choice).

# how does it differ from littlefs?

`littlefs` threads a linked list through its directory tree so the tree can be traversed in constant RAM. This necessitates some extra complexity to ensure that the tree and linked list agree. This implementation does not guarantee constant RAM operations, therefore does not use the threaded linked list (`softlink`s in the parlance of the `littlefs` spec), therefore also does not implement global move state.

`littlefs` implements wear leveling strategies aimed at increasing the life of flash-based storage. This implementation does not, because we assume the execution environment does not provide direct access to real storage; we trust the hypervisor's disk driver to handle this if it's appropriate and required.

This implementation also does not detect or track bad blocks, for similar reasons. (This decision would be fairly easy to reverse.)

The [fuse driver for littlefs](https://github.com/geky/littlefs-fuse) is interoperable with this implementation, to the best of my knowledge.

# how do I thank you for writing and maintaining this software?

[Directly with money](https://ko-fi.com/yomimono), by [donating to a 501c3](https://www.freedom-inc.org/index.php?page=Support-Us) that does important work in my community, or by writing a nice [e-mail](mailto:maintenance at identity-function dot com).
