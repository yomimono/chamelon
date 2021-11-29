# what is this?

An implementation of [LittleFS](https://github.com/littlefs-project/littlefs) for the MirageOS [BLOCK](https://github.com/mirage/mirage-block) API, and exposing the MirageOS [KV](https://github.com/mirage/mirage-kv) interface.

# what isn't this?

Performant. Wear-alert. Making big promises. Well-tested. Backed by Big Camel or suitable for Big Data.

# why though?

Sometimes you just gotta store some stuff. You don't have to store much stuff and you don't have to do it very often but people are gonna get real mad if you don't do it at least a little.

# can I use it?

Sure, if you want. `littlefs` is released under the ISC license (like many MirageOS libraries and its core tooling). Prospective users are encouraged to remember that THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE.

# why would I use it over other solutions?

Good question. I'm using it because I didn't want to end up using an unmaintained filesystem implementation or maintaining someone else's filesystem implementation. Obviously that's not going to apply to you (or if it does, you're unlikely to decide `littlefs` is the right choice).
