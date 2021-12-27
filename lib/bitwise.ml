(* popcount code from Base ( https://github.com/janestreet/base ) popcount implementation.
 * License for that implementation is reproduced below:
 Copyright (c) 2016--2021 Jane Street Group, LLC <opensource@janestreet.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*)

(* To maintain javascript compatibility and enable unboxing, we implement popcount in
   OCaml rather than use C stubs. Implementation adapted from:
   https://en.wikipedia.org/wiki/Hamming_weight#Efficient_implementation *)
let int64_popcount =
  let open Int64 in
  let ( + ) = add in
  let ( - ) = sub in
  let ( * ) = mul in
  let ( lsr ) = shift_right_logical in
  let ( land ) = logand in
  let m1 = 0x5555555555555555L in
  (* 0b01010101... *)
  let m2 = 0x3333333333333333L in
  (* 0b00110011... *)
  let m4 = 0x0f0f0f0f0f0f0f0fL in
  (* 0b00001111... *)
  let h01 = 0x0101010101010101L in
  (* 1 bit set per byte *)
  fun [@inline] x ->
    (* gather the bit count for every pair of bits *)
    let x = x - ((x lsr 1) land m1) in
    (* gather the bit count for every 4 bits *)
    let x = (x land m2) + ((x lsr 2) land m2) in
    (* gather the bit count for every byte *)
    let x = (x + (x lsr 4)) land m4 in
    (* sum the bit counts in the top byte and shift it down *)
    to_int ((x * h01) lsr 56)

let int32_popcount =
  (* On 64-bit systems, this is faster than implementing using [int32] arithmetic. *)
  let mask = 0xffff_ffffL in
  fun [@inline] x -> int64_popcount (Int64.logand (Int64.of_int32 x) mask)

let popcount = int32_popcount
