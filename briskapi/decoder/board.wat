;; The board totals of a BRiSK decoder's price ladder, summed in its own memory (see decoder.cjs, BoardReader).
;; Assembled with WABT's wat2wasm; decoder.cjs embeds the bytes (BOARD_KERNEL), and a test checks they match.
;;
;; The ladder is a std::deque of 72-byte levels, 56 to a block: `map` is the deque's block map, `start` its
;; first slot, `size` its length. A level holds the quantities (f64) of its bids and closing-condition bids at
;; +8 and +16, their order counts (u32) at +24 and +28, and the same for the asks at +40, +48, +56 and +60.
;; sum() adds each up over every level, in order, and stores them at `out`: the bid quantities (f64x2), the ask
;; quantities (f64x2), the bid order counts (i64x2), the ask order counts (i64x2); then (i32) the highest slot
;; with a bid quantity, and with a closing-condition one, and the lowest with an ask quantity, and with a
;; closing-condition one (-1: none), where a book's best levels begin. A quantity counts if it is not zero (or NaN).
;;
;; compact() writes the levels that hold anything (a quantity of any of the four, not zero or NaN), lowest slot first,
;; as arrays at `out` for `cap` levels: their tick index (i32, `base` plus the slot), then from out + 4 cap their four
;; quantities (f64: bid, ask, closing-condition bid, closing-condition ask), then from out + 36 cap their four order
;; counts (u32, the same order). `cap` is even and at least `size`, `out` 8-aligned. It also stores at `sums` what
;; sum() would, from the same pass. It returns how many levels it wrote: a book read whole in one pass, for decoder.cjs
;; to compare with the last one read (book changes).
;;
;; diff() compares two such ladders, `cur` (n levels, room for `ccap`) and `prev` (pn, room for `pcap`), and writes at
;; `out` each of the four books' levels that differ, 24 bytes each, by tick index then book: the index (i32), the book
;; (i32: 0 bid, 1 ask, 2 and 3 the closing-condition bid and ask), the quantity now (f64; 0 where the level emptied) and
;; the order count now (u32). It returns how many; at most 4 (n + pn).
(module
  (import "env" "memory" (memory 1))
  (func (export "sum") (param $map i32) (param $start i32) (param $size i32) (param $out i32)
    (local $i i32) (local $k i32) (local $run i32) (local $e i32) (local $end i32) (local $slot i32) (local $q f64)
    (local $bids v128) (local $asks v128) (local $bid_orders v128) (local $ask_orders v128)
    (local $top_bid i32) (local $top_closing_bid i32) (local $low_ask i32) (local $low_closing_ask i32)
    (local.set $top_bid (i32.const -1)) (local.set $top_closing_bid (i32.const -1))
    (local.set $low_ask (i32.const -1)) (local.set $low_closing_ask (i32.const -1))
    (block $done
      (loop $blocks
        (br_if $done (i32.ge_u (local.get $i) (local.get $size)))
        (local.set $k (i32.add (local.get $start) (local.get $i)))
        ;; The levels left in this block, or in the deque.
        (local.set $run (i32.sub (i32.const 56) (i32.rem_u (local.get $k) (i32.const 56))))
        (if (i32.gt_u (local.get $run) (i32.sub (local.get $size) (local.get $i)))
          (then (local.set $run (i32.sub (local.get $size) (local.get $i)))))
        (local.set $e
          (i32.add
            (i32.load (i32.add (local.get $map) (i32.shl (i32.div_u (local.get $k) (i32.const 56)) (i32.const 2))))
            (i32.mul (i32.rem_u (local.get $k) (i32.const 56)) (i32.const 72))))
        (local.set $end (i32.add (local.get $e) (i32.mul (local.get $run) (i32.const 72))))
        (local.set $slot (local.get $i))
        (loop $levels
          (local.set $bids (f64x2.add (local.get $bids) (v128.load offset=8 align=8 (local.get $e))))
          (local.set $asks (f64x2.add (local.get $asks) (v128.load offset=40 align=8 (local.get $e))))
          (local.set $bid_orders (i64x2.add (local.get $bid_orders)
            (i64x2.extend_low_i32x4_u (v128.load64_zero offset=24 align=4 (local.get $e)))))
          (local.set $ask_orders (i64x2.add (local.get $ask_orders)
            (i64x2.extend_low_i32x4_u (v128.load64_zero offset=56 align=4 (local.get $e)))))
          (local.set $q (f64.load offset=8 (local.get $e)))
          (local.set $top_bid (select (local.get $slot) (local.get $top_bid)
            (i32.or (f64.gt (local.get $q) (f64.const 0)) (f64.lt (local.get $q) (f64.const 0)))))
          (local.set $q (f64.load offset=16 (local.get $e)))
          (local.set $top_closing_bid (select (local.get $slot) (local.get $top_closing_bid)
            (i32.or (f64.gt (local.get $q) (f64.const 0)) (f64.lt (local.get $q) (f64.const 0)))))
          (local.set $q (f64.load offset=40 (local.get $e)))
          (local.set $low_ask (select (local.get $slot) (local.get $low_ask)
            (i32.and (i32.lt_s (local.get $low_ask) (i32.const 0))
              (i32.or (f64.gt (local.get $q) (f64.const 0)) (f64.lt (local.get $q) (f64.const 0))))))
          (local.set $q (f64.load offset=48 (local.get $e)))
          (local.set $low_closing_ask (select (local.get $slot) (local.get $low_closing_ask)
            (i32.and (i32.lt_s (local.get $low_closing_ask) (i32.const 0))
              (i32.or (f64.gt (local.get $q) (f64.const 0)) (f64.lt (local.get $q) (f64.const 0))))))
          (local.set $slot (i32.add (local.get $slot) (i32.const 1)))
          (local.set $e (i32.add (local.get $e) (i32.const 72)))
          (br_if $levels (i32.lt_u (local.get $e) (local.get $end))))
        (local.set $i (i32.add (local.get $i) (local.get $run)))
        (br $blocks)))
    (v128.store offset=0 align=8 (local.get $out) (local.get $bids))
    (v128.store offset=16 align=8 (local.get $out) (local.get $asks))
    (v128.store offset=32 align=8 (local.get $out) (local.get $bid_orders))
    (v128.store offset=48 align=8 (local.get $out) (local.get $ask_orders))
    (i32.store offset=64 (local.get $out) (local.get $top_bid))
    (i32.store offset=68 (local.get $out) (local.get $top_closing_bid))
    (i32.store offset=72 (local.get $out) (local.get $low_ask))
    (i32.store offset=76 (local.get $out) (local.get $low_closing_ask)))
  (func (export "compact") (param $map i32) (param $start i32) (param $size i32) (param $base i32) (param $out i32) (param $cap i32)
    (param $sums i32) (result i32)
    (local $i i32) (local $k i32) (local $run i32) (local $e i32) (local $end i32) (local $slot i32) (local $n i32)
    (local $qs i32) (local $cs i32) (local $q i32) (local $c i32)
    (local $bid f64) (local $ask f64) (local $closing_bid f64) (local $closing_ask f64)
    (local $bids v128) (local $asks v128) (local $bid_orders v128) (local $ask_orders v128)
    (local $top_bid i32) (local $top_closing_bid i32) (local $low_ask i32) (local $low_closing_ask i32)
    (local.set $top_bid (i32.const -1)) (local.set $top_closing_bid (i32.const -1))
    (local.set $low_ask (i32.const -1)) (local.set $low_closing_ask (i32.const -1))
    (local.set $qs (i32.add (local.get $out) (i32.shl (local.get $cap) (i32.const 2))))
    (local.set $cs (i32.add (local.get $qs) (i32.shl (local.get $cap) (i32.const 5))))
    (block $done
      (loop $blocks
        (br_if $done (i32.ge_u (local.get $i) (local.get $size)))
        (local.set $k (i32.add (local.get $start) (local.get $i)))
        (local.set $run (i32.sub (i32.const 56) (i32.rem_u (local.get $k) (i32.const 56))))
        (if (i32.gt_u (local.get $run) (i32.sub (local.get $size) (local.get $i)))
          (then (local.set $run (i32.sub (local.get $size) (local.get $i)))))
        (local.set $e
          (i32.add
            (i32.load (i32.add (local.get $map) (i32.shl (i32.div_u (local.get $k) (i32.const 56)) (i32.const 2))))
            (i32.mul (i32.rem_u (local.get $k) (i32.const 56)) (i32.const 72))))
        (local.set $end (i32.add (local.get $e) (i32.mul (local.get $run) (i32.const 72))))
        (local.set $slot (local.get $i))
        (loop $levels
          (local.set $bids (f64x2.add (local.get $bids) (v128.load offset=8 align=8 (local.get $e))))
          (local.set $asks (f64x2.add (local.get $asks) (v128.load offset=40 align=8 (local.get $e))))
          (local.set $bid_orders (i64x2.add (local.get $bid_orders)
            (i64x2.extend_low_i32x4_u (v128.load64_zero offset=24 align=4 (local.get $e)))))
          (local.set $ask_orders (i64x2.add (local.get $ask_orders)
            (i64x2.extend_low_i32x4_u (v128.load64_zero offset=56 align=4 (local.get $e)))))
          (local.set $bid (f64.load offset=8 (local.get $e)))
          (local.set $closing_bid (f64.load offset=16 (local.get $e)))
          (local.set $ask (f64.load offset=40 (local.get $e)))
          (local.set $closing_ask (f64.load offset=48 (local.get $e)))
          (local.set $top_bid (select (local.get $slot) (local.get $top_bid)
            (i32.or (f64.gt (local.get $bid) (f64.const 0)) (f64.lt (local.get $bid) (f64.const 0)))))
          (local.set $top_closing_bid (select (local.get $slot) (local.get $top_closing_bid)
            (i32.or (f64.gt (local.get $closing_bid) (f64.const 0)) (f64.lt (local.get $closing_bid) (f64.const 0)))))
          (local.set $low_ask (select (local.get $slot) (local.get $low_ask)
            (i32.and (i32.lt_s (local.get $low_ask) (i32.const 0))
              (i32.or (f64.gt (local.get $ask) (f64.const 0)) (f64.lt (local.get $ask) (f64.const 0))))))
          (local.set $low_closing_ask (select (local.get $slot) (local.get $low_closing_ask)
            (i32.and (i32.lt_s (local.get $low_closing_ask) (i32.const 0))
              (i32.or (f64.gt (local.get $closing_ask) (f64.const 0)) (f64.lt (local.get $closing_ask) (f64.const 0))))))
          (if (i32.or
                (i32.or (i32.or (f64.gt (local.get $bid) (f64.const 0)) (f64.lt (local.get $bid) (f64.const 0)))
                        (i32.or (f64.gt (local.get $ask) (f64.const 0)) (f64.lt (local.get $ask) (f64.const 0))))
                (i32.or (i32.or (f64.gt (local.get $closing_bid) (f64.const 0)) (f64.lt (local.get $closing_bid) (f64.const 0)))
                        (i32.or (f64.gt (local.get $closing_ask) (f64.const 0)) (f64.lt (local.get $closing_ask) (f64.const 0)))))
            (then
              (i32.store (i32.add (local.get $out) (i32.shl (local.get $n) (i32.const 2)))
                (i32.add (local.get $base) (local.get $slot)))
              (local.set $q (i32.add (local.get $qs) (i32.shl (local.get $n) (i32.const 5))))
              (f64.store offset=0 (local.get $q) (local.get $bid))
              (f64.store offset=8 (local.get $q) (local.get $ask))
              (f64.store offset=16 (local.get $q) (local.get $closing_bid))
              (f64.store offset=24 (local.get $q) (local.get $closing_ask))
              (local.set $c (i32.add (local.get $cs) (i32.shl (local.get $n) (i32.const 4))))
              (i32.store offset=0 (local.get $c) (i32.load offset=24 (local.get $e)))
              (i32.store offset=4 (local.get $c) (i32.load offset=56 (local.get $e)))
              (i32.store offset=8 (local.get $c) (i32.load offset=28 (local.get $e)))
              (i32.store offset=12 (local.get $c) (i32.load offset=60 (local.get $e)))
              (local.set $n (i32.add (local.get $n) (i32.const 1)))))
          (local.set $slot (i32.add (local.get $slot) (i32.const 1)))
          (local.set $e (i32.add (local.get $e) (i32.const 72)))
          (br_if $levels (i32.lt_u (local.get $e) (local.get $end))))
        (local.set $i (i32.add (local.get $i) (local.get $run)))
        (br $blocks)))
    (v128.store offset=0 align=8 (local.get $sums) (local.get $bids))
    (v128.store offset=16 align=8 (local.get $sums) (local.get $asks))
    (v128.store offset=32 align=8 (local.get $sums) (local.get $bid_orders))
    (v128.store offset=48 align=8 (local.get $sums) (local.get $ask_orders))
    (i32.store offset=64 (local.get $sums) (local.get $top_bid))
    (i32.store offset=68 (local.get $sums) (local.get $top_closing_bid))
    (i32.store offset=72 (local.get $sums) (local.get $low_ask))
    (i32.store offset=76 (local.get $sums) (local.get $low_closing_ask))
    (local.get $n))
  (func $emit (param $out i32) (param $m i32) (param $index i32) (param $book i32) (param $q f64) (param $c i32) (result i32)
    (local $r i32)
    (local.set $r (i32.add (local.get $out) (i32.mul (local.get $m) (i32.const 24))))
    (i32.store offset=0 (local.get $r) (local.get $index))
    (i32.store offset=4 (local.get $r) (local.get $book))
    (f64.store offset=8 (local.get $r) (local.get $q))
    (i32.store offset=16 (local.get $r) (local.get $c))
    (i32.add (local.get $m) (i32.const 1)))
  (func (export "diff") (param $cur i32) (param $ccap i32) (param $n i32) (param $prev i32) (param $pcap i32) (param $pn i32)
    (param $out i32) (result i32)
    (local $i i32) (local $j i32) (local $m i32) (local $a i32) (local $b i32) (local $k i32)
    (local $cq i32) (local $cc i32) (local $pq i32) (local $pc i32) (local $x i32) (local $y i32) (local $xc i32) (local $yc i32)
    (local $same v128)
    (local.set $cq (i32.add (local.get $cur) (i32.shl (local.get $ccap) (i32.const 2))))
    (local.set $cc (i32.add (local.get $cq) (i32.shl (local.get $ccap) (i32.const 5))))
    (local.set $pq (i32.add (local.get $prev) (i32.shl (local.get $pcap) (i32.const 2))))
    (local.set $pc (i32.add (local.get $pq) (i32.shl (local.get $pcap) (i32.const 5))))
    (block $done
      (loop $merge
        (br_if $done (i32.and (i32.ge_u (local.get $i) (local.get $n)) (i32.ge_u (local.get $j) (local.get $pn))))
        (local.set $a (select (i32.load (i32.add (local.get $cur) (i32.shl (local.get $i) (i32.const 2)))) (i32.const 0x7fffffff)
          (i32.lt_u (local.get $i) (local.get $n))))
        (local.set $b (select (i32.load (i32.add (local.get $prev) (i32.shl (local.get $j) (i32.const 2)))) (i32.const 0x7fffffff)
          (i32.lt_u (local.get $j) (local.get $pn))))
        (local.set $x (i32.add (local.get $cq) (i32.shl (local.get $i) (i32.const 5))))
        (local.set $xc (i32.add (local.get $cc) (i32.shl (local.get $i) (i32.const 4))))
        (local.set $y (i32.add (local.get $pq) (i32.shl (local.get $j) (i32.const 5))))
        (local.set $yc (i32.add (local.get $pc) (i32.shl (local.get $j) (i32.const 4))))
        (if (i32.eq (local.get $a) (local.get $b))
          (then
            ;; The same level: its four quantities and four counts at once (bit for bit), then each book whose quantity
            ;; differs as a number, as decoder.cjs compares them, or whose count differs.
            (local.set $same (v128.and
              (v128.and (i64x2.eq (v128.load offset=0 align=8 (local.get $x)) (v128.load offset=0 align=8 (local.get $y)))
                        (i64x2.eq (v128.load offset=16 align=8 (local.get $x)) (v128.load offset=16 align=8 (local.get $y))))
              (i64x2.eq (v128.load align=4 (local.get $xc)) (v128.load align=4 (local.get $yc)))))
            (if (i32.eqz (i64x2.all_true (local.get $same)))
              (then
                (local.set $k (i32.const 0))
                (loop $books
                  (if (i32.or
                        (f64.ne (f64.load (i32.add (local.get $x) (i32.shl (local.get $k) (i32.const 3))))
                                (f64.load (i32.add (local.get $y) (i32.shl (local.get $k) (i32.const 3)))))
                        (i32.ne (i32.load (i32.add (local.get $xc) (i32.shl (local.get $k) (i32.const 2))))
                                (i32.load (i32.add (local.get $yc) (i32.shl (local.get $k) (i32.const 2))))))
                    (then (local.set $m (call $emit (local.get $out) (local.get $m) (local.get $a) (local.get $k)
                      (f64.load (i32.add (local.get $x) (i32.shl (local.get $k) (i32.const 3))))
                      (i32.load (i32.add (local.get $xc) (i32.shl (local.get $k) (i32.const 2))))))))
                  (local.set $k (i32.add (local.get $k) (i32.const 1)))
                  (br_if $books (i32.lt_u (local.get $k) (i32.const 4))))))
            (local.set $i (i32.add (local.get $i) (i32.const 1)))
            (local.set $j (i32.add (local.get $j) (i32.const 1))))
          (else
            (if (i32.lt_s (local.get $a) (local.get $b))
              (then   ;; a level the last read lacked: each book that holds anything there
                (local.set $k (i32.const 0))
                (loop $new
                  (if (f64.ne (f64.load (i32.add (local.get $x) (i32.shl (local.get $k) (i32.const 3)))) (f64.const 0))
                    (then (local.set $m (call $emit (local.get $out) (local.get $m) (local.get $a) (local.get $k)
                      (f64.load (i32.add (local.get $x) (i32.shl (local.get $k) (i32.const 3))))
                      (i32.load (i32.add (local.get $xc) (i32.shl (local.get $k) (i32.const 2))))))))
                  (local.set $k (i32.add (local.get $k) (i32.const 1)))
                  (br_if $new (i32.lt_u (local.get $k) (i32.const 4))))
                (local.set $i (i32.add (local.get $i) (i32.const 1))))
              (else   ;; a level now empty: each book that held anything there
                (local.set $k (i32.const 0))
                (loop $gone
                  (if (f64.ne (f64.load (i32.add (local.get $y) (i32.shl (local.get $k) (i32.const 3)))) (f64.const 0))
                    (then (local.set $m (call $emit (local.get $out) (local.get $m) (local.get $b) (local.get $k)
                      (f64.const 0) (i32.const 0)))))
                  (local.set $k (i32.add (local.get $k) (i32.const 1)))
                  (br_if $gone (i32.lt_u (local.get $k) (i32.const 4))))
                (local.set $j (i32.add (local.get $j) (i32.const 1)))))))
        (br $merge)))
    (local.get $m)))
