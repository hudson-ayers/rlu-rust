# RLU in Rust

### Overview

This is a userspace implementation of the Read-Log-Update concurrency mechanism ([SOSP '15](https://dl.acm.org/citation.cfm?id=2815406)), written in Rust. This implementation
is for the most part a direct port of the C code to Rust, with a few exceptions, and as a result uses lots
of unsafe and raw pointer manipulations, and is not particularly "rusty".

### Comparison to Original Implementation

One advantage of this implementation over the original [C implementation](https://github.com/rlu-sync/rlu) is that it does not rely on any global state, so multiple RLU instances can exist simultaneously in a single process.

One disadvantage of this implementation vs. the original is that any data type wishing to use it must be placed
in a wrapper struct that contains the RLU header and implements the `RluObj` trait. This requires some small overhead each time a new data type is used with the mechanism. The advantage of this approach is that it removes
the need for custom allocation and freeing of headers placed in memory ahead of whatever data type is being used.

When evaluated using a linked-list based set, with workloads ranging from 2% writes to 40% writes and up to 8 simultaneous read/write threads, this implementation acheives on average 90% of the throughput of the original C implementation, and significantly outperforms lock based use of sets in the Rust standard library such as BTreeSet.

### Other information

This project was implemented as a final project for Stanford's Programming Languages course (CS 242), taught by Will Crichton, and the benchmarking code and set testing code were written by him.

Disclaimer: It is very possible there exist race conditions or memory safety bugs in this code which my tests do not catch. As a result, it is definitely possible that functions not marked unsafe could actually lead to
memory errors. This code should not be used in production under any assumption that it upholds all of Rust's
guarantees with respect to memory safety / correctness.
