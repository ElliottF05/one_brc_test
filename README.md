# one-brc-test

---

My 1brc (one billion row challenge) attempts and tests. View the original challenge repository and details [here](https://github.com/gunnarmorling/1brc).

## Results

*(Note: all of these tests are run on my M2 MacBook Air with 16GB of memory)*

- **Single-threaded**: ~14s on average
- **Multi-threaded**: ~4s on average

In the multi-threaded case, I am IO-bottlenecked now since the reader thread reports ~98% of wall time in pread. The original challenge uses a RAM disk to avoid disk IO, unfortunately that's not feasible on my laptop :(.

## Optimization Techniques

### General
- No heap allocations in hot loops; large buffers and maps are preallocated at startup.
- SIMD delimiter search (u8x16) for '\n' and ';'.
- Manual temperature parsing: scan ASCII and interpret temperature value as an i32 representing tenths of a degree.
- Custom hash map with custom hash fine-tuned to the station names for zero collisions.

### Single-threaded (v14.rs)
- Buffered file reads via BufReader with large capacity; scan lines in-place.
- Carry-over buffer to stitch lines split across chunk boundaries.

### Multi-threaded (v16.rs)
- Preallocated buffer pool (Box<[u8]>) shared across threads.
- Single reader thread fills buffers and truncates to the last newline.
- Multiple worker threads take filled buffers from the shared pool, and update thread-local maps (no locking in the hot path).
- Buffer pools coordinated with Mutex + Condvar.
- Workers return buffers to the pool after processing; reader reuses them.
- Final merge reduces per-thread maps into one aggregated result.