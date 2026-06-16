# sd-event-futures

Rust futures on top of systemd's [`sd-event`] loop, with no dependencies
beyond `libc` and `libsystemd`. It shows that the asynchronous patterns
systemd writes as callbacks and state machines become straight-line
`async fn`s in Rust, on the same event loop and without a thread in
sight unless a call really blocks.

[`sd-event`]: https://man.archlinux.org/man/sd-event.3.en

## Layout

| Where | What |
|---|---|
| [`src/`](src/lib.rs) | The library: executor, reactor, combinators, child processes, the service manager's protocols. |
| [`examples/`](examples) | Runnable programs: systemd components ported to the library, and two complete daemons. |
| [`tests/`](tests) | The library's tests, and `example_<name>.rs` for each example. |
| [`benches/executor.rs`](benches/executor.rs) | Executor micro-benchmarks. |

## Examples

Each example names the systemd C code it ports, with links pinned to one
systemd commit, and ends its description with a verdict on how the two
compare.

| Example | Ports | Run it to |
|---|---|---|
| [`greeter`](examples/greeter.rs) | a service done right | serve TCP with socket activation, `Type=notify-reload`, the watchdog, memory pressure, the fd store and a graceful stop |
| [`systemd-socket-proxyd`](examples/systemd-socket-proxyd/main.rs) | `src/socket-proxy/` | proxy connections; a drop-in replacement, tested against the original |
| [`resolve`](examples/resolve.rs) | `sd-resolve` | look up many names at once without blocking the loop |
| [`timesync`](examples/timesync/main.rs) | `systemd-timesyncd` | find an NTP server that answers and measure your clock's offset |
| [`wireguard_endpoints`](examples/wireguard_endpoints.rs) | `systemd-networkd`'s WireGuard endpoints | resolve peers, retrying with exponential backoff |
| [`journal_offline`](examples/journal_offline.rs) | `systemd-journald`'s offlining | `fsync()` a journal file while logging carries on |
| [`modules_load`](examples/modules_load.rs) | `systemd-modules-load` | load this system's `modules-load.d` modules in parallel |
| [`pid1_offload`](examples/pid1_offload.rs) | PID 1's `async.c`, shutdown's sync | remove, close and sync in forked helpers while the loop keeps ticking |
| [`machine_operation`](examples/machine_operation.rs) | `systemd-machined`'s `Operation` | fork long-running work off a request, and cancel it |

```sh
cargo run --example timesync
cargo run --example pid1_offload
systemd-socket-activate -l 7777 --fdname=greeting target/debug/examples/greeter
```

## Waker or LocalWaker

`sd-event` loops are single-threaded, so the executor could use the
nightly [`LocalWaker`] (`--features local-waker`, built from
[`src/executor_local.rs`](src/executor_local.rs)) instead of the
thread-safe `Waker` ([`src/executor.rs`](src/executor.rs)). The price
shows in `spawn_blocking`: with a `Waker` the pool thread just wakes the
task, while a `LocalWaker` can't leave the loop's thread, so every
blocking call needs an eventfd of its own for the thread to signal. The
gain is a few nanoseconds per wake (`cargo bench`, both ways):

| | `Waker` | `LocalWaker` |
|---|---:|---:|
| waker clone + drop | 9.6 ns | 3.6 ns |
| spawn + join | 305 ns | 290 ns |
| wake between tasks | 36 ns | 29 ns |
| socket round trip | 5.07 µs | 5.01 µs |

Next to a system call the difference disappears, so the default is
`Waker`.

[`LocalWaker`]: https://doc.rust-lang.org/nightly/std/task/struct.LocalWaker.html

## Building

Needs `libsystemd` and, for `--features local-waker`, a nightly
toolchain; `nix develop` provides both. `cargo test` runs everything;
`tests/example_systemd_socket_proxyd.rs` also compares against
`systemd-socket-proxyd` from `$SYSTEMD_SOCKET_PROXYD` or the running
system.

## License

The library is licensed under either of [Apache License, Version
2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT), at your option.

The programs in `examples/` port systemd code and are licensed, like
systemd, under the [GNU Lesser General Public License, version 2.1 or
later](LICENSE-LGPL-2.1), as their `SPDX-License-Identifier` headers say.
