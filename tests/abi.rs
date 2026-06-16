//! Tier 2 — ABI sanity check.
//!
//! Builds a tiny C program against the *real* `<systemd/sd-event.h>` that
//! prints every constant value plus the `sizeof()` of every C type our
//! bindings depend on, runs it, and asserts each value matches our
//! Rust-side declaration.
//!
//! This catches the realistic drift modes:
//!   - A constant changing value in a future libsystemd release.
//!   - `libc::clockid_t` / `libc::pid_t` / `libc::signalfd_siginfo` /
//!     `libc::inotify_event` / `libc::siginfo_t` widths or layouts not
//!     matching what the system's headers actually emit.
//!
//! Full per-function signature checking is left to the behavioral tests
//! in `tests/behavior.rs` — a wrong signature there will surface as a
//! crash or a wrong value.
#![deny(unsafe_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;

use sd_event_futures::sys;

const HEADER_PATH: &str = env!("SD_EVENT_HEADER");

const VERIFIER_C: &str = r#"
#include <stdio.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
#include <signal.h>
#include <sys/signalfd.h>
#include <sys/inotify.h>
#include <systemd/sd-event.h>

int main(void) {
    printf("SD_EVENT_OFF=%lld\n",                 (long long)SD_EVENT_OFF);
    printf("SD_EVENT_ON=%lld\n",                  (long long)SD_EVENT_ON);
    printf("SD_EVENT_ONESHOT=%lld\n",             (long long)SD_EVENT_ONESHOT);
    printf("SD_EVENT_INITIAL=%lld\n",             (long long)SD_EVENT_INITIAL);
    printf("SD_EVENT_ARMED=%lld\n",               (long long)SD_EVENT_ARMED);
    printf("SD_EVENT_PENDING=%lld\n",             (long long)SD_EVENT_PENDING);
    printf("SD_EVENT_RUNNING=%lld\n",             (long long)SD_EVENT_RUNNING);
    printf("SD_EVENT_EXITING=%lld\n",             (long long)SD_EVENT_EXITING);
    printf("SD_EVENT_FINISHED=%lld\n",            (long long)SD_EVENT_FINISHED);
    printf("SD_EVENT_PREPARING=%lld\n",           (long long)SD_EVENT_PREPARING);
    printf("SD_EVENT_PRIORITY_IMPORTANT=%lld\n",  (long long)SD_EVENT_PRIORITY_IMPORTANT);
    printf("SD_EVENT_PRIORITY_NORMAL=%lld\n",     (long long)SD_EVENT_PRIORITY_NORMAL);
    printf("SD_EVENT_PRIORITY_IDLE=%lld\n",       (long long)SD_EVENT_PRIORITY_IDLE);
    printf("SD_EVENT_SIGNAL_PROCMASK=%lld\n",     (long long)SD_EVENT_SIGNAL_PROCMASK);

    printf("sizeof_int=%zu\n",                    sizeof(int));
    printf("sizeof_uint32_t=%zu\n",               sizeof(uint32_t));
    printf("sizeof_uint64_t=%zu\n",               sizeof(uint64_t));
    printf("sizeof_clockid_t=%zu\n",              sizeof(clockid_t));
    printf("sizeof_pid_t=%zu\n",                  sizeof(pid_t));
    printf("sizeof_signalfd_siginfo=%zu\n",       sizeof(struct signalfd_siginfo));
    printf("sizeof_inotify_event=%zu\n",          sizeof(struct inotify_event));
    printf("sizeof_siginfo_t=%zu\n",              sizeof(siginfo_t));
    return 0;
}
"#;

fn header_include_dir() -> PathBuf {
    // .../include/systemd/sd-event.h  → .../include
    let p = PathBuf::from(HEADER_PATH);
    p.parent()
        .and_then(|p| p.parent())
        .expect("malformed SD_EVENT_HEADER path")
        .to_path_buf()
}

fn compile_and_run_verifier() -> Result<HashMap<String, i64>, String> {
    let cc = std::env::var("CC").unwrap_or_else(|_| "cc".to_string());

    let scratch = std::env::temp_dir().join(format!("sd-event-futures-abi-{}", std::process::id()));
    std::fs::create_dir_all(&scratch).map_err(|e| format!("mkdir {scratch:?}: {e}"))?;
    let src = scratch.join("verifier.c");
    let bin = scratch.join("verifier");
    std::fs::write(&src, VERIFIER_C).map_err(|e| format!("write {src:?}: {e}"))?;

    let include = header_include_dir();
    let out = Command::new(&cc)
        .arg("-O2")
        .arg("-Wall")
        .arg("-I")
        .arg(&include)
        .arg("-o")
        .arg(&bin)
        .arg(&src)
        .output()
        .map_err(|e| format!("spawn {cc}: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "C verifier failed to compile:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        ));
    }

    let run = Command::new(&bin)
        .output()
        .map_err(|e| format!("run verifier: {e}"))?;
    if !run.status.success() {
        return Err(format!("verifier exited non-zero: {}", run.status));
    }

    let text = String::from_utf8_lossy(&run.stdout).into_owned();
    let mut map = HashMap::new();
    for line in text.lines() {
        let (k, v) = line
            .split_once('=')
            .ok_or_else(|| format!("malformed verifier line: {line:?}"))?;
        let parsed: i64 = v.parse().map_err(|e| format!("parse {v:?}: {e}"))?;
        map.insert(k.to_string(), parsed);
    }

    // Best-effort cleanup; ignore errors.
    let _ = std::fs::remove_dir_all(&scratch);

    Ok(map)
}

#[test]
fn constants_and_sizes_match_c_headers() {
    let values = match compile_and_run_verifier() {
        Ok(v) => v,
        Err(e) => panic!("verifier setup failed: {e}"),
    };

    let constants: &[(&str, i64)] = &[
        ("SD_EVENT_OFF", sys::SD_EVENT_OFF as i64),
        ("SD_EVENT_ON", sys::SD_EVENT_ON as i64),
        ("SD_EVENT_ONESHOT", sys::SD_EVENT_ONESHOT as i64),
        ("SD_EVENT_INITIAL", sys::SD_EVENT_INITIAL as i64),
        ("SD_EVENT_ARMED", sys::SD_EVENT_ARMED as i64),
        ("SD_EVENT_PENDING", sys::SD_EVENT_PENDING as i64),
        ("SD_EVENT_RUNNING", sys::SD_EVENT_RUNNING as i64),
        ("SD_EVENT_EXITING", sys::SD_EVENT_EXITING as i64),
        ("SD_EVENT_FINISHED", sys::SD_EVENT_FINISHED as i64),
        ("SD_EVENT_PREPARING", sys::SD_EVENT_PREPARING as i64),
        (
            "SD_EVENT_PRIORITY_IMPORTANT",
            sys::SD_EVENT_PRIORITY_IMPORTANT,
        ),
        ("SD_EVENT_PRIORITY_NORMAL", sys::SD_EVENT_PRIORITY_NORMAL),
        ("SD_EVENT_PRIORITY_IDLE", sys::SD_EVENT_PRIORITY_IDLE),
        (
            "SD_EVENT_SIGNAL_PROCMASK",
            sys::SD_EVENT_SIGNAL_PROCMASK as i64,
        ),
    ];
    for (name, rust_value) in constants {
        let c_value = values
            .get(*name)
            .unwrap_or_else(|| panic!("verifier didn't report {name}"));
        assert_eq!(
            *c_value, *rust_value,
            "{name}: C={c_value}, Rust={rust_value}",
        );
    }

    let sizes: &[(&str, usize)] = &[
        ("sizeof_int", std::mem::size_of::<std::os::raw::c_int>()),
        ("sizeof_uint32_t", std::mem::size_of::<u32>()),
        ("sizeof_uint64_t", std::mem::size_of::<u64>()),
        ("sizeof_clockid_t", std::mem::size_of::<libc::clockid_t>()),
        ("sizeof_pid_t", std::mem::size_of::<libc::pid_t>()),
        (
            "sizeof_signalfd_siginfo",
            std::mem::size_of::<libc::signalfd_siginfo>(),
        ),
        (
            "sizeof_inotify_event",
            std::mem::size_of::<libc::inotify_event>(),
        ),
        ("sizeof_siginfo_t", std::mem::size_of::<libc::siginfo_t>()),
    ];
    for (name, rust_size) in sizes {
        let c_size = values
            .get(*name)
            .unwrap_or_else(|| panic!("verifier didn't report {name}"));
        assert_eq!(
            *c_size as usize, *rust_size,
            "{name}: C={c_size}, Rust={rust_size}",
        );
    }
}
