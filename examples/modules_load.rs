// SPDX-License-Identifier: LGPL-2.1-or-later

//! systemd-modules-load: loading kernel modules in parallel.
//!
//! Loading a module blocks in the kernel, so modules-load spreads them over
//! up to 16 threads. Module names are queued as packets on a
//! `SOCK_SEQPACKET` socketpair, the main thread closes the sending end so
//! that `recv()` returns 0 once the queue is empty, takes part in loading
//! itself, then joins every thread and gathers their errors through
//! `void *` return values. Missing modules (`ENOENT`) are not an error.
//!
//! - queue and worker loop:
//!   https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/modules-load/modules-load.c#L170-L234
//! - thread pool: https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/modules-load/modules-load.c#L236-L305
//! - driving it: https://github.com/systemd/systemd/blob/f1d45e3d1466b24ceed38e3a54289ecf31edf9d1/src/modules-load/modules-load.c#L394-L427
//!
//! Here the thread pool is the one every blocking call already shares
//! ([`blocking`](sd_event_futures::blocking)), so there is no queue, no
//! worker loop and no joining: each module is one job and `join_all`
//! gathers the results. modules-load has no event loop of its own, so in a
//! standalone tool `std::thread::scope` would do just as well; inside a
//! daemon this keeps the loop free while modules load.
//!
//! ```sh
//! cargo run --example modules_load            # asks modprobe --dry-run
//! sudo target/debug/examples/modules_load --load
//! ```
//!
//! loads the modules listed in this system's `modules-load.d` files. Each
//! load runs `modprobe`, standing in for the blocking libkmod call the C
//! code makes.
//!
//! Links to the C code are pinned to systemd commit f1d45e3d.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::Instant;

use sd_event_futures::combinator::join_all;
use sd_event_futures::executor::{block_on, spawn_blocking};
use sd_event_futures::log;

/// `CONF_PATHS("modules-load.d")`, most important first.
const CONFIG_DIRS: [&str; 4] = [
    "/etc/modules-load.d",
    "/run/modules-load.d",
    "/usr/local/lib/modules-load.d",
    "/usr/lib/modules-load.d",
];

/// Loads one module, blocking; `kmod_module_probe_insert_module()` in C.
pub type Loader = fn(&str) -> io::Result<()>;

pub fn main() -> ExitCode {
    let (load, done): (Loader, _) = match std::env::args().nth(1).as_deref() {
        Some("--load") => (modprobe, "loaded"),
        _ => (modprobe_dry_run, "would be loaded"),
    };
    let dirs = CONFIG_DIRS.map(Path::new);
    let modules = match modules_to_load(&dirs) {
        Ok(modules) => modules,
        Err(e) => {
            log!(Error, "Failed to read configuration: {e}");
            return ExitCode::FAILURE;
        }
    };
    let started = Instant::now();
    let results = block_on(load_modules(&modules, load));
    for (module, result) in modules.iter().zip(&results) {
        match result {
            Ok(()) => println!("{module}: {done}"),
            Err(e) => println!("{module}: {e}"),
        }
    }
    println!("{} modules in {:.1?}.", modules.len(), started.elapsed());
    match gather(results) {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => ExitCode::FAILURE,
    }
}

/// The module names in the `*.conf` files of `dirs`. A file shadows those
/// of the same name in later directories, and files are read in order of
/// their names, like `conf_files_list()`. A file that is empty, or a
/// symlink to `/dev/null`, so masks the ones it shadows.
pub fn modules_to_load(dirs: &[&Path]) -> io::Result<Vec<String>> {
    let mut files: BTreeMap<OsString, PathBuf> = BTreeMap::new();
    for dir in dirs {
        let entries = match std::fs::read_dir(dir) {
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            entries => entries?,
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "conf") {
                files.entry(entry.file_name()).or_insert(path);
            }
        }
    }
    let mut modules = Vec::new();
    for path in files.values() {
        for line in std::fs::read_to_string(path)?.lines().map(str::trim) {
            if !line.is_empty() && !line.starts_with(['#', ';']) {
                modules.push(line.to_owned());
            }
        }
    }
    Ok(modules)
}

/// Load all `modules` at once on the blocking pool.
pub async fn load_modules(modules: &[String], load: Loader) -> Vec<io::Result<()>> {
    join_all(modules.iter().map(|module| {
        let module = module.clone();
        spawn_blocking(move || load(&module))
    }))
    .await
}

/// Missing modules are fine; otherwise the first error wins, like
/// `RET_GATHER()`.
pub fn gather(results: Vec<io::Result<()>>) -> io::Result<()> {
    results
        .into_iter()
        .filter(|r| !matches!(r, Err(e) if e.kind() == ErrorKind::NotFound))
        .collect()
}

fn modprobe(module: &str) -> io::Result<()> {
    run_modprobe(&["--quiet", module])
}

fn modprobe_dry_run(module: &str) -> io::Result<()> {
    run_modprobe(&["--dry-run", module])
}

fn run_modprobe(args: &[&str]) -> io::Result<()> {
    let out = Command::new("modprobe").args(args).output()?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    match (out.status.success(), stderr.trim()) {
        (true, _) => Ok(()),
        (false, why) if why.contains("not found") => Err(io::Error::new(ErrorKind::NotFound, why)),
        (false, why) => Err(io::Error::other(why.to_owned())),
    }
}
