//! Tests of `examples/modules_load.rs`.
#![deny(unsafe_code)]

mod common;
#[allow(dead_code)]
#[path = "../examples/modules_load.rs"]
mod modules_load;

use std::io::{self, ErrorKind};
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use sd_event_futures::executor::block_on;

use common::within;
use modules_load::{gather, load_modules, modules_to_load};

const LIMIT: Duration = Duration::from_secs(5);
const LOAD_TIME: Duration = Duration::from_millis(30);

fn slow_load(module: &str) -> io::Result<()> {
    thread::sleep(LOAD_TIME);
    match module {
        "missing" => Err(ErrorKind::NotFound.into()),
        "broken" => Err(io::Error::other("Exec format error")),
        _ => Ok(()),
    }
}

#[test]
fn loads_in_parallel_and_reports_the_first_real_error() {
    let mut modules: Vec<String> = (0..14).map(|i| format!("mod{i}")).collect();
    modules.push("missing".into());
    modules.push("broken".into());

    let started = Instant::now();
    let results = block_on(within(LIMIT, load_modules(&modules, slow_load)));
    let elapsed = started.elapsed();

    assert_eq!(
        gather(results).unwrap_err().to_string(),
        "Exec format error"
    );
    // One at a time would take 16 rounds; other tests share the pool, so
    // allow a few rounds rather than exactly one.
    assert!(elapsed >= LOAD_TIME, "{elapsed:?}");
    assert!(elapsed < LOAD_TIME * 8, "not parallel: {elapsed:?}");
}

#[test]
fn earlier_directories_shadow_and_mask_files() {
    let root =
        std::env::temp_dir().join(format!("sd-event-futures-modules-{}", std::process::id()));
    let (etc, usr) = (root.join("etc"), root.join("usr"));
    for dir in [&etc, &usr] {
        std::fs::create_dir_all(dir).unwrap();
    }
    std::fs::write(
        usr.join("10-net.conf"),
        "# networking\nbridge\n; tap\n\n  macvlan  \n",
    )
    .unwrap();
    std::fs::write(usr.join("20-kvm.conf"), "kvm-intel\n").unwrap();
    std::fs::write(usr.join("30-old.conf"), "floppy\n").unwrap();
    std::fs::write(etc.join("20-kvm.conf"), "kvm-amd\n").unwrap();
    std::os::unix::fs::symlink("/dev/null", etc.join("30-old.conf")).unwrap();
    std::fs::write(etc.join("README"), "not a .conf file\n").unwrap();

    let modules = modules_to_load(&[&etc, Path::new("/nonexistent"), &usr]).unwrap();
    assert_eq!(modules, ["bridge", "macvlan", "kvm-amd"]);
    let _ = std::fs::remove_dir_all(&root);
}
