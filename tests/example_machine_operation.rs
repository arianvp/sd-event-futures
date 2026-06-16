//! Tests of `examples/machine_operation.rs`, run single-threaded (see
//! `run_single_threaded`) since operations fork.
#![deny(unsafe_code)]

mod common;
#[allow(dead_code)]
#[path = "../examples/machine_operation.rs"]
mod machine_operation;

use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use sd_event_futures::combinator::{Either, race};
use sd_event_futures::executor::{block_on, spawn};
use sd_event_futures::os;
use sd_event_futures::reactor::{sleep, yield_now};

use common::within;
use machine_operation::{Manager, remove_image};

const LIMIT: Duration = Duration::from_secs(5);

fn scratch(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("sd-event-futures-{name}-{}", std::process::id()))
}

/// Records its pid in `pidfile` and then never finishes.
fn hang_forever(pidfile: &Path) -> impl FnOnce() -> io::Result<()> + use<> {
    let pidfile = pidfile.to_owned();
    move || {
        std::fs::write(&pidfile, std::process::id().to_string())?;
        std::thread::sleep(Duration::MAX);
        Ok(())
    }
}

fn remove_image_succeeds() {
    let image = scratch("image");
    std::fs::create_dir_all(image.join("usr/lib")).unwrap();
    std::fs::write(image.join("usr/lib/os-release"), b"ID=test\n").unwrap();
    let m = Manager::new(64);
    block_on(within(LIMIT, remove_image(&m, image.clone()))).unwrap();
    assert!(!image.exists());
    assert_eq!(m.n_operations(), 0);
}

fn failure_in_child_is_reported_as_errno() {
    let m = Manager::new(64);
    let err = block_on(within(LIMIT, remove_image(&m, scratch("missing")))).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
}

fn too_many_operations_are_refused() {
    let pidfile = scratch("op-limit");
    block_on(async {
        let m = Rc::new(Manager::new(1));
        let busy = {
            let (m, work) = (m.clone(), hang_forever(&pidfile));
            spawn(async move { m.run_operation(work).await })
        };
        yield_now().await;
        let err = within(LIMIT, m.run_operation(|| Ok(()))).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::QuotaExceeded);
        busy.abort();
        assert_eq!(m.n_operations(), 0);
    });
    let _ = std::fs::remove_file(&pidfile);
}

fn cancelled_operation_kills_and_reaps_child() {
    let pidfile = scratch("op-cancel");
    let m = Manager::new(64);
    block_on(async {
        // The caller gives up after 100ms, dropping the operation.
        let outcome = race(
            m.run_operation(hang_forever(&pidfile)),
            sleep(Duration::from_millis(100)),
        )
        .await;
        assert!(matches!(outcome, Either::Right(())));
    });
    let pid: libc::pid_t = std::fs::read_to_string(&pidfile).unwrap().parse().unwrap();
    assert!(
        !os::process_exists(pid),
        "operation child {pid} survived cancellation"
    );
    assert_eq!(m.n_operations(), 0);
    let _ = std::fs::remove_file(&pidfile);
}

fn main() -> std::process::ExitCode {
    common::run_single_threaded(common::tests![
        remove_image_succeeds,
        failure_in_child_is_reported_as_errno,
        too_many_operations_are_refused,
        cancelled_operation_kills_and_reaps_child,
    ])
}
