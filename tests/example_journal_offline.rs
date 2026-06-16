//! Tests of `examples/journal_offline.rs`.
#![deny(unsafe_code)]

mod common;
#[allow(dead_code)]
#[path = "../examples/journal_offline.rs"]
mod journal_offline;

use std::path::PathBuf;
use std::time::Duration;

use sd_event_futures::executor::block_on;

use common::within;
use journal_offline::{JournalFile, OFFLINE, ONLINE};

const LIMIT: Duration = Duration::from_secs(5);

fn journal_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "sd-event-futures-{name}-{}.journal",
        std::process::id()
    ))
}

#[test]
fn goes_offline_in_the_background_and_back_online() {
    let path = journal_path("offline");
    block_on(within(LIMIT, async {
        let mut journal = JournalFile::create(&path).unwrap();
        journal.append(b"MESSAGE=before offline\n").unwrap();
        assert_eq!(journal.state().unwrap(), ONLINE);

        journal.set_offline().await.unwrap();
        // Logging goes on while the file goes offline.
        journal.append(b"MESSAGE=while going offline\n").unwrap();
        journal.set_online().await.unwrap();
        assert_eq!(journal.state().unwrap(), ONLINE);
        assert!(!journal.is_offlining());

        journal.set_offline().await.unwrap();
        // A second request waits for the first.
        journal.set_offline().await.unwrap();
        while journal.is_offlining() {
            sd_event_futures::reactor::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(journal.state().unwrap(), OFFLINE);
    }));
    let contents = std::fs::read(&path).unwrap();
    assert!(contents.ends_with(b"MESSAGE=while going offline\n"));
    let _ = std::fs::remove_file(&path);
}
