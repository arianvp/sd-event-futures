//! The service manager's end of `$NOTIFY_SOCKET`.

use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::net::UnixDatagram;
use std::path::Path;
use std::time::{Duration, Instant};

use sd_event_futures::os;

/// One message received on a [`NotifySocket`].
pub struct Notification {
    pub text: String,
    /// Passed along with the message (`FDSTORE=1`).
    pub fds: Vec<OwnedFd>,
}

impl Notification {
    /// Whether the message contains the line `assignment`, e.g. `READY=1`.
    pub fn has(&self, assignment: &str) -> bool {
        self.text.lines().any(|line| line == assignment)
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.text
            .lines()
            .find_map(|line| line.strip_prefix(key)?.strip_prefix('='))
    }
}

pub struct NotifySocket {
    socket: UnixDatagram,
    /// Text of every message received so far.
    pub history: Vec<String>,
}

impl NotifySocket {
    pub fn bind(path: &Path) -> Self {
        let socket = UnixDatagram::bind(path).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_millis(20)))
            .unwrap();
        Self {
            socket,
            history: Vec::new(),
        }
    }

    /// The next message, if one arrives within the socket's short timeout.
    pub fn recv(&mut self) -> Option<Notification> {
        let mut buf = [0; 4096];
        let (n, fds) = os::recv_with_fds(self.socket.as_fd(), &mut buf).ok()?;
        let text = String::from_utf8_lossy(&buf[..n]).into_owned();
        self.history.push(text.clone());
        Some(Notification { text, fds })
    }

    /// The first message from now on that satisfies `matches`, waiting up
    /// to five seconds.
    pub fn expect(&mut self, what: &str, matches: impl Fn(&Notification) -> bool) -> Notification {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if let Some(n) = self.recv().filter(&matches) {
                return n;
            }
        }
        panic!("no {what} notification; got {:?}", self.history);
    }

    /// Every message that arrives within `period`.
    pub fn collect(&mut self, period: Duration) -> Vec<Notification> {
        let deadline = Instant::now() + period;
        std::iter::from_fn(|| (Instant::now() < deadline).then(|| self.recv()))
            .flatten()
            .collect()
    }
}
