//! End-to-end tests of `examples/greeter.rs`, with the test playing the
//! service manager: it passes a listening socket (socket activation) or
//! keeps one in its fd store, receives notifications on its own
//! `NOTIFY_SOCKET`, provides a credentials directory, a FIFO as
//! `MEMORY_PRESSURE_WATCH`, and a pipe as stderr that it registers as the
//! journal stream.
#![deny(unsafe_code)]

mod common;
#[path = "../examples/greeter.rs"]
mod greeter;

use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

use sd_event_futures::os;

use common::{Notification, NotifySocket, RUN_EXAMPLE, socket_activated};

const LIMIT: Duration = Duration::from_secs(5);

#[derive(Default)]
struct Setup<'a> {
    greeting: Option<&'a str>,
    watchdog: Option<Duration>,
    /// Passed in as fd 3 with this name; without it the service binds its own.
    socket: Option<(OwnedFd, &'a str)>,
    /// Overrides the manager's own `NOTIFY_SOCKET`.
    notify_socket: Option<String>,
}

impl Setup<'_> {
    fn activated() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        Setup {
            socket: Some((listener.into(), "greeting")),
            ..Setup::default()
        }
    }
}

struct Manager {
    dir: PathBuf,
    notify: NotifySocket,
    service: Child,
    /// Lines of its stderr, as they come.
    log: Receiver<String>,
    logged: Vec<String>,
    pressure: Option<File>,
    port: Option<u16>,
}

impl Manager {
    fn start(test: &str, setup: Setup<'_>) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "sd-event-futures-service-{test}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        Self::start_in(dir, setup)
    }

    fn start_in(dir: PathBuf, setup: Setup<'_>) -> Self {
        let credentials = dir.join("credentials");
        std::fs::create_dir_all(&credentials).unwrap();
        if let Some(greeting) = setup.greeting {
            std::fs::write(credentials.join("greeting"), greeting).unwrap();
        }
        let _ = std::fs::remove_file(dir.join("notify"));
        let notify = NotifySocket::bind(&dir.join("notify"));
        let fifo = dir.join("memory-pressure");
        if !fifo.exists() {
            os::mkfifo(&fifo, 0o600).unwrap();
        }

        let (stderr_rd, stderr_wr) = os::pipe().unwrap();
        let (device, inode) = os::file_identity(stderr_wr.as_fd()).unwrap();

        let port = setup.socket.as_ref().map(|(fd, _)| port_of(fd));
        let greeter = common::this_program();
        let mut command = match &setup.socket {
            Some((fd, name)) => socket_activated(&greeter, &[(fd.as_fd(), name)]),
            None => {
                let mut command = Command::new(&greeter);
                command.env_clear();
                command
            }
        };
        command
            .envs([RUN_EXAMPLE])
            .env(
                "NOTIFY_SOCKET",
                setup
                    .notify_socket
                    .unwrap_or_else(|| dir.join("notify").display().to_string()),
            )
            .env("CREDENTIALS_DIRECTORY", &credentials)
            .env("MEMORY_PRESSURE_WATCH", &fifo)
            .env("JOURNAL_STREAM", format!("{device}:{inode}"))
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr_wr));
        if let Some(interval) = setup.watchdog {
            command.env("WATCHDOG_USEC", interval.as_micros().to_string());
        }
        let service = command.spawn().unwrap();
        drop(command);

        Manager {
            dir,
            notify,
            service,
            log: lines_of(stderr_rd),
            logged: Vec::new(),
            pressure: None,
            port,
        }
    }

    fn ready(&mut self) -> Notification {
        self.notify.expect("READY=1", |n| n.has("READY=1"))
    }

    fn connect(&self) -> BufReader<TcpStream> {
        let stream =
            TcpStream::connect(("127.0.0.1", self.port.expect("service port known"))).unwrap();
        stream.set_read_timeout(Some(LIMIT)).unwrap();
        BufReader::new(stream)
    }

    fn greeting(&self) -> String {
        let mut line = String::new();
        self.connect().read_line(&mut line).unwrap();
        line
    }

    fn set_greeting(&self, greeting: &str) {
        std::fs::write(self.dir.join("credentials/greeting"), greeting).unwrap();
    }

    fn signal(&self, sig: i32) {
        os::kill(self.service.id() as i32, sig).unwrap();
    }

    /// Writing to the FIFO is what the kernel's PSI trigger does. The
    /// writer stays open: hanging up would make the service's watch fail.
    fn memory_pressure(&mut self) {
        if self.pressure.is_none() {
            let file = File::options()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(self.dir.join("memory-pressure"))
                .expect("service is watching the FIFO");
            self.pressure = Some(file);
        }
        self.pressure.as_mut().unwrap().write_all(b"x").unwrap();
    }

    fn wait(&mut self) -> ExitStatus {
        let deadline = Instant::now() + LIMIT;
        loop {
            if let Some(status) = self.service.try_wait().unwrap() {
                return status;
            }
            assert!(Instant::now() < deadline, "service did not exit");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Wait until the service has logged `line`.
    fn expect_log(&mut self, line: &str) {
        let deadline = Instant::now() + LIMIT;
        while !self.logged.iter().any(|l| l == line) {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.log.recv_timeout(left) {
                Ok(next) => self.logged.push(next),
                Err(_) => panic!("never logged {line:?}: {:?}", self.logged),
            }
        }
    }

    /// Everything the service logged; call after it exited.
    fn log(&mut self) -> Vec<String> {
        self.logged.extend(self.log.iter());
        self.logged.clone()
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        let _ = self.service.kill();
        let _ = self.service.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn port_of(fd: &OwnedFd) -> u16 {
    os::local_address(fd.as_fd()).expect("a TCP socket").port()
}

fn starts_serves_and_stops() {
    let mut m = Manager::start(
        "lifecycle",
        Setup {
            greeting: Some("hello"),
            ..Setup::activated()
        },
    );
    let ready = m.ready();
    assert_eq!(ready.get("STATUS"), Some(r#"Greeting with "hello""#));
    assert!(
        ready.fds.is_empty(),
        "socket-activated service used the fd store"
    );

    assert_eq!(m.greeting(), "hello\n");
    assert_eq!(m.greeting(), "hello\n");

    m.signal(libc::SIGTERM);
    m.notify.expect("STOPPING=1", |n| n.has("STOPPING=1"));
    assert!(m.wait().success());
    assert!(
        !m.notify.history.iter().any(|t| t.contains("WATCHDOG=1")),
        "watchdog wasn't enabled"
    );

    let log = m.log();
    assert!(
        log.iter()
            .any(|l| l.starts_with("<6>Listening on 127.0.0.1:")),
        "{log:?}"
    );
    assert!(
        log.iter().all(|l| l.starts_with('<')),
        "line without journal priority: {log:?}"
    );
}

fn echoes_after_the_greeting() {
    let mut m = Manager::start("echo", Setup::activated());
    m.ready();
    let mut client = m.connect();
    let mut line = String::new();
    client.read_line(&mut line).unwrap();
    assert_eq!(line, "hello from sd-event futures\n");
    client.get_mut().write_all(b"ping\n").unwrap();
    line.clear();
    client.read_line(&mut line).unwrap();
    assert_eq!(line, "ping\n");
}

fn sigint_stops_too() {
    let mut m = Manager::start("sigint", Setup::activated());
    m.ready();
    m.signal(libc::SIGINT);
    m.notify.expect("STOPPING=1", |n| n.has("STOPPING=1"));
    assert!(m.wait().success());
}

fn reloads_on_sighup() {
    let mut m = Manager::start(
        "reload",
        Setup {
            greeting: Some("hello"),
            ..Setup::activated()
        },
    );
    m.ready();

    m.set_greeting("bonjour");
    m.signal(libc::SIGHUP);
    let reloading = m.notify.expect("RELOADING=1", |n| n.has("RELOADING=1"));
    let stamp: u64 = reloading
        .get("MONOTONIC_USEC")
        .expect("MONOTONIC_USEC=")
        .parse()
        .unwrap();
    let now = os::monotonic_usec();
    assert!(
        now >= stamp && now - stamp < LIMIT.as_micros() as u64,
        "stamp {stamp}, now {now}"
    );

    assert_eq!(m.ready().get("STATUS"), Some(r#"Greeting with "bonjour""#));
    assert_eq!(m.greeting(), "bonjour\n");

    // A broken configuration still completes the reload, keeping the old one.
    m.set_greeting("");
    m.signal(libc::SIGHUP);
    m.notify.expect("RELOADING=1", |n| n.has("RELOADING=1"));
    assert_eq!(m.ready().get("STATUS"), Some(r#"Greeting with "bonjour""#));
    assert_eq!(m.greeting(), "bonjour\n");

    m.signal(libc::SIGTERM);
    assert!(m.wait().success());
    assert!(
        m.log()
            .iter()
            .any(|l| l.starts_with("<4>Failed to reload configuration"))
    );
}

fn pings_the_watchdog() {
    let mut m = Manager::start(
        "watchdog",
        Setup {
            watchdog: Some(Duration::from_millis(100)),
            ..Setup::activated()
        },
    );
    m.ready();
    // sd-event pings at half the interval.
    let pings = m
        .notify
        .collect(Duration::from_millis(500))
        .iter()
        .filter(|n| n.has("WATCHDOG=1"))
        .count();
    assert!(pings >= 3, "{pings} watchdog pings in 500ms");
    m.signal(libc::SIGTERM);
    assert!(m.wait().success());
}

fn releases_memory_under_pressure() {
    let mut m = Manager::start("pressure", Setup::activated());
    m.ready();
    m.memory_pressure();
    m.expect_log("<5>Memory pressure, released cached memory.");
    m.signal(libc::SIGTERM);
    assert!(m.wait().success());
    let trims = m
        .log()
        .iter()
        .filter(|l| l.contains("Memory pressure"))
        .count();
    assert_eq!(trims, 1, "released memory without pressure");
}

fn startup_failure_reports_errno() {
    let mut m = Manager::start(
        "failure",
        Setup {
            greeting: Some(""),
            ..Setup::activated()
        },
    );
    let failed = m.notify.expect("ERRNO=", |n| n.get("ERRNO").is_some());
    assert_eq!(failed.get("ERRNO"), Some(libc::EINVAL.to_string().as_str()));
    assert!(
        failed
            .get("STATUS")
            .is_some_and(|s| s.contains("must contain exactly one non-empty line"))
    );
    assert!(!m.wait().success());
    assert!(!m.notify.history.iter().any(|t| t.contains("READY=1")));
    assert!(m.log().iter().any(|l| l.starts_with("<3>")));
}

fn keeps_its_listening_socket_in_the_fd_store_across_restarts() {
    let mut first = Manager::start("fdstore", Setup::default());
    let stored = first.notify.expect("FDSTORE=1", |n| n.has("FDSTORE=1"));
    assert_eq!(stored.get("FDNAME"), Some("listener"));
    let Notification { mut fds, .. } = stored;
    assert_eq!(fds.len(), 1);
    let listener = fds.pop().unwrap();
    first.port = Some(port_of(&listener));
    first.ready();
    assert_eq!(first.greeting(), "hello from sd-event futures\n");

    // A crash: the socket lives on in the store, and a client connecting
    // meanwhile waits in its queue instead of being refused.
    let dir = first.dir.clone();
    first.service.kill().unwrap();
    first.wait();
    let early = TcpStream::connect(("127.0.0.1", first.port.unwrap())).unwrap();
    std::mem::forget(first);

    let mut second = Manager::start_in(
        dir,
        Setup {
            socket: Some((listener, "listener")),
            ..Setup::default()
        },
    );
    let ready = second.ready();
    assert!(
        ready.fds.is_empty()
            && !second
                .notify
                .history
                .iter()
                .any(|t| t.contains("FDSTORE=1"))
    );
    let mut line = String::new();
    early.set_read_timeout(Some(LIMIT)).unwrap();
    BufReader::new(early).read_line(&mut line).unwrap();
    assert_eq!(line, "hello from sd-event futures\n");
}

fn drains_open_connections_before_stopping() {
    let mut m = Manager::start("drain", Setup::activated());
    m.ready();
    let mut client = m.connect();
    let mut line = String::new();
    client.read_line(&mut line).unwrap();

    m.signal(libc::SIGTERM);
    m.notify.expect("STOPPING=1", |n| n.has("STOPPING=1"));
    let extend = m.notify.expect("EXTEND_TIMEOUT_USEC=", |n| {
        n.get("EXTEND_TIMEOUT_USEC").is_some()
    });
    assert_eq!(extend.get("EXTEND_TIMEOUT_USEC"), Some("5000000"));

    // Still served while stopping.
    client.get_mut().write_all(b"still there?\n").unwrap();
    line.clear();
    client.read_line(&mut line).unwrap();
    assert_eq!(line, "still there?\n");
    assert!(
        m.service.try_wait().unwrap().is_none(),
        "exited with a connection open"
    );

    client.get_mut().shutdown(Shutdown::Write).unwrap();
    assert!(m.wait().success());
    assert!(!m.log().iter().any(|l| l.contains("Abandoning")));
}

/// A vsock listener on the local CID, as a hypervisor would hold one.
struct VsockListener(OwnedFd, u32);

impl VsockListener {
    fn bind() -> Self {
        let (socket, port) = os::vsock_listen(libc::SOCK_SEQPACKET).expect("AF_VSOCK");
        VsockListener(socket, port)
    }

    /// The next message, each sent over a connection of its own.
    fn recv(&self) -> Option<String> {
        os::wait_readable(self.0.as_fd(), LIMIT)
            .ok()
            .filter(|&ready| ready)?;
        let connection = os::accept(self.0.as_fd()).ok()?;
        os::wait_readable(connection.as_fd(), LIMIT).ok()?;
        let mut buf = [0u8; 4096];
        let n = File::from(connection).read(&mut buf).ok()?;
        Some(String::from_utf8_lossy(&buf[..n]).into_owned())
    }
}

fn notifies_over_vsock() {
    for scheme in ["vsock-seqpacket", "vsock"] {
        let vsock = VsockListener::bind();
        let mut m = Manager::start(
            &format!("{scheme}-notify"),
            Setup {
                notify_socket: Some(format!("{scheme}:local:{}", vsock.1)),
                ..Setup::activated()
            },
        );
        let Some(ready) = vsock.recv() else {
            let _ = m.service.kill();
            m.wait();
            panic!("{scheme}: no READY=1; log: {:?}", m.log());
        };
        assert!(ready.lines().any(|l| l == "READY=1"), "{scheme}: {ready:?}");
        m.signal(libc::SIGTERM);
        let Some(stopping) = vsock.recv() else {
            let _ = m.service.kill();
            m.wait();
            panic!("{scheme}: no STOPPING=1; log: {:?}", m.log());
        };
        assert!(
            stopping.lines().any(|l| l == "STOPPING=1"),
            "{scheme}: {stopping:?}"
        );
        assert!(m.wait().success());
    }
}

fn main() -> std::process::ExitCode {
    common::run_example_or(
        greeter::main,
        common::tests![
            starts_serves_and_stops,
            echoes_after_the_greeting,
            sigint_stops_too,
            reloads_on_sighup,
            pings_the_watchdog,
            releases_memory_under_pressure,
            startup_failure_reports_errno,
            keeps_its_listening_socket_in_the_fd_store_across_restarts,
            drains_open_connections_before_stopping,
            notifies_over_vsock
        ],
    )
}

/// The lines read from `pipe`, until it reaches end-of-file.
fn lines_of(pipe: OwnedFd) -> Receiver<String> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(File::from(pipe)).lines() {
            let Ok(line) = line else { break };
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    rx
}
