//! Differential tests of `examples/systemd-socket-proxyd/` against
//! systemd's own `systemd-socket-proxyd`, the oracle.
//!
//! Every scenario runs against both programs and records what can be
//! observed from outside: bytes arriving at each end, end-of-file and
//! half-close behaviour, refused connections, notifications, exit status
//! and error messages. The two records must be equal. The original is
//! taken from `$SYSTEMD_SOCKET_PROXYD`, or the running system's systemd;
//! without it the scenarios still check this implementation against the
//! expected outcome.
//!
//! Intended differences, tested separately:
//! - `--version`, and the project, version and features fields of
//!   `--introspect-cli`, name this crate rather than a systemd release.
//! - The original has no `SIGTERM`/`SIGINT` handler and dies from the
//!   signal; this one sends `STOPPING=1` and exits successfully. systemd
//!   treats both as a clean stop.
//! - This follows two fixes made after systemd 262, so an original of
//!   262 or older differs there: it accepts one connection more than
//!   `--connections-max` (fixed by systemd commit 9b51efd69fad), and leaves
//!   `RLIMIT_NOFILE` alone (raised since 98752540be47).
#![deny(unsafe_code)]

mod common;
// Its unit tests run with the example itself.
#[allow(dead_code)]
#[path = "../examples/systemd-socket-proxyd/main.rs"]
mod proxyd;

use std::fmt::Debug;
use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use sd_event_futures::os;

use common::{NotifySocket, RUN_EXAMPLE, socket_activated};

const LIMIT: Duration = Duration::from_secs(5);
/// This implementation: the test binary, run as the example.
fn ours() -> PathBuf {
    common::this_program().into()
}

fn original() -> Option<PathBuf> {
    let path = std::env::var_os("SYSTEMD_SOCKET_PROXYD")
        .map(PathBuf::from)
        .unwrap_or_else(|| "/run/current-system/sw/lib/systemd/systemd-socket-proxyd".into());
    path.exists().then_some(path)
}

/// The original's systemd version, from `--version`.
fn version(original: &Path) -> u32 {
    let out = Command::new(original).arg("--version").output().unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    text.split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .expect("systemd NNN")
}

/// Run `scenario` against this implementation and, if available, the
/// original; they must agree. Returns this implementation's outcome.
fn differential<T: PartialEq + Debug>(scenario: impl Fn(&Path) -> T) -> T {
    let ours = scenario(&ours());
    if let Some(original) = original() {
        let theirs = scenario(&original);
        assert_eq!(
            ours,
            theirs,
            "differs from {} (left: ours)",
            original.display()
        );
    }
    ours
}

// =============================================================================
// Harness
// =============================================================================

/// A proxy started the way systemd starts one: socket-activated, with a
/// notification socket and a clean environment.
struct Proxy {
    service: Child,
    notify: NotifySocket,
    stderr: Option<OwnedFd>,
    dir: PathBuf,
}

impl Proxy {
    fn start(program: &Path, args: &[&str], sockets: &[(BorrowedFd<'_>, &str)]) -> Self {
        Self::start_with_env(program, args, sockets, &[])
    }

    fn start_with_env(
        program: &Path,
        args: &[&str],
        sockets: &[(BorrowedFd<'_>, &str)],
        env: &[(&str, &str)],
    ) -> Self {
        static RUNS: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "sd-event-futures-proxyd-{}-{}",
            std::process::id(),
            RUNS.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let notify = NotifySocket::bind(&dir.join("notify"));

        let (stderr_rd, stderr_wr) = os::pipe().unwrap();

        let program = program.to_str().unwrap();
        let mut command = if sockets.is_empty() {
            let mut command = Command::new(program);
            command.env_clear();
            command
        } else {
            socket_activated(program, sockets)
        };
        let service = command
            .args(args)
            .envs(env.iter().copied())
            .envs([RUN_EXAMPLE])
            .env("NOTIFY_SOCKET", dir.join("notify"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr_wr))
            .spawn()
            .unwrap();
        drop(command);
        Proxy {
            service,
            notify,
            stderr: Some(stderr_rd),
            dir,
        }
    }

    fn ready(&mut self) -> String {
        self.notify.expect("READY=1", |n| n.has("READY=1")).text
    }

    fn wait(&mut self) -> ExitStatus {
        let deadline = Instant::now() + LIMIT;
        loop {
            if let Some(status) = self.service.try_wait().unwrap() {
                return status;
            }
            assert!(Instant::now() < deadline, "proxy did not exit");
            thread::sleep(Duration::from_millis(5));
        }
    }

    /// Stop it and return what it logged.
    fn finish(mut self) -> Vec<String> {
        let _ = self.service.kill();
        self.wait();
        self.log()
    }

    /// Everything logged so far; the proxy must have exited.
    fn log(&mut self) -> Vec<String> {
        let mut log = String::new();
        File::from(self.stderr.take().unwrap())
            .read_to_string(&mut log)
            .unwrap();
        log.lines().map(str::to_owned).collect()
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        let _ = self.service.kill();
        let _ = self.service.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn tcp_listener() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

/// Echoes everything back as it arrives, and half-closes on end of file.
fn echo_backend() -> String {
    let (listener, port) = tcp_listener();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            thread::spawn(move || {
                let mut buf = [0; 65536];
                while let Ok(n @ 1..) = stream.read(&mut buf) {
                    if stream.write_all(&buf[..n]).is_err() {
                        return;
                    }
                }
                let _ = stream.shutdown(Shutdown::Write);
            });
        }
    });
    format!("127.0.0.1:{port}")
}

/// Hands every connection's first `n` bytes to `received`, sends `reply`,
/// then closes.
fn recording_backend(
    n: usize,
    reply: &'static [u8],
) -> (String, std::sync::mpsc::Receiver<Vec<u8>>) {
    let (listener, port) = tcp_listener();
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            let mut data = vec![0; n];
            let _ = stream.read_exact(&mut data);
            let _ = tx.send(data);
            let _ = stream.write_all(reply);
        }
    });
    (format!("127.0.0.1:{port}"), rx)
}

/// How a connection through the proxy ended, as the client saw it.
#[derive(Debug, PartialEq)]
enum Reply {
    Data(Vec<u8>),
    /// Closed without data, by FIN or RST.
    Closed,
}

fn exchange(port: u16, message: &[u8]) -> Reply {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream.set_read_timeout(Some(LIMIT)).unwrap();
    let _ = stream.write_all(message);
    let _ = stream.shutdown(Shutdown::Write);
    let mut reply = Vec::new();
    match stream.read_to_end(&mut reply) {
        Ok(_) if !reply.is_empty() => Reply::Data(reply),
        Ok(_) => Reply::Closed,
        Err(e) if e.kind() == ErrorKind::ConnectionReset => Reply::Closed,
        Err(e) => panic!("client: {e}"),
    }
}

// =============================================================================
// Command line
// =============================================================================

#[derive(Debug, PartialEq)]
struct CliOutcome {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

fn cli(program: &Path, args: &[&str]) -> CliOutcome {
    let out = Command::new(program)
        .args(args)
        .env_clear()
        .envs([RUN_EXAMPLE])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    CliOutcome {
        code: out.status.code(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

fn command_line_matches() {
    let cases: &[&[&str]] = &[
        &["--help"],
        &["-h"],
        &["-hc5"],
        &["-c5", "--help"],
        &["x", "--help", "--bogus"],
        &["--hel"],
        &[],
        &["a", "b"],
        &["-c"],
        &["-c", "abc", "x"],
        &["-cabc", "x"],
        &["-c", "5x", "x"],
        &["-c", "0", "x"],
        &["-c", "-1", "x"],
        &["-c", "4294967296", "x"],
        &["--connections-max"],
        &["--help=x"],
        &["-x"],
        &["--bogus"],
        &["--bogus", "--help"],
        &["--exit-idle-time"],
        &["--exit-idle-time=", "x"],
        &["--exit-idle-time=5x", "x"],
        &["--exit-idle-time=3.", "x"],
        &["--proxy-protocol", "x"],
        &["--proxy-protocol=V1", "x"],
        &["--proxy-protocol=v2", "x"],
        // These parse, and then find no sockets.
        &["x"],
        &["--", "--help"],
        &["-c", "+5", "x"],
        &["--conn=5", "--exit=1min 30s", "x"],
        &["--exit-idle-time=infinity", "--proxy-protocol=v1", "/run/x"],
    ];
    for args in cases {
        let outcome = differential(|program| cli(program, args));
        assert!(outcome.code.is_some(), "{args:?}");
    }
}

/// The introspection document with this crate's own identity in place of
/// the systemd release's.
fn introspection(program: &Path) -> CliOutcome {
    let mut outcome = cli(program, &["--introspect-cli"]);
    let start = outcome.stdout.find(r#""project":"#).expect("project field");
    let end = outcome
        .stdout
        .find(r#","abstract":"#)
        .expect("abstract field");
    outcome.stdout.replace_range(start..end, "IDENTITY");
    outcome
}

fn introspection_matches_apart_from_identity() {
    let outcome = differential(introspection);
    assert_eq!(outcome.code, Some(0));
    let ours = cli(&ours(), &["--introspect-cli"]).stdout;
    assert!(ours.contains(&format!(
        r#""project":"sd-event-futures","version":"{}""#,
        env!("CARGO_PKG_VERSION")
    )));
}

fn version_names_this_crate() {
    let outcome = cli(&ours(), &["--version"]);
    assert_eq!(outcome.code, Some(0));
    assert_eq!(
        outcome.stdout,
        format!(
            "systemd-socket-proxyd (sd-event-futures {})\n",
            env!("CARGO_PKG_VERSION")
        )
    );
}

// =============================================================================
// Start-up
// =============================================================================

#[derive(Debug, PartialEq)]
struct StartOutcome {
    code: Option<i32>,
    log: Vec<String>,
}

fn start_with(program: &Path, sockets: &[(BorrowedFd<'_>, &str)]) -> StartOutcome {
    let mut proxy = Proxy::start(program, &["127.0.0.1:1"], sockets);
    let code = proxy.wait().code();
    StartOutcome {
        code,
        log: proxy.log(),
    }
}

fn refuses_to_start_without_sockets() {
    let outcome = differential(|p| start_with(p, &[]));
    assert_eq!(outcome.code, Some(1));
    assert_eq!(outcome.log, ["Didn't get any sockets passed in."]);
}

fn ignores_sockets_passed_to_another_process() {
    let (listener, _) = tcp_listener();
    // A pid may be reused; the pidfd ID names one process for good. This
    // is the test's own: a real process, but not the proxy.
    let elsewhere = os::own_pidfd_id().unwrap().to_string();
    let outcome = differential(|p| {
        let env = [("LISTEN_PIDFDID", elsewhere.as_str())];
        let sockets = [(listener.as_fd(), "proxy")];
        let mut proxy = Proxy::start_with_env(p, &["127.0.0.1:1"], &sockets, &env);
        StartOutcome {
            code: proxy.wait().code(),
            log: proxy.log(),
        }
    });
    assert_eq!(outcome.code, Some(1));
    assert_eq!(outcome.log, ["Didn't get any sockets passed in."]);
}

fn refuses_datagram_and_non_listening_sockets() {
    let udp = UdpSocket::bind("127.0.0.1:0").unwrap();
    let outcome = differential(|p| start_with(p, &[(udp.as_fd(), "udp")]));
    assert_eq!(outcome.log, ["Passed in socket is not a stream socket."]);

    let (backend, port) = tcp_listener();
    let connected = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let outcome = differential(|p| start_with(p, &[(connected.as_fd(), "connected")]));
    assert_eq!(outcome.code, Some(1));
    assert_eq!(outcome.log, ["Passed in socket is not a stream socket."]);
    drop(backend);
}

fn reports_ready_with_the_same_status() {
    let (listener, _) = tcp_listener();
    let ready = differential(|p| {
        let mut proxy = Proxy::start(p, &["127.0.0.1:1"], &[(listener.as_fd(), "proxy")]);
        proxy.ready()
    });
    assert_eq!(ready, "READY=1\nSTATUS=Processing requests...");
}

// =============================================================================
// Forwarding
// =============================================================================

fn forwards_large_transfers_both_ways() {
    let backend = echo_backend();
    let payload: Vec<u8> = (0..4 << 20).map(|i: u32| (i % 251) as u8).collect();
    let outcome = differential(|p| {
        let (listener, port) = tcp_listener();
        let mut proxy = Proxy::start(p, &[&backend], &[(listener.as_fd(), "proxy")]);
        proxy.ready();
        let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream.set_read_timeout(Some(LIMIT)).unwrap();
        let mut reader = stream.try_clone().unwrap();
        let echoed = thread::scope(|s| {
            s.spawn(|| {
                let mut writer = &stream;
                writer.write_all(&payload).unwrap();
                writer.shutdown(Shutdown::Write).unwrap();
            });
            let mut echoed = Vec::new();
            reader.read_to_end(&mut echoed).unwrap();
            echoed
        });
        (echoed.len(), echoed == payload, proxy.finish())
    });
    assert_eq!(outcome, (payload.len(), true, vec![]));
}

fn half_close_reaches_the_backend() {
    // The backend answers only after seeing end of file.
    let (listener, port) = tcp_listener();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            let mut request = Vec::new();
            stream.read_to_end(&mut request).unwrap();
            stream.write_all(&[b"got ", &request[..]].concat()).unwrap();
        }
    });
    let backend = format!("127.0.0.1:{port}");
    let reply = differential(|p| {
        let (listener, port) = tcp_listener();
        let mut proxy = Proxy::start(p, &[&backend], &[(listener.as_fd(), "proxy")]);
        proxy.ready();
        exchange(port, b"request")
    });
    assert_eq!(reply, Reply::Data(b"got request".to_vec()));
}

fn backend_closing_first_reaches_the_client() {
    let (backend, received) = recording_backend(0, b"goodbye");
    let reply = differential(|p| {
        let (listener, port) = tcp_listener();
        let mut proxy = Proxy::start(p, &[&backend], &[(listener.as_fd(), "proxy")]);
        proxy.ready();
        let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream.set_read_timeout(Some(LIMIT)).unwrap();
        let mut reply = Vec::new();
        stream.read_to_end(&mut reply).unwrap();
        received.recv_timeout(LIMIT).unwrap();
        reply
    });
    assert_eq!(reply, b"goodbye");
}

fn forwards_to_unix_sockets_by_path_and_abstract_name() {
    let dir = std::env::temp_dir().join(format!(
        "sd-event-futures-proxyd-unix-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("backend.sock");
    let _ = std::fs::remove_file(&path);
    let name = format!("@sd-event-futures-proxyd-{}", std::process::id());
    for listener in [
        UnixListener::bind(&path).unwrap(),
        UnixListener::bind_addr(&<std::os::unix::net::SocketAddr as std::os::linux::net::SocketAddrExt>::from_abstract_name(
            &name.as_bytes()[1..],
        ).unwrap()).unwrap(),
    ] {
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let mut request = Vec::new();
                stream.read_to_end(&mut request).unwrap();
                stream.write_all(&request.to_ascii_uppercase()).unwrap();
            }
        });
    }
    for remote in [path.to_str().unwrap(), name.as_str()] {
        let reply = differential(|p| {
            let (listener, port) = tcp_listener();
            let mut proxy = Proxy::start(p, &[remote], &[(listener.as_fd(), "proxy")]);
            proxy.ready();
            exchange(port, b"shout")
        });
        assert_eq!(reply, Reply::Data(b"SHOUT".to_vec()), "{remote}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

fn listens_on_every_socket_passed_in() {
    let backend = echo_backend();
    let dir = std::env::temp_dir().join(format!(
        "sd-event-futures-proxyd-listen-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let replies = differential(|p| {
        let (tcp, port) = tcp_listener();
        let path = dir.join(format!("listen-{}.sock", p.file_name().unwrap().len()));
        let _ = std::fs::remove_file(&path);
        let unix = UnixListener::bind(&path).unwrap();
        let mut proxy = Proxy::start(
            p,
            &[&backend],
            &[(tcp.as_fd(), "tcp"), (unix.as_fd(), "unix")],
        );
        proxy.ready();
        let tcp_reply = exchange(port, b"over tcp");
        let mut stream = UnixStream::connect(&path).unwrap();
        stream.write_all(b"over unix").unwrap();
        stream.shutdown(Shutdown::Write).unwrap();
        let mut unix_reply = Vec::new();
        stream.read_to_end(&mut unix_reply).unwrap();
        (tcp_reply, unix_reply)
    });
    assert_eq!(
        replies,
        (Reply::Data(b"over tcp".to_vec()), b"over unix".to_vec())
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// =============================================================================
// PROXY protocol
// =============================================================================

/// The header the backend received, with the client's port replaced by
/// `CLIENT` and the proxy's by `PROXY`.
fn proxy_header(program: &Path, listen: SocketAddr) -> String {
    let (backend, received) = recording_backend(64, b"");
    let listener = TcpListener::bind(listen).unwrap();
    let proxy_port = listener.local_addr().unwrap().port();
    let mut proxy = Proxy::start(
        program,
        &["--proxy-protocol=v1", &backend],
        &[(listener.as_fd(), "proxy")],
    );
    proxy.ready();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let client_port = client.local_addr().unwrap().port();
    (&client).write_all(&[b'x'; 64]).unwrap();
    let data = received.recv_timeout(LIMIT).unwrap();
    let text = String::from_utf8_lossy(&data);
    let header = text.split_inclusive("\r\n").next().unwrap();
    header.replace(
        &format!(" {client_port} {proxy_port}\r\n"),
        " CLIENT PROXY\r\n",
    )
}

fn proxy_protocol_describes_ipv4_clients() {
    let header = differential(|p| proxy_header(p, "127.0.0.1:0".parse().unwrap()));
    assert_eq!(header, "PROXY TCP4 127.0.0.1 127.0.0.1 CLIENT PROXY\r\n");
}

fn proxy_protocol_describes_ipv6_clients() {
    if TcpListener::bind("[::1]:0").is_err() {
        eprintln!("no IPv6 loopback, skipping");
        return;
    }
    let header = differential(|p| proxy_header(p, "[::1]:0".parse().unwrap()));
    assert_eq!(header, "PROXY TCP6 ::1 ::1 CLIENT PROXY\r\n");
}

fn proxy_protocol_reports_unknown_for_unix_clients() {
    let dir = std::env::temp_dir().join(format!(
        "sd-event-futures-proxyd-unknown-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let outcome = differential(|p| {
        let (backend, received) = recording_backend(15, b"");
        let path = dir.join("listen.sock");
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).unwrap();
        let mut proxy = Proxy::start(
            p,
            &["--proxy-protocol=v1", &backend],
            &[(listener.as_fd(), "proxy")],
        );
        proxy.ready();
        let _client = UnixStream::connect(&path).unwrap();
        let header = received.recv_timeout(LIMIT).unwrap();
        (String::from_utf8(header).unwrap(), proxy.finish())
    });
    assert_eq!(outcome.0, "PROXY UNKNOWN\r\n");
    let _ = std::fs::remove_dir_all(&dir);
}

// =============================================================================
// Limits, failures and exiting
// =============================================================================

fn over_the_limit(program: &Path, backend: &str) -> (Reply, Reply, Vec<String>) {
    let (listener, port) = tcp_listener();
    let mut proxy = Proxy::start(
        program,
        &["-c", "1", backend],
        &[(listener.as_fd(), "proxy")],
    );
    proxy.ready();
    // The first connection is established end to end before the second.
    let mut first = TcpStream::connect(("127.0.0.1", port)).unwrap();
    first.set_read_timeout(Some(LIMIT)).unwrap();
    first.write_all(b"1").unwrap();
    let mut one = [0];
    first.read_exact(&mut one).unwrap();
    let second = exchange(port, b"2");
    first.shutdown(Shutdown::Write).unwrap();
    let mut rest = Vec::new();
    first.read_to_end(&mut rest).unwrap();
    // With the first one gone, there is room again.
    let third = exchange(port, b"3");
    (second, third, proxy.finish())
}

fn refuses_connections_over_the_limit() {
    let backend = echo_backend();
    let ours = over_the_limit(&ours(), &backend);
    let refused = (
        Reply::Closed,
        Reply::Data(b"3".to_vec()),
        vec!["Hit connection limit, refusing connection.".to_owned()],
    );
    assert_eq!(ours, refused);
    if let Some(original) = original() {
        let theirs = over_the_limit(&original, &backend);
        if version(&original) > 262 {
            assert_eq!(ours, theirs);
        } else {
            // One past the limit is still let through.
            assert_eq!(
                theirs,
                (
                    Reply::Data(b"2".to_vec()),
                    Reply::Data(b"3".to_vec()),
                    vec![]
                )
            );
        }
    }
}

/// The soft and hard `RLIMIT_NOFILE` of a running process.
fn open_files_limit(pid: u32) -> (String, String) {
    let limits = std::fs::read_to_string(format!("/proc/{pid}/limits")).unwrap();
    let line = limits
        .lines()
        .find(|l| l.starts_with("Max open files"))
        .unwrap();
    let mut fields = line["Max open files".len()..].split_whitespace();
    (
        fields.next().unwrap().to_owned(),
        fields.next().unwrap().to_owned(),
    )
}

fn limit_while_running(program: &Path) -> (String, String) {
    let (listener, _) = tcp_listener();
    let mut proxy = Proxy::start(program, &["127.0.0.1:1"], &[(listener.as_fd(), "proxy")]);
    proxy.ready();
    open_files_limit(proxy.service.id())
}

fn raises_the_open_files_limit() {
    let (soft, hard) = limit_while_running(&ours());
    assert_eq!(soft, hard, "soft limit not raised to the hard one");
    if let Some(original) = original()
        && version(&original) > 262
    {
        assert_eq!(limit_while_running(&original), (soft, hard));
    }
}

fn unreachable_backend_closes_only_that_connection() {
    let (refusing, port) = tcp_listener();
    drop(refusing);
    let outcome = differential(|p| {
        let (listener, port_in) = tcp_listener();
        let mut proxy = Proxy::start(
            p,
            &[&format!("127.0.0.1:{port}")],
            &[(listener.as_fd(), "proxy")],
        );
        proxy.ready();
        let replies = (exchange(port_in, b"a"), exchange(port_in, b"b"));
        let alive = proxy.service.try_wait().unwrap().is_none();
        (replies, alive, proxy.finish())
    });
    assert_eq!(outcome.0, (Reply::Closed, Reply::Closed));
    assert!(outcome.1);
    assert_eq!(
        outcome.2,
        ["Failed to connect to remote host: Connection refused"; 2]
    );
}

fn unresolvable_backend_closes_only_that_connection() {
    let outcome = differential(|p| {
        let (listener, port) = tcp_listener();
        let mut proxy = Proxy::start(
            p,
            &["localhost:no-such-service"],
            &[(listener.as_fd(), "proxy")],
        );
        proxy.ready();
        let reply = exchange(port, b"a");
        let alive = proxy.service.try_wait().unwrap().is_none();
        (reply, alive, proxy.finish())
    });
    assert_eq!(outcome.0, Reply::Closed);
    assert!(outcome.1);
    assert_eq!(outcome.2.len(), 1);
    assert!(
        outcome.2[0].starts_with("Failed to resolve host: "),
        "{:?}",
        outcome.2
    );
}

#[derive(Debug, PartialEq)]
struct IdleOutcome {
    alive_before_any_connection: bool,
    code: Option<i32>,
    stopping: bool,
    waited_out_the_idle_time: bool,
}

fn exits_when_idle_after_a_connection() {
    let backend = echo_backend();
    let outcome = differential(|p| {
        let (listener, port) = tcp_listener();
        let mut proxy = Proxy::start(
            p,
            &["--exit-idle-time=200ms", &backend],
            &[(listener.as_fd(), "proxy")],
        );
        proxy.ready();
        // Started by socket activation, a connection is always on its way;
        // the timer only starts once one has come and gone.
        thread::sleep(Duration::from_millis(400));
        let alive_before_any_connection = proxy.service.try_wait().unwrap().is_none();
        exchange(port, b"x");
        let closed = Instant::now();
        let code = proxy.wait().code();
        let waited_out_the_idle_time = closed.elapsed() >= Duration::from_millis(200);
        let stopping = proxy
            .notify
            .collect(Duration::from_millis(50))
            .iter()
            .any(|n| n.has("STOPPING=1"));
        IdleOutcome {
            alive_before_any_connection,
            code,
            stopping,
            waited_out_the_idle_time,
        }
    });
    assert_eq!(
        outcome,
        IdleOutcome {
            alive_before_any_connection: true,
            code: Some(0),
            stopping: true,
            waited_out_the_idle_time: true
        }
    );
}

fn idle_timer_restarts_after_the_last_connection() {
    let backend = echo_backend();
    let outcome = differential(|p| {
        let (listener, port) = tcp_listener();
        let mut proxy = Proxy::start(
            p,
            &["--exit-idle-time=400ms", &backend],
            &[(listener.as_fd(), "proxy")],
        );
        proxy.ready();
        // Held open for longer than the idle time: no timer runs meanwhile.
        let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();
        client.write_all(b"x").unwrap();
        let mut x = [0];
        client.read_exact(&mut x).unwrap();
        thread::sleep(Duration::from_millis(300));
        let alive_while_connected = proxy.service.try_wait().unwrap().is_none();
        drop(client);
        let closed = Instant::now();
        let code = proxy.wait().code();
        (
            alive_while_connected,
            code,
            closed.elapsed() >= Duration::from_millis(350),
        )
    });
    assert_eq!(outcome, (true, Some(0), true));
}

// =============================================================================
// Intended differences
// =============================================================================

fn stop_with(program: &Path, sig: i32) -> (ExitStatus, bool) {
    let (listener, _) = tcp_listener();
    let mut proxy = Proxy::start(program, &["127.0.0.1:1"], &[(listener.as_fd(), "proxy")]);
    proxy.ready();
    os::kill(proxy.service.id() as i32, sig).unwrap();
    let status = proxy.wait();
    let stopping = proxy
        .notify
        .collect(Duration::from_millis(50))
        .iter()
        .any(|n| n.has("STOPPING=1"));
    (status, stopping)
}

fn stops_cleanly_on_sigterm_and_sigint() {
    use std::os::unix::process::ExitStatusExt;
    for sig in [libc::SIGTERM, libc::SIGINT] {
        let (status, stopping) = stop_with(&ours(), sig);
        assert!(status.success() && stopping, "signal {sig}: {status}");
        if let Some(original) = original() {
            let (status, _) = stop_with(&original, sig);
            assert_eq!(
                status.signal(),
                Some(sig),
                "the original is expected to die from the signal"
            );
        }
    }
}

// =============================================================================
// System call properties
// =============================================================================

/// Bytes moved by `read`, `write`, `recv*` and `send*` calls, and the
/// number of `splice` calls, in an strace log.
fn copied_and_spliced(trace: &str) -> (u64, usize) {
    let mut copied = 0;
    let mut splices = 0;
    for line in trace.lines() {
        // Lines from processes other than the first carry a "[pid N] " prefix.
        let call = match line.strip_prefix("[pid ") {
            Some(rest) => rest.split_once("] ").map_or(rest, |(_, call)| call),
            None => line,
        };
        let result = line
            .rsplit_once(" = ")
            .and_then(|(_, r)| r.split_whitespace().next()?.parse::<i64>().ok());
        if call.starts_with("splice(") {
            splices += 1;
        } else if [
            "read(",
            "write(",
            "recvfrom(",
            "sendto(",
            "recvmsg(",
            "sendmsg(",
        ]
        .iter()
        .any(|c| call.starts_with(c))
        {
            copied += result.filter(|&n| n > 0).unwrap_or(0) as u64;
        }
    }
    (copied, splices)
}

fn moves_payload_with_splice_not_through_user_space() {
    let Some(strace) = std::env::var_os("PATH").and_then(|path| {
        std::env::split_paths(&path)
            .map(|dir| dir.join("strace"))
            .find(|p| p.exists())
    }) else {
        eprintln!("strace not available, skipping");
        return;
    };
    let backend = echo_backend();
    let payload = vec![7u8; 1 << 20];
    let check = |program: &Path| {
        let (listener, port) = tcp_listener();
        let program = program.to_str().unwrap();
        // strace runs the proxy as its child, whose pid isn't known until
        // then: an inner shell sets LISTEN_PID for it, and leaves out the
        // pidfd ID, which it has no way to learn.
        let mut proxy = Proxy::start(
            &strace,
            &[
                "-f",
                "-qq",
                "-e",
                "trace=read,write,recvfrom,sendto,recvmsg,sendmsg,splice",
                "/bin/sh",
                "-c",
                r#"unset LISTEN_PIDFDID; LISTEN_PID=$$ exec "$0" "$@""#,
                program,
                // Exits once the transfer is over, and strace with it.
                "--exit-idle-time=100ms",
                &backend,
            ],
            &[(listener.as_fd(), "proxy")],
        );
        proxy.ready();
        let echoed = {
            let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
            stream.set_read_timeout(Some(LIMIT)).unwrap();
            let mut reader = stream.try_clone().unwrap();
            thread::scope(|s| {
                s.spawn(|| {
                    (&stream).write_all(&payload).unwrap();
                    stream.shutdown(Shutdown::Write).unwrap();
                });
                let mut echoed = Vec::new();
                reader.read_to_end(&mut echoed).unwrap();
                echoed.len()
            })
        };
        assert!(proxy.wait().success());
        // The trace went to the proxy's stderr.
        let (copied, splices) = copied_and_spliced(&proxy.log().join("\n"));
        (echoed, copied, splices)
    };
    for program in std::iter::once(ours()).chain(original()) {
        let (echoed, copied, splices) = check(&program);
        assert_eq!(echoed, payload.len(), "{}", program.display());
        assert!(splices > 0, "{}: no splice()", program.display());
        assert!(
            copied < 64 * 1024,
            "{}: {copied} bytes copied through user space",
            program.display()
        );
    }
}

fn main() -> std::process::ExitCode {
    common::run_example_or(
        proxyd::main,
        common::tests![
            command_line_matches,
            introspection_matches_apart_from_identity,
            version_names_this_crate,
            refuses_to_start_without_sockets,
            ignores_sockets_passed_to_another_process,
            refuses_datagram_and_non_listening_sockets,
            reports_ready_with_the_same_status,
            forwards_large_transfers_both_ways,
            half_close_reaches_the_backend,
            backend_closing_first_reaches_the_client,
            forwards_to_unix_sockets_by_path_and_abstract_name,
            listens_on_every_socket_passed_in,
            proxy_protocol_describes_ipv4_clients,
            proxy_protocol_describes_ipv6_clients,
            proxy_protocol_reports_unknown_for_unix_clients,
            refuses_connections_over_the_limit,
            raises_the_open_files_limit,
            unreachable_backend_closes_only_that_connection,
            unresolvable_backend_closes_only_that_connection,
            exits_when_idle_after_a_connection,
            idle_timer_restarts_after_the_last_connection,
            stops_cleanly_on_sigterm_and_sigint,
            moves_payload_with_splice_not_through_user_space
        ],
    )
}
