// SPDX-License-Identifier: LGPL-2.1-or-later

//! The command line: one table of options, from which parsing, `--help`
//! and `--introspect-cli` follow.

use std::fmt::Write;
use std::os::linux::net::SocketAddrExt;
use std::os::unix::net::SocketAddr as UnixSocketAddr;
use std::time::Duration;

use sd_event_futures::daemon;

pub const NAME: &str = "systemd-socket-proxyd";
pub const ABSTRACT: &str =
    "Bidirectionally proxy local sockets to another (possibly remote) socket.";
pub const POSTSCRIPT: &str = "See systemd.time(7) for the --exit-idle-time= time span format.";

#[derive(Copy, Clone)]
enum Flag {
    Help,
    Version,
    ConnectionsMax,
    ExitIdleTime,
    ProxyProtocol,
    IntrospectCli,
}

pub struct Opt {
    flag: Flag,
    pub short: Option<char>,
    pub long: &'static str,
    /// The value's placeholder, for options that take one.
    pub value: Option<&'static str>,
    /// `None` keeps the option out of `--help`.
    pub help: Option<&'static str>,
}

pub const OPTIONS: &[Opt] = &[
    Opt {
        flag: Flag::Help,
        short: Some('h'),
        long: "help",
        value: None,
        help: Some("Show this help"),
    },
    Opt {
        flag: Flag::Version,
        short: None,
        long: "version",
        value: None,
        help: Some("Show package version"),
    },
    Opt {
        flag: Flag::ConnectionsMax,
        short: Some('c'),
        long: "connections-max",
        value: Some("NUMBER"),
        help: Some("Set the maximum number of connections to be accepted"),
    },
    Opt {
        flag: Flag::ExitIdleTime,
        short: None,
        long: "exit-idle-time",
        value: Some("TIME"),
        help: Some("Exit when without a connection for this duration"),
    },
    Opt {
        flag: Flag::ProxyProtocol,
        short: None,
        long: "proxy-protocol",
        value: Some("v1"),
        help: Some("Enable PROXY protocol: v1"),
    },
    Opt {
        flag: Flag::IntrospectCli,
        short: None,
        long: "introspect-cli",
        value: None,
        help: None,
    },
];

pub enum Action {
    Help,
    Version,
    IntrospectCli,
    Run(Config),
}

pub struct Config {
    pub connections_max: u32,
    pub exit_idle_time: Option<Duration>,
    pub proxy_protocol: bool,
    pub remote: Remote,
}

pub enum Remote {
    Inet { node: String, service: String },
    Unix(UnixSocketAddr),
}

/// What the options set, before the remote address is known.
struct Settings {
    connections_max: u32,
    exit_idle_time: Option<Duration>,
    proxy_protocol: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            connections_max: 256,
            exit_idle_time: None,
            proxy_protocol: false,
        }
    }
}

/// Parse the arguments the way systemd's option parser does: options and
/// positional arguments may be mixed, long options may be abbreviated to
/// any unique prefix, `--` ends the options, and the first `--help`,
/// `--version` or error ends parsing.
pub fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Action, String> {
    let mut args = args.into_iter();
    let mut settings = Settings::default();
    let mut positional = Vec::new();
    while let Some(arg) = args.next() {
        let action = if arg == "--" {
            positional.extend(args.by_ref());
            break;
        } else if let Some(long) = arg.strip_prefix("--") {
            settings.long_option(long, &mut args)?
        } else if let Some(shorts) = arg.strip_prefix('-').filter(|s| !s.is_empty()) {
            settings.short_options(shorts, &mut args)?
        } else {
            positional.push(arg);
            None
        };
        if let Some(action) = action {
            return Ok(action);
        }
    }
    let [remote] = &positional[..] else {
        return Err(if positional.is_empty() {
            "Not enough parameters."
        } else {
            "Too many parameters."
        }
        .into());
    };
    Ok(Action::Run(Config {
        connections_max: settings.connections_max,
        exit_idle_time: settings.exit_idle_time,
        proxy_protocol: settings.proxy_protocol,
        remote: parse_remote(remote)?,
    }))
}

/// Another argument, for an option whose value isn't attached to it.
type Rest<'a> = &'a mut dyn Iterator<Item = String>;

impl Settings {
    /// `--name` or `--name=value`, where `name` may be a unique prefix.
    fn long_option(&mut self, arg: &str, rest: Rest<'_>) -> Result<Option<Action>, String> {
        let (name, value) = match arg.split_once('=') {
            Some((name, value)) => (name, Some(value.to_owned())),
            None => (arg, None),
        };
        let opt = find_long(name)?;
        if opt.value.is_none() && value.is_some() {
            return Err(format!(
                "{NAME}: option '--{}' doesn't allow an argument",
                opt.long
            ));
        }
        self.apply(opt, &format!("--{}", opt.long), value, rest)
    }

    /// A cluster like `-hc5`: flags, and possibly one option that takes
    /// the rest of the cluster, or else the next argument, as its value.
    fn short_options(&mut self, cluster: &str, rest: Rest<'_>) -> Result<Option<Action>, String> {
        for (i, c) in cluster.char_indices() {
            let opt = OPTIONS
                .iter()
                .find(|o| o.short == Some(c))
                .ok_or_else(|| format!("{NAME}: unrecognized option '-{c}'"))?;
            if opt.value.is_some() {
                let attached = &cluster[i + c.len_utf8()..];
                return self.apply(
                    opt,
                    &format!("-{c}"),
                    (!attached.is_empty()).then(|| attached.to_owned()),
                    rest,
                );
            }
            if let Some(action) = self.apply(opt, &format!("-{c}"), None, rest)? {
                return Ok(Some(action));
            }
        }
        Ok(None)
    }

    /// Apply `opt`, as spelled on the command line; `Some` if it ends
    /// parsing.
    fn apply(
        &mut self,
        opt: &Opt,
        spelled: &str,
        value: Option<String>,
        rest: Rest<'_>,
    ) -> Result<Option<Action>, String> {
        let value = || {
            value
                .or_else(|| rest.next())
                .ok_or_else(|| format!("{NAME}: option '{spelled}' requires an argument"))
        };
        match opt.flag {
            Flag::Help => return Ok(Some(Action::Help)),
            Flag::Version => return Ok(Some(Action::Version)),
            Flag::IntrospectCli => return Ok(Some(Action::IntrospectCli)),
            Flag::ConnectionsMax => {
                let value = value()?;
                self.connections_max = parse_unsigned(&value).ok_or_else(|| {
                    format!("Failed to parse --connections-max= argument: {value}")
                })?;
                if self.connections_max < 1 {
                    return Err("Connection limit is too low.".into());
                }
            }
            Flag::ExitIdleTime => {
                let value = value()?;
                self.exit_idle_time = daemon::parse_time_span(&value)
                    .map_err(|_| format!("Failed to parse --exit-idle-time= argument: {value}"))?;
            }
            Flag::ProxyProtocol => match value()?.as_str() {
                "v1" => self.proxy_protocol = true,
                other => {
                    return Err(format!(
                        "Failed to parse --proxy-protocol= argument: {other}"
                    ));
                }
            },
        }
        Ok(None)
    }
}

fn find_long(name: &str) -> Result<&'static Opt, String> {
    if let Some(exact) = OPTIONS.iter().find(|o| o.long == name) {
        return Ok(exact);
    }
    let mut candidates = OPTIONS.iter().filter(|o| o.long.starts_with(name));
    match (candidates.next(), candidates.next()) {
        (Some(only), None) => Ok(only),
        (None, _) => Err(format!("{NAME}: unrecognized option '--{name}'")),
        _ => Err(format!("{NAME}: option '--{name}' is ambiguous")),
    }
}

/// `safe_atou()`: decimal, an optional `+`, no sign otherwise, 32 bits.
fn parse_unsigned(s: &str) -> Option<u32> {
    let digits = s.strip_prefix('+').unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// `HOST:PORT` (port 80 if left out), or a Unix socket path or `@`name.
fn parse_remote(remote: &str) -> Result<Remote, String> {
    let unix = match remote.as_bytes() {
        [b'/', ..] => Some(UnixSocketAddr::from_pathname(remote)),
        [b'@', name @ ..] => Some(UnixSocketAddr::from_abstract_name(name)),
        _ => None,
    };
    if let Some(address) = unix {
        return address.map(Remote::Unix).map_err(|_| {
            "Specified address doesn't fit in an AF_UNIX address, refusing: File name too long"
                .into()
        });
    }
    let (node, service) = remote.rsplit_once(':').unwrap_or((remote, "80"));
    Ok(Remote::Inet {
        node: node.to_owned(),
        service: service.to_owned(),
    })
}

pub fn help() -> String {
    let rows: Vec<(String, &str)> = OPTIONS
        .iter()
        .filter_map(|o| {
            let short = o
                .short
                .map_or_else(|| "   ".to_owned(), |c| format!("-{c} "));
            let value = o.value.map(|v| format!("={v}")).unwrap_or_default();
            Some((format!("  {short}--{}{value}", o.long), o.help?))
        })
        .collect();
    let width = rows
        .iter()
        .map(|(names, _)| names.chars().count())
        .max()
        .unwrap_or(0)
        + 1;
    let mut text = format!(
        "> {NAME} [OPTION…] HOST:PORT\n> {NAME} [OPTION…] SOCKET\n\n{ABSTRACT}\n\nOptions:\n"
    );
    for (names, help) in rows {
        let mut lines = wrap(help, 80 - width).into_iter();
        let _ = writeln!(text, "{names:<width$}{}", lines.next().unwrap_or_default());
        for line in lines {
            let _ = writeln!(text, "{:width$}{line}", "");
        }
    }
    let _ = write!(
        text,
        "\n{POSTSCRIPT}\n\nSee the {NAME}(8) man page for details.\n"
    );
    text
}

/// Break `text` into lines of at most `width` characters, at spaces.
fn wrap(text: &str, width: usize) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    for word in text.split(' ') {
        match lines.last_mut() {
            Some(line) if line.len() + 1 + word.len() <= width => {
                line.push(' ');
                line.push_str(word);
            }
            _ => lines.push(word.to_owned()),
        }
    }
    lines
}

pub fn version() -> String {
    format!(
        "{NAME} ({} {})\n",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Action, String> {
        parse_args(args.iter().map(|a| a.to_string()))
    }

    fn config(args: &[&str]) -> Config {
        match parse(args) {
            Ok(Action::Run(config)) => config,
            Ok(_) => panic!("{args:?} didn't parse into a configuration"),
            Err(e) => panic!("{args:?}: {e}"),
        }
    }

    #[test]
    fn parses_options_anywhere_and_by_prefix() {
        let c = config(&[
            "host:8080",
            "--conn=5",
            "--exit=1min",
            "--proxy-protocol",
            "v1",
        ]);
        assert_eq!(c.connections_max, 5);
        assert_eq!(c.exit_idle_time, Some(Duration::from_secs(60)));
        assert!(c.proxy_protocol);
        assert!(
            matches!(c.remote, Remote::Inet { ref node, ref service } if node == "host" && service == "8080")
        );
        assert_eq!(config(&["-c5", "x"]).connections_max, 5);
        assert_eq!(config(&["-c", "+7", "x"]).connections_max, 7);
    }

    #[test]
    fn remote_defaults_to_port_80_and_takes_unix_sockets() {
        assert!(
            matches!(config(&["example.com"]).remote, Remote::Inet { ref service, .. } if service == "80")
        );
        assert!(
            matches!(config(&["/run/sock"]).remote, Remote::Unix(ref a) if a.as_pathname().is_some())
        );
        assert!(
            matches!(config(&["@sock"]).remote, Remote::Unix(ref a) if a.as_abstract_name() == Some(b"sock"))
        );
    }

    #[test]
    fn first_help_or_error_wins() {
        assert!(matches!(parse(&["-hc5"]), Ok(Action::Help)));
        assert!(matches!(
            parse(&["x", "--help", "--bogus"]),
            Ok(Action::Help)
        ));
        assert_eq!(
            parse(&["--bogus", "--help"]).err().unwrap(),
            "systemd-socket-proxyd: unrecognized option '--bogus'"
        );
        assert!(matches!(parse(&["--", "--help"]), Ok(Action::Run(_))));
    }

    #[test]
    fn rejects_bad_values() {
        for (args, message) in [
            (
                &["-c"][..],
                "systemd-socket-proxyd: option '-c' requires an argument",
            ),
            (
                &["--connections-max"],
                "systemd-socket-proxyd: option '--connections-max' requires an argument",
            ),
            (
                &["-c", "-1", "x"],
                "Failed to parse --connections-max= argument: -1",
            ),
            (
                &["-c", "4294967296", "x"],
                "Failed to parse --connections-max= argument: 4294967296",
            ),
            (&["-c", "0", "x"], "Connection limit is too low."),
            (
                &["--help=x"],
                "systemd-socket-proxyd: option '--help' doesn't allow an argument",
            ),
            (
                &["--exit-idle-time=", "x"],
                "Failed to parse --exit-idle-time= argument: ",
            ),
            (
                &["--proxy-protocol=V1", "x"],
                "Failed to parse --proxy-protocol= argument: V1",
            ),
            (&[], "Not enough parameters."),
            (&["a", "b"], "Too many parameters."),
        ] {
            assert_eq!(parse(args).err().as_deref(), Some(message), "{args:?}");
        }
    }
}
