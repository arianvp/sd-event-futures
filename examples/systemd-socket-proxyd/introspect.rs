// SPDX-License-Identifier: LGPL-2.1-or-later

//! `--introspect-cli`: the command line described as JSON, in the format
//! `application/vnd.io.systemd.cli-introspection-0`.

use std::fmt::Write;

use super::cli::{ABSTRACT, NAME, OPTIONS, Opt, POSTSCRIPT};

pub fn introspect() -> String {
    let options: Vec<String> = OPTIONS.iter().map(option).collect();
    format!(
        concat!(
            r#"{{"mediaType":"application/vnd.io.systemd.cli-introspection-0","commands":[{{"#,
            r#""names":[{}],"project":{},"version":{},"features":[],"#,
            r#""abstract":[{}],"postscript":[{}],"options":[{}]}}]}}"#
        ),
        string(NAME),
        string(env!("CARGO_PKG_NAME")),
        string(env!("CARGO_PKG_VERSION")),
        string(ABSTRACT),
        string(POSTSCRIPT),
        options.join(","),
    )
}

fn option(opt: &Opt) -> String {
    let names: Vec<String> = opt
        .short
        .map(|c| format!("-{c}"))
        .into_iter()
        .chain([format!("--{}", opt.long)])
        .map(|n| string(&n))
        .collect();
    let argument = if opt.value.is_some() {
        "required_argument"
    } else {
        "no_argument"
    };
    let mut fields = vec![
        format!(r#""names":[{}]"#, names.join(",")),
        format!(r#""argument":{}"#, string(argument)),
    ];
    fields.extend(opt.value.map(|v| format!(r#""metavar":{}"#, string(v))));
    fields.extend(opt.help.map(|h| format!(r#""help":{}"#, string(h))));
    format!("{{{}}}", fields.join(","))
}

/// `s` as a JSON string.
fn string(s: &str) -> String {
    let mut out = String::from('"');
    for c in s.chars() {
        match c {
            '"' => out += "\\\"",
            '\\' => out += "\\\\",
            c if c < ' ' => {
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out + "\""
}
