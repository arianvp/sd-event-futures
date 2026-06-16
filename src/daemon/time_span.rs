//! Time spans in the format of `systemd.time(7)`.

use std::io::{self, ErrorKind};
use std::time::Duration;

/// Parse a time span in the format of `systemd.time(7)`, as `parse_sec()`
/// does: "5", "1.5h", "2min 30s", "500ms", … with seconds as the default
/// unit. `infinity` is `None`.
pub fn parse_time_span(text: &str) -> io::Result<Option<Duration>> {
    const SEC: u64 = 1_000_000;
    // Tried in this order, so longer names win over their prefixes.
    const UNITS: &[(&str, u64)] = &[
        ("seconds", SEC),
        ("second", SEC),
        ("sec", SEC),
        ("s", SEC),
        ("minutes", 60 * SEC),
        ("minute", 60 * SEC),
        ("min", 60 * SEC),
        ("months", 2_629_800 * SEC),
        ("month", 2_629_800 * SEC),
        ("M", 2_629_800 * SEC),
        ("msec", 1_000),
        ("ms", 1_000),
        ("m", 60 * SEC),
        ("hours", 3_600 * SEC),
        ("hour", 3_600 * SEC),
        ("hr", 3_600 * SEC),
        ("h", 3_600 * SEC),
        ("days", 86_400 * SEC),
        ("day", 86_400 * SEC),
        ("d", 86_400 * SEC),
        ("weeks", 604_800 * SEC),
        ("week", 604_800 * SEC),
        ("w", 604_800 * SEC),
        ("years", 31_557_600 * SEC),
        ("year", 31_557_600 * SEC),
        ("y", 31_557_600 * SEC),
        ("usec", 1),
        ("us", 1),
        ("\u{3bc}s", 1),
        ("\u{b5}s", 1),
    ];
    let invalid = || {
        io::Error::new(
            ErrorKind::InvalidInput,
            format!("Invalid time span {text:?}"),
        )
    };

    let mut rest = text.trim_start();
    if let Some(after) = rest.strip_prefix("infinity") {
        return if after.trim().is_empty() {
            Ok(None)
        } else {
            Err(invalid())
        };
    }
    let mut usec: u64 = 0;
    let mut something = false;
    loop {
        rest = rest.trim_start();
        if rest.is_empty() {
            return if something {
                Ok(Some(Duration::from_micros(usec)))
            } else {
                Err(invalid())
            };
        }
        let number = rest.strip_prefix('+').unwrap_or(rest);
        let digits = |s: &str| s.bytes().take_while(u8::is_ascii_digit).count();
        let (whole, after) = number.split_at(digits(number));
        let (fraction, after) = match after.strip_prefix('.') {
            // "3." and "3.s" are not numbers, ".5" is.
            Some(f) if digits(f) > 0 => f.split_at(digits(f)),
            Some(_) => return Err(invalid()),
            None if whole.is_empty() => return Err(invalid()),
            None => ("", after),
        };
        let spaced = after.trim_start();
        let (multiplier, after_unit) = UNITS
            .iter()
            .find_map(|&(unit, m)| Some((m, spaced.strip_prefix(unit)?)))
            .unwrap_or((SEC, spaced));
        // A number must be followed by a unit, whitespace or the end: no "12.34.56".
        if after_unit.len() == after.len() && !after.is_empty() {
            return Err(invalid());
        }
        rest = after_unit;

        let whole: u64 = if whole.is_empty() {
            0
        } else {
            whole.parse().map_err(|_| invalid())?
        };
        usec = whole
            .checked_mul(multiplier)
            .and_then(|k| usec.checked_add(k))
            .ok_or_else(invalid)?;
        let mut place = multiplier / 10;
        for digit in fraction.bytes() {
            usec = usec
                .checked_add(u64::from(digit - b'0') * place)
                .ok_or_else(invalid)?;
            place /= 10;
        }
        something = true;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::parse_time_span;

    #[test]
    fn parses_time_spans() {
        let ms = Duration::from_millis;
        for (text, expected) in [
            ("5", Some(Duration::from_secs(5))),
            ("5s", Some(Duration::from_secs(5))),
            ("1.5h", Some(Duration::from_secs(5400))),
            ("2min 30s", Some(Duration::from_secs(150))),
            ("2min30s", Some(Duration::from_secs(150))),
            ("500ms", Some(ms(500))),
            ("500 ms", Some(ms(500))),
            (".5", Some(ms(500))),
            ("1m", Some(Duration::from_secs(60))),
            ("1M", Some(Duration::from_secs(2_629_800))),
            ("1y 12month", Some(Duration::from_secs(31_557_600 * 2))),
            ("3us", Some(Duration::from_micros(3))),
            ("3\u{b5}s", Some(Duration::from_micros(3))),
            ("  infinity ", None),
        ] {
            assert_eq!(parse_time_span(text).unwrap(), expected, "{text:?}");
        }
    }

    #[test]
    fn rejects_bad_time_spans() {
        for bad in [
            "",
            " ",
            "-5",
            "3.",
            "3.s",
            "12.34.56",
            "5x",
            "infinity5",
            "99999999999999999999",
        ] {
            assert!(parse_time_span(bad).is_err(), "{bad:?} accepted");
        }
    }
}
