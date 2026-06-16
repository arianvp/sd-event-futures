// SPDX-License-Identifier: LGPL-2.1-or-later

//! Just enough of NTP (RFC 5905) to read a server's clock, with the checks
//! timesyncd makes on a reply.

use std::fmt;
use std::io::{self, ErrorKind};
use std::time::{SystemTime, UNIX_EPOCH};

pub const PACKET_SIZE: usize = 48;
pub const PORT: u16 = 123;

/// Seconds from the NTP epoch (1900) to the Unix epoch (1970).
const UNIX_EPOCH_IN_NTP: u64 = 2_208_988_800;
const MODE_CLIENT: u8 = 3;
const MODE_SERVER: u8 = 4;
const LEAP_NOT_IN_SYNC: u8 = 3;

/// An NTP timestamp: seconds since 1900 as 32.32 fixed point.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Timestamp(pub u64);

impl Timestamp {
    pub fn from_system_time(time: SystemTime) -> Self {
        let since_unix = time.duration_since(UNIX_EPOCH).unwrap_or_default();
        let seconds = since_unix.as_secs() + UNIX_EPOCH_IN_NTP;
        let fraction = (u64::from(since_unix.subsec_nanos()) << 32) / 1_000_000_000;
        Self(seconds << 32 | fraction)
    }

    /// `self - earlier` in seconds; negative if `self` is earlier.
    fn seconds_since(self, earlier: Self) -> f64 {
        (i128::from(self.0) - i128::from(earlier.0)) as f64 / (1u64 << 32) as f64
    }

    fn read(packet: &[u8; PACKET_SIZE], at: usize) -> Self {
        Self(u64::from_be_bytes(packet[at..at + 8].try_into().unwrap()))
    }
}

/// A client request. Its transmit timestamp is `nonce` rather than the
/// time, like timesyncd sends, so that an off-path attacker can't guess
/// the origin timestamp a genuine reply must echo.
pub fn request(nonce: u64) -> [u8; PACKET_SIZE] {
    let mut packet = [0; PACKET_SIZE];
    packet[0] = 4 << 3 | MODE_CLIENT;
    packet[40..].copy_from_slice(&nonce.to_be_bytes());
    packet
}

/// What a reply says about the server's clock.
#[derive(Debug)]
pub struct Sample {
    /// How far the server's clock is ahead of ours, in seconds.
    pub offset: f64,
    /// The network round trip, in seconds.
    pub delay: f64,
    pub stratum: u8,
}

impl fmt::Display for Sample {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "clock offset {:+.1} ms, round trip {:.1} ms, stratum {}",
            self.offset * 1e3,
            self.delay * 1e3,
            self.stratum
        )
    }
}

/// Read the reply to a request with `nonce`, sent at `sent` and received
/// at `received`.
pub fn sample(
    reply: &[u8; PACKET_SIZE],
    nonce: u64,
    sent: SystemTime,
    received: SystemTime,
) -> io::Result<Sample> {
    let invalid = |why: &str| io::Error::new(ErrorKind::InvalidData, why);
    let (leap, version, mode) = (reply[0] >> 6, reply[0] >> 3 & 7, reply[0] & 7);
    let stratum = reply[1];
    if Timestamp::read(reply, 24).0 != nonce {
        return Err(invalid("Invalid reply; not our transmit time."));
    }
    if leap == LEAP_NOT_IN_SYNC || stratum == 0 || stratum >= 16 {
        return Err(invalid("Server is not synchronized."));
    }
    if !matches!(version, 3 | 4) || mode != MODE_SERVER {
        return Err(invalid("Not an NTPv3 or NTPv4 server reply."));
    }
    // T1 to T4 in RFC 5905's terms.
    let origin = Timestamp::from_system_time(sent);
    let receive = Timestamp::read(reply, 32);
    let transmit = Timestamp::read(reply, 40);
    let destination = Timestamp::from_system_time(received);
    Ok(Sample {
        offset: (receive.seconds_since(origin) + transmit.seconds_since(destination)) / 2.0,
        delay: destination.seconds_since(origin) - transmit.seconds_since(receive),
        stratum,
    })
}

/// The reply a server synchronized at `stratum` sends to `request` when
/// its clock says `now`, for a server to test against.
pub fn reply(request: &[u8; PACKET_SIZE], stratum: u8, now: SystemTime) -> [u8; PACKET_SIZE] {
    let now = Timestamp::from_system_time(now).0.to_be_bytes();
    let mut packet = [0; PACKET_SIZE];
    packet[0] = 4 << 3 | MODE_SERVER;
    packet[1] = stratum;
    packet[24..32].copy_from_slice(&request[40..48]);
    packet[32..40].copy_from_slice(&now);
    packet[40..48].copy_from_slice(&now);
    packet
}
