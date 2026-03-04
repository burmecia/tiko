//! PostgreSQL LSN wrapper and utilities.

use core::fmt;
use serde::{Deserialize, Serialize};

/// PostgreSQL WAL pointer type (`XLogRecPtr`) wrapped for type safety.
#[repr(transparent)]
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Lsn(u64);

impl Lsn {
    pub const INVALID: Self = Self(0);

    #[inline(always)]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[inline(always)]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Format as fixed-width uppercase hex (`{:016X}`), suitable for PITR/S3 key suffixes.
    #[inline]
    pub fn to_hex(self) -> String {
        format!("{:016X}", self.0)
    }

    /// Parse a fixed-width or variable-width hex representation of an LSN.
    pub fn from_hex(hex: &str) -> Result<Self, core::num::ParseIntError> {
        u64::from_str_radix(hex, 16).map(Self)
    }

    /// Format as PostgreSQL-style `X/Y` LSN string.
    #[inline]
    pub fn to_pg_string(self) -> String {
        format!("{:X}/{:X}", (self.0 >> 32) as u32, self.0 as u32)
    }
}

impl From<u64> for Lsn {
    #[inline(always)]
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Lsn> for u64 {
    #[inline(always)]
    fn from(value: Lsn) -> Self {
        value.0
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_pg_string())
    }
}

#[cfg(test)]
mod tests {
    use super::Lsn;

    #[test]
    fn to_hex_fixed_width() {
        assert_eq!(Lsn::new(0).to_hex(), "0000000000000000");
        assert_eq!(Lsn::new(0x3A000028).to_hex(), "000000003A000028");
        assert_eq!(Lsn::new(u64::MAX).to_hex(), "FFFFFFFFFFFFFFFF");
    }

    #[test]
    fn from_hex_round_trip() {
        let lsn = Lsn::from_hex("000000003A000028").unwrap();
        assert_eq!(lsn.as_u64(), 0x3A000028);
        assert_eq!(lsn.to_hex(), "000000003A000028");
    }

    #[test]
    fn pg_style_format() {
        assert_eq!(Lsn::new(0x000000003A000028).to_pg_string(), "0/3A000028");
    }
}
