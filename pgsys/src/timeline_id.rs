//! PostgreSQL Timeline ID wrapper and utilities.

use serde::{Deserialize, Serialize};
use std::fmt;

/// PostgreSQL Timeline ID type (`TimelineId`) wrapped for type safety.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TimelineId(u32);

impl TimelineId {
    pub const INVALID: Self = Self(0);

    #[inline(always)]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[inline(always)]
    pub const fn as_u32(self) -> u32 {
        self.0
    }

    /// Format as fixed-width uppercase hex (`{:08X}`).
    #[inline]
    pub fn to_hex(self) -> String {
        format!("{:08X}", self.0)
    }

    /// Format as variable-width uppercase hex (`{:X}`).
    #[inline]
    pub fn to_hex_variable_width(self) -> String {
        format!("{:X}", self.0)
    }

    /// Parse a fixed-width or variable-width hex representation of a TimelineId.
    pub fn from_hex(hex: &str) -> Result<Self, core::num::ParseIntError> {
        u32::from_str_radix(hex, 16).map(Self)
    }
}

impl Default for TimelineId {
    fn default() -> Self {
        Self(1)
    }
}

impl From<u32> for TimelineId {
    #[inline(always)]
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<TimelineId> for u32 {
    #[inline(always)]
    fn from(value: TimelineId) -> Self {
        value.0
    }
}

impl fmt::Display for TimelineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::TimelineId;

    #[test]
    fn to_hex_fixed_width() {
        assert_eq!(TimelineId::new(0).to_hex(), "00000000");
        assert_eq!(TimelineId::new(0x3A000028).to_hex(), "3A000028");
        assert_eq!(TimelineId::new(u32::MAX).to_hex(), "FFFFFFFF");
    }

    #[test]
    fn from_hex_round_trip() {
        let timeline_id = TimelineId::from_hex("3A000028").unwrap();
        assert_eq!(timeline_id.as_u32(), 0x3A000028);
        assert_eq!(timeline_id.to_hex(), "3A000028");
    }
}
