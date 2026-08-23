//! Validated Kafka broker identity for one `ShareFetch` session.

/// Validated nonnegative Kafka broker identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ShareFetchBrokerId(i32);

impl ShareFetchBrokerId {
    /// Accepts Kafka's nonnegative broker-ID domain.
    pub const fn try_from_raw(value: i32) -> Option<Self> {
        if value < 0 { None } else { Some(Self(value)) }
    }

    /// Returns the Kafka broker identifier.
    pub const fn get(self) -> i32 {
        self.0
    }
}
