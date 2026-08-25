//! Nonzero broker-issued Kafka topic identity.

/// One nonzero Kafka topic UUID retained as exact wire bytes.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TopicUuid([u8; 16]);

impl TopicUuid {
    /// Validates exact Kafka UUID bytes without accepting the zero sentinel.
    pub const fn try_from_bytes(bytes: [u8; 16]) -> Option<Self> {
        let mut index = 0;
        while index < bytes.len() {
            if bytes[index] != 0 {
                return Some(Self(bytes));
            }
            index += 1;
        }
        None
    }

    /// Borrows the exact Kafka UUID bytes.
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Returns the exact Kafka UUID bytes.
    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }
}
