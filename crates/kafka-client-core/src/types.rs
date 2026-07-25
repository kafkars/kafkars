//! Small deterministic value types shared by semantic machines.

/// Stable identity for one public client operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OperationId(u64);

impl OperationId {
    /// Creates an identity from its deterministic raw value.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw deterministic value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Absolute monotonic nanosecond observation supplied by an effect interpreter.
///
/// Production maps the engine's monotonic clock into this unit. Deterministic
/// simulators use the same nanosecond unit without consulting an ambient clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Moment(u64);

impl Moment {
    /// Creates a moment from an absolute monotonic nanosecond tick.
    pub const fn from_tick(tick: u64) -> Self {
        Self(tick)
    }

    /// Returns the absolute monotonic nanosecond tick.
    pub const fn tick(self) -> u64 {
        self.0
    }

    /// Constructs an absolute deadline by checked addition.
    pub const fn checked_deadline_after(self, ticks: u64) -> Option<Deadline> {
        match self.0.checked_add(ticks) {
            Some(value) => Some(Deadline(value)),
            None => None,
        }
    }
}

/// Absolute monotonic nanosecond tick owned by an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Deadline(u64);

impl Deadline {
    /// Creates a deadline from an absolute monotonic nanosecond tick.
    pub const fn from_tick(tick: u64) -> Self {
        Self(tick)
    }

    /// Returns the absolute monotonic nanosecond tick.
    pub const fn tick(self) -> u64 {
        self.0
    }

    /// Returns whether this deadline has elapsed at the supplied observation.
    pub const fn is_elapsed_at(self, now: Moment) -> bool {
        self.0 <= now.0
    }

    /// Returns the earlier of two absolute deadlines.
    pub const fn min(self, other: Self) -> Self {
        if self.0 <= other.0 { self } else { other }
    }
}

/// Count of bytes retained on behalf of client work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteCount(u64);

impl ByteCount {
    /// Creates a byte count.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the number of bytes.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Adds two counts when the result is representable.
    pub const fn checked_add(self, other: Self) -> Option<Self> {
        match self.0.checked_add(other.0) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Subtracts one count when it does not exceed this count.
    pub const fn checked_sub(self, other: Self) -> Option<Self> {
        match self.0.checked_sub(other.0) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}
