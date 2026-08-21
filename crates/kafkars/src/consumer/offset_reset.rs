//! Public policy for missing committed consumer positions.

/// Policy used when a group has no committed position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetReset {
    /// Return an error rather than silently choosing a position.
    Error,
    /// Begin at the earliest available offset.
    Earliest,
    /// Begin after the latest available offset.
    Latest,
}
