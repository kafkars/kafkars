//! Closed integrity and capacity failures for the Fetch delivery store.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchStoreFailure {
    CountCapacity,
    ByteCapacity,
    AccountingOverflow,
    DuplicateFence,
    UnknownFence,
    InvalidState,
    ReservationMismatch,
    ReservationIdentityExhausted,
    AuthorizationIdentityExhausted,
    InvalidNextOffset,
    MissingThrottle,
    NextOffsetMismatch,
    NotDeliverable,
}
