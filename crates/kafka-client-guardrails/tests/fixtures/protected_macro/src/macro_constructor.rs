//! Opaque macro tokens cannot hide a protected constructor.

macro_rules! invoke {
    ($($tokens:tt)*) => {};
}

invoke! {
    PendingNotificationPermitPool::from_pending_permit_authority()
}
