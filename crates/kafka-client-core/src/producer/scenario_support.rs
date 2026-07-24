//! Test-only identity and retry scenario support.

#[path = "idempotence_test_support.rs"]
pub(super) mod idempotence;
#[path = "test_support.rs"]
mod installed_identity;
#[path = "retry_test_support.rs"]
pub(super) mod retry;
