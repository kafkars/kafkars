//! Bounded classic-group session identity without membership execution.

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "awaiting private group-consumer integration")
)]
mod offset_commit;
mod session_catalog;
mod session_catalog_prepared;

#[cfg(test)]
mod session_catalog_identity_test;
#[cfg(test)]
mod session_catalog_prepared_test;
#[cfg(test)]
mod session_catalog_test;
