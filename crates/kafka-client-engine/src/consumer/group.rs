//! Bounded classic-group session identity without membership execution.

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "awaiting private group-consumer integration")
)]
mod offset_commit;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "awaiting private group-consumer integration")
)]
mod registry;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "awaiting private group-consumer integration")
)]
mod registry_close;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "awaiting private group-consumer integration")
)]
mod registry_commit;
mod registry_entry;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "awaiting private group-consumer integration")
)]
mod registry_session;
mod session_catalog;
mod session_catalog_prepared;

#[cfg(test)]
mod registry_close_test;
#[cfg(test)]
mod registry_commit_test;
#[cfg(test)]
mod registry_entry_test;
#[cfg(test)]
mod registry_session_test;
#[cfg(test)]
mod registry_test;
#[cfg(test)]
mod registry_test_support;
#[cfg(test)]
mod session_catalog_identity_test;
#[cfg(test)]
mod session_catalog_prepared_test;
#[cfg(test)]
mod session_catalog_test;
