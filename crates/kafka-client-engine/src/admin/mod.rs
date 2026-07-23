//! Concrete bounded `CreateTopics` ownership without a generic admin framework.

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "request reservation consumes the shared charge in the next milestone"
    )
)]
pub(crate) mod retention;

#[cfg(test)]
mod retention_test;
