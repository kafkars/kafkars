//! Generated-message adaptation for concrete Kafka admin operations.

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "create-topics execution joins the engine host in the next milestone"
    )
)]
pub(crate) mod create_topics;
mod result_budget;

#[cfg(test)]
mod create_topics_test;
#[cfg(test)]
mod result_budget_test;
