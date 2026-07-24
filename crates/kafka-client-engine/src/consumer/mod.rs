//! Concrete direct-consumer effect execution without public API ownership.

mod fetch_execution;
mod fetch_store;
#[cfg(test)]
mod fetch_store_domain_test;
#[cfg(test)]
mod fetch_store_test;
mod position_execution;
#[cfg(test)]
mod position_execution_fence_test;
#[cfg(test)]
mod position_execution_test;
