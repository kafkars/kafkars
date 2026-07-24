//! Concrete direct-consumer effect execution without public API ownership.

#[cfg(test)]
mod assigned_close_composition_test;
mod assigned_close_error;
mod assigned_close_slot;
#[cfg(test)]
mod assigned_close_slot_test;
mod assigned_timer_model;
mod assigned_timers;
#[cfg(test)]
mod assigned_timers_generation_test;
#[cfg(test)]
mod assigned_timers_identity_test;
#[cfg(test)]
mod assigned_timers_order_test;
#[cfg(test)]
mod assigned_timers_test;
mod assigned_topics;
#[cfg(test)]
mod assigned_topics_replacement_test;
#[cfg(test)]
mod assigned_topics_test;
mod fetch_execution;
mod fetch_store;
#[cfg(test)]
mod fetch_store_domain_test;
#[cfg(test)]
mod fetch_store_test;
mod position_execution;
#[cfg(test)]
mod position_execution_close_test;
#[cfg(test)]
mod position_execution_fence_test;
#[cfg(test)]
mod position_execution_ownership_test;
#[cfg(test)]
mod position_execution_test;
mod position_prepare_error;
#[cfg(test)]
mod position_prepare_error_test;
