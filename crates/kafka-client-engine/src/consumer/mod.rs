//! Concrete direct-consumer effect execution without public API ownership.
#[cfg(test)]
mod assigned_close_composition_test;
mod assigned_close_error;
mod assigned_close_slot;
#[cfg(test)]
mod assigned_close_slot_test;
mod assigned_event;
#[cfg(test)]
mod assigned_event_test;
mod assigned_host;
mod assigned_owner;
mod assigned_owner_admission;
#[cfg(test)]
mod assigned_owner_admission_test;
mod assigned_owner_close;
#[cfg(test)]
mod assigned_owner_close_test;
mod assigned_owner_control;
#[cfg(test)]
mod assigned_owner_control_test;
mod assigned_owner_effect;
#[cfg(test)]
mod assigned_owner_effect_test;
mod assigned_owner_event;
#[cfg(test)]
mod assigned_owner_event_test;
mod assigned_owner_fault;
#[cfg(test)]
mod assigned_owner_fault_test;
mod assigned_owner_model;
#[cfg(test)]
mod assigned_owner_model_test;
mod assigned_owner_pending;
#[cfg(test)]
mod assigned_owner_pending_test;
mod assigned_owner_recovery;
#[cfg(test)]
mod assigned_owner_recovery_test;
mod assigned_owner_status;
#[cfg(test)]
mod assigned_owner_status_test;
#[cfg(test)]
mod assigned_owner_test;
mod assigned_owner_turn;
#[cfg(test)]
mod assigned_owner_turn_test;
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
mod exports;
mod fetch_execution;
mod fetch_store;
#[cfg(test)]
mod fetch_store_domain_test;
#[cfg(test)]
mod fetch_store_test;
mod group;
mod group_batch;
mod group_close;
mod group_commit;
mod group_event;
mod group_recv;
#[cfg(test)]
mod group_recv_test;
#[cfg(test)]
mod group_recv_test_support;
mod group_registration;
mod group_registration_engine;
mod group_registration_request;
#[cfg(test)]
mod group_registration_request_test;
mod group_release;
mod group_start;
mod position_execution;
mod position_prepare_error;
#[cfg(test)]
mod position_prepare_error_test;
pub use exports::*;
