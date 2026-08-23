//! Declarative real-broker qualification scenario facade.

#[path = "consume.rs"]
mod consume;
#[path = "evidence.rs"]
mod evidence;
#[path = "nightly.rs"]
mod nightly;
#[path = "nightly_admin.rs"]
mod nightly_admin;
#[path = "nightly_consumer.rs"]
mod nightly_consumer;
#[path = "nightly_control.rs"]
mod nightly_control;
#[path = "nightly_group.rs"]
mod nightly_group;
#[path = "nightly_kip848.rs"]
mod nightly_kip848;
#[path = "nightly_producer.rs"]
mod nightly_producer;
#[path = "nightly_resilience.rs"]
mod nightly_resilience;
#[path = "nightly_resources.rs"]
mod nightly_resources;
#[path = "nightly_share.rs"]
mod nightly_share;
#[path = "nightly_support.rs"]
mod nightly_support;
#[path = "nightly_transaction.rs"]
mod nightly_transaction;
#[path = "share.rs"]
mod share;
#[path = "smoke.rs"]
mod smoke;
#[path = "transaction.rs"]
mod transaction;

pub(crate) use nightly::{run_classic_matrix, run_nightly_matrix};
pub(crate) use nightly_share::run_share_matrix;
pub(crate) use smoke::run_pull_request_smoke;
