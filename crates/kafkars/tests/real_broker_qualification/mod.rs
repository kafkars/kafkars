//! Declarative real-broker qualification scenario facade.

mod consume;
mod evidence;
mod smoke;
mod transaction;

pub(crate) use smoke::run_pull_request_smoke;
