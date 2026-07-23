//! Prepared request, admission, and engine-owned default timeout scenarios.

use std::time::Duration;

use kafka_client_engine::{Engine, EngineConfig};

use super::admin::{AdminEngine, AdminRequest};
use crate::{ErrorKind, NewTopic};

#[test]
fn prepared_empty_request_is_rejected_without_first_poll_work() {
    let engine = Engine::start(EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("start engine: {error}"));
    let admin = AdminEngine::new(engine.admin(), Duration::from_secs(7));
    let operation = admin.submit(
        AdminRequest::from_topics(std::iter::empty::<NewTopic>()),
        admin.default_timeout(),
    );

    let error = operation
        .wait()
        .err()
        .unwrap_or_else(|| panic!("empty request must fail before admission"));
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(admin.default_timeout(), Duration::from_secs(7));
}
