//! Native host-thread startup retains the reactor through terminal shutdown.

use std::time::Duration;

use crate::EngineConfig;

use super::thread_start;

#[test]
fn host_thread_owns_driver_from_build_through_shutdown() {
    let config = EngineConfig::new(vec!["127.0.0.1:1".to_owned()]);
    let validated = config
        .validate()
        .unwrap_or_else(|error| panic!("validate engine config: {error:?}"));
    let started = thread_start::start(&config, validated)
        .unwrap_or_else(|error| panic!("start host thread: {error}"));

    started.lifecycle.request(&started.control);

    assert!(started.lifecycle.wait_closed(Duration::from_secs(5)));
    assert!(started.lifecycle.closed_error().is_none());
}
