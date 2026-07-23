//! Producer flush settlement after deterministic and damaged execution loss.

use kafka_client_core::{Deadline, Moment};

use crate::{ProducerDeliveryError, ProducerFlushError};

use super::super::{
    admission_test::{admit, record},
    host_limits_test::{start, valid_limits},
};

#[test]
fn deterministic_execution_stop_completes_flush_after_record_failure() {
    let mut host = start(valid_limits());
    let record = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );
    let flush = host
        .try_admit_flush(Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("flush should be accepted: {error:?}"));

    host.execution_unavailable(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("execution stop should settle: {error}"));

    assert!(matches!(
        record.into_delivery_observer().wait(),
        Err(ProducerDeliveryError::Failed(_))
    ));
    assert_eq!(flush.into_flush_observer().wait(), Ok(()));
}

#[test]
fn damaged_execution_stop_settles_flush_with_conservative_failure() {
    let mut host = start(valid_limits());
    let record = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );
    let flush = host
        .try_admit_flush(Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("flush should be accepted: {error:?}"));
    host.inject_terminal_interpretation_fault();

    assert!(host.execution_unavailable(Moment::from_tick(2)).is_err());
    assert!(matches!(
        record.into_delivery_observer().wait(),
        Err(ProducerDeliveryError::Failed(_))
    ));
    assert_eq!(
        flush.into_flush_observer().wait(),
        Err(ProducerFlushError::ExecutionUnavailable)
    );
    assert!(host.terminal_resources_empty());
}

#[test]
fn deterministic_execution_stop_completes_close_after_record_failure() {
    let mut host = start(valid_limits());
    let record = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );
    let close = host
        .try_admit_close(Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("close should be accepted: {error:?}"));

    host.execution_unavailable(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("execution stop should settle: {error}"));

    assert!(matches!(
        record.into_delivery_observer().wait(),
        Err(ProducerDeliveryError::Failed(_))
    ));
    assert_eq!(close.into_flush_observer().wait(), Ok(()));
}

#[test]
fn damaged_execution_stop_settles_close_with_conservative_failure() {
    let mut host = start(valid_limits());
    let record = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );
    let close = host
        .try_admit_close(Moment::from_tick(1))
        .unwrap_or_else(|error| panic!("close should be accepted: {error:?}"));
    host.inject_terminal_interpretation_fault();

    assert!(host.execution_unavailable(Moment::from_tick(2)).is_err());
    assert!(matches!(
        record.into_delivery_observer().wait(),
        Err(ProducerDeliveryError::Failed(_))
    ));
    assert_eq!(
        close.into_flush_observer().wait(),
        Err(ProducerFlushError::ExecutionUnavailable)
    );
    assert!(host.terminal_resources_empty());
}
