//! Ready and runtime-neutral cluster-description observation scenarios.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

use super::admin_describe_operation::AdminDescribeCluster;
use crate::{ErrorKind, KafkaError, admin::ClusterDescription};

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

#[test]
fn wait_and_future_share_one_concrete_result_shape() {
    let waited = AdminDescribeCluster::ready_for_test(Ok(ClusterDescription::new(
        String::from("cluster-a"),
        None,
        Vec::new(),
    )))
    .wait()
    .unwrap_or_else(|error| panic!("ready wait should succeed: {error}"));
    assert_eq!(waited.cluster_id(), "cluster-a");
    let mut operation = AdminDescribeCluster::ready_for_test(Err(KafkaError::new(
        ErrorKind::Backpressure,
        "cluster capacity is full",
    )));
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut operation).poll(&mut context),
        Poll::Ready(Err(error)) if error.kind() == ErrorKind::Backpressure
    ));
    assert!(matches!(
        Pin::new(&mut operation).poll(&mut context),
        Poll::Ready(Err(error)) if error.kind() == ErrorKind::State
    ));
}

#[test]
fn private_operation_is_send_without_runtime_dependencies() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<AdminDescribeCluster>();
}

#[test]
fn expected_cluster_identity_is_checked_for_wait_and_future() {
    let result = AdminDescribeCluster::ready_for_test(Ok(ClusterDescription::new(
        String::from("cluster-b"),
        None,
        Vec::new(),
    )))
    .with_expected_cluster_id(Some(Arc::from("cluster-a")))
    .wait();
    let Err(mismatch) = result else {
        panic!("a different broker-issued cluster ID must fail closed")
    };
    assert_eq!(mismatch.kind(), ErrorKind::Identity);
    assert!(mismatch.is_fatal());

    let mut matched = AdminDescribeCluster::ready_for_test(Ok(ClusterDescription::new(
        String::from("cluster-a"),
        None,
        Vec::new(),
    )))
    .with_expected_cluster_id(Some(Arc::from("cluster-a")));
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut matched).poll(&mut context),
        Poll::Ready(Ok(description)) if description.cluster_id() == "cluster-a"
    ));
}
