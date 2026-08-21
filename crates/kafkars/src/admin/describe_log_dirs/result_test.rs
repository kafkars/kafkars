//! Caller-order and nested broker/log-directory error preservation tests.
#![expect(
    clippy::expect_used,
    reason = "the test asserts a nested broker rejection"
)]

use std::time::Duration;

use crate::{ErrorKind, KafkaError, admin::BatchResult};

use super::{DescribeLogDirsResult, LogDirDescription};

#[test]
fn broker_order_and_nested_log_directory_errors_remain_explicit() {
    let log_dirs = BatchResult::new(vec![
        (
            "/var/lib/kafka-a".to_owned(),
            Ok(LogDirDescription::new(None, None, None, Vec::new())),
        ),
        (
            "/var/lib/kafka-b".to_owned(),
            Err(
                KafkaError::new(ErrorKind::Broker, "Kafka rejected the log directory")
                    .with_broker_code(Some(56)),
            ),
        ),
    ]);
    let result = DescribeLogDirsResult::new(
        Duration::from_millis(9),
        BatchResult::new(vec![
            (7, Ok(log_dirs)),
            (
                2,
                Err(KafkaError::new(
                    ErrorKind::Transport,
                    "exact broker call failed",
                )),
            ),
        ]),
    );

    assert_eq!(result.throttle_time(), Duration::from_millis(9));
    assert_eq!(result.brokers().entries()[0].0, 7);
    assert_eq!(result.brokers().entries()[1].0, 2);
    let first = result.brokers().entries()[0]
        .1
        .as_ref()
        .unwrap_or_else(|error| panic!("first broker unexpectedly failed: {error}"));
    assert_eq!(first.entries()[0].0, "/var/lib/kafka-a");
    assert_eq!(
        first.entries()[1]
            .1
            .as_ref()
            .expect_err("second log directory must preserve its error")
            .broker_code(),
        Some(56)
    );
    assert_eq!(result.into_brokers().into_entries().len(), 2);
}
