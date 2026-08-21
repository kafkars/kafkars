//! SCRAM credential-description result ownership and order tests.

use std::time::Duration;

use crate::{DeliveryStatus, ErrorKind, KafkaError, admin::BatchResult};

use super::{DescribeUserScramCredentialsResult, ScramCredentialInfo, ScramMechanism};

#[test]
fn throttle_order_metadata_and_exact_user_error_remain_explicit() {
    let result = DescribeUserScramCredentialsResult::new(
        Duration::from_millis(11),
        BatchResult::new(vec![
            (
                "alice".to_owned(),
                Ok(vec![ScramCredentialInfo::new(
                    ScramMechanism::SHA_256,
                    4096,
                )]),
            ),
            (
                "bob".to_owned(),
                Err(
                    KafkaError::new(ErrorKind::Broker, "credential lookup denied")
                        .with_broker_code(Some(-713))
                        .with_delivery_status(DeliveryStatus::PossiblySent)
                        .with_diagnostic_truncated(true),
                ),
            ),
        ]),
    );

    assert_eq!(result.throttle_time(), Duration::from_millis(11));
    assert_eq!(result.users().entries()[0].0, "alice");
    let info = result.users().entries()[0]
        .1
        .as_ref()
        .unwrap_or_else(|error| panic!("credential facts expected: {error}"))[0];
    assert_eq!(info.mechanism(), ScramMechanism::SHA_256);
    assert_eq!(info.iterations(), 4096);
    let error = result.users().entries()[1]
        .1
        .as_ref()
        .err()
        .unwrap_or_else(|| panic!("user-level broker error expected"));
    assert_eq!(error.broker_code(), Some(-713));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert!(error.diagnostic_truncated());
}
