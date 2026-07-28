//! Bounded configuration and exact-fencing classification scenarios.

use super::{TransactionPartitionEnrollmentFailureKind, TransactionPartitionEnrollmentLimits};

#[test]
fn limits_require_nonzero_partition_and_topic_byte_capacity() {
    assert_eq!(TransactionPartitionEnrollmentLimits::try_new(0, 1), None);
    assert_eq!(TransactionPartitionEnrollmentLimits::try_new(1, 0), None);
    assert_eq!(
        TransactionPartitionEnrollmentLimits::try_new(4, 64),
        Some(
            TransactionPartitionEnrollmentLimits::try_new(4, 64)
                .unwrap_or_else(|| panic!("valid limits")),
        )
    );
}

#[test]
fn only_exact_broker_fencing_is_fatal() {
    assert!(
        TransactionPartitionEnrollmentFailureKind::Broker {
            code: 90,
            fenced: true,
        }
        .is_fatal()
    );
    for kind in [
        TransactionPartitionEnrollmentFailureKind::InvalidResponse,
        TransactionPartitionEnrollmentFailureKind::DriverClosed,
        TransactionPartitionEnrollmentFailureKind::Broker {
            code: -731,
            fenced: false,
        },
    ] {
        assert!(!kind.is_fatal());
    }
}
