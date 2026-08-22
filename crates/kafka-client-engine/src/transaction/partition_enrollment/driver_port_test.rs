//! Exact broker-code normalization at the partition-enrollment driver seam.

use kafka_client_core::DeliveryStatus;

use super::{
    TransactionPartitionEnrollmentFailureKind, port::TransactionPartitionEnrollmentPortFact,
};

#[test]
fn only_exact_concurrent_transactions_normalizes_to_same_route_retry() {
    assert_eq!(
        normalized_fact(51, false),
        TransactionPartitionEnrollmentPortFact::RetryableConcurrentTransactions {
            kind: TransactionPartitionEnrollmentFailureKind::Broker {
                code: 51,
                fenced: false,
            },
            delivery: DeliveryStatus::PossiblySent,
        }
    );

    for code in [-31_000, 14, 15, 16, 50, 52] {
        assert_eq!(
            normalized_fact(code, false),
            TransactionPartitionEnrollmentPortFact::Failed {
                kind: TransactionPartitionEnrollmentFailureKind::Broker {
                    code,
                    fenced: false,
                },
                delivery: DeliveryStatus::PossiblySent,
            }
        );
    }
}

fn normalized_fact(
    error_code: i16,
    retry_safe_after_refresh: bool,
) -> TransactionPartitionEnrollmentPortFact {
    TransactionPartitionEnrollmentPortFact::broker_rejection(
        TransactionPartitionEnrollmentFailureKind::Broker {
            code: error_code,
            fenced: false,
        },
        retry_safe_after_refresh,
    )
}
