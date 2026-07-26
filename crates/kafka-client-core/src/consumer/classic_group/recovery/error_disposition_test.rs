//! Stage-specific Kafka rejection disposition evidence.

use super::{
    ClassicBrokerError, ClassicBrokerStage, ClassicCoordinatorRecovery,
    error_disposition::{ClassicErrorDisposition, disposition},
};

#[test]
fn join_rejoins_only_for_dynamic_v1_through_v3_recoverable_errors() {
    assert_rejoin(
        ClassicBrokerStage::Join,
        14,
        ClassicCoordinatorRecovery::Retain,
    );
    assert_rejoin(
        ClassicBrokerStage::Join,
        25,
        ClassicCoordinatorRecovery::Retain,
    );
    assert_rejoin(
        ClassicBrokerStage::Join,
        27,
        ClassicCoordinatorRecovery::Retain,
    );
    assert_rejoin(
        ClassicBrokerStage::Join,
        15,
        ClassicCoordinatorRecovery::Rediscover,
    );
    assert_rejoin(
        ClassicBrokerStage::Join,
        16,
        ClassicCoordinatorRecovery::Rediscover,
    );
    for code in [22, 23, 24, 26, 30, 35, 79, 81, 82, 1234] {
        assert_fatal(ClassicBrokerStage::Join, code);
    }
}

#[test]
fn sync_and_heartbeat_share_generation_fenced_rejoin_errors() {
    for stage in [ClassicBrokerStage::Sync, ClassicBrokerStage::Heartbeat] {
        for code in [14, 22, 25, 27] {
            assert_rejoin(stage, code, ClassicCoordinatorRecovery::Retain);
        }
        for code in [15, 16] {
            assert_rejoin(stage, code, ClassicCoordinatorRecovery::Rediscover);
        }
        for code in [30, 79, 82, -1, 1234] {
            assert_fatal(stage, code);
        }
    }
}

fn assert_rejoin(stage: ClassicBrokerStage, code: i16, expected: ClassicCoordinatorRecovery) {
    let error = broker_error(code);
    assert_eq!(
        disposition(stage, error),
        ClassicErrorDisposition::Rejoin(expected)
    );
}

fn assert_fatal(stage: ClassicBrokerStage, code: i16) {
    assert_eq!(
        disposition(stage, broker_error(code)),
        ClassicErrorDisposition::Fatal
    );
}

fn broker_error(code: i16) -> ClassicBrokerError {
    ClassicBrokerError::try_from_code(code).unwrap_or_else(|| panic!("nonzero code"))
}
