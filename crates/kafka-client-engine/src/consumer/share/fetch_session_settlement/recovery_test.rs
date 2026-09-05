//! Recovery-authority classification for share-session and transport failures.

use core::num::NonZeroI16;

use crate::driver::ShareFetchFailureKind;
use crate::protocol::consumer::share_fetch::ShareFetchPartitionRejection;
use kafka_client_core::{Deadline, Moment};

use super::{
    recovery::{
        ShareFetchResponseRecovery, broker_recovery, driver_recovery_authorized,
        replacement_deadline, response_recovery,
    },
    settlement_test::success,
};

#[test]
fn only_exact_share_session_codes_authorize_broker_session_replacement() {
    for recoverable in [122, 123] {
        assert!(
            broker_recovery(NonZeroI16::new(recoverable).unwrap_or_else(|| panic!("nonzero")))
                .is_some()
        );
    }
    for terminal in [1, 6, 16, 74, 121, 124, 133] {
        assert!(
            broker_recovery(NonZeroI16::new(terminal).unwrap_or_else(|| panic!("nonzero")))
                .is_none()
        );
    }
}

#[test]
fn only_exact_partition_session_and_routing_codes_authorize_replacement() {
    for (code, expected) in [
        (122, ShareFetchResponseRecovery::Session),
        (123, ShareFetchResponseRecovery::Session),
        (3, ShareFetchResponseRecovery::Route([7; 16])),
        (6, ShareFetchResponseRecovery::Route([7; 16])),
        (56, ShareFetchResponseRecovery::Route([7; 16])),
        (74, ShareFetchResponseRecovery::Route([7; 16])),
        (100, ShareFetchResponseRecovery::Route([7; 16])),
        (29, ShareFetchResponseRecovery::Terminal),
        (30, ShareFetchResponseRecovery::Terminal),
        (42, ShareFetchResponseRecovery::Terminal),
    ] {
        let mut response = success(Some(30_000));
        response.topics[0].partitions[0].rejection = Some(ShareFetchPartitionRejection {
            fetch_error: NonZeroI16::new(code),
            acknowledge_error: None,
            current_leader: None,
        });
        assert_eq!(response_recovery(&response), expected, "broker code {code}");
    }
}

#[test]
fn mixed_terminal_partition_error_prevents_retry_of_other_routing_errors() {
    let mut response = success(Some(30_000));
    response.topics[0].partitions[0].rejection = Some(ShareFetchPartitionRejection {
        fetch_error: NonZeroI16::new(6),
        acknowledge_error: NonZeroI16::new(29),
        current_leader: Some((2, 7)),
    });
    assert_eq!(
        response_recovery(&response),
        ShareFetchResponseRecovery::Terminal
    );
}

#[test]
fn routing_recovery_names_the_exact_rejected_kafka_topic() {
    let mut response = success(Some(30_000));
    response.topics[0].topic_id = [8; 16];
    response.topics[0].partitions[0].rejection = Some(ShareFetchPartitionRejection {
        fetch_error: NonZeroI16::new(6),
        acknowledge_error: None,
        current_leader: Some((3, 11)),
    });

    assert_eq!(
        response_recovery(&response),
        ShareFetchResponseRecovery::Route([8; 16])
    );
}

#[test]
fn fenced_leader_epoch_refreshes_only_the_exact_topic_without_masking_terminal_errors() {
    let mut response = success(Some(30_000));
    response.topics[0].topic_id = [9; 16];
    response.topics[0].partitions[0].rejection = Some(ShareFetchPartitionRejection {
        fetch_error: NonZeroI16::new(74),
        acknowledge_error: None,
        current_leader: Some((2, 1)),
    });
    assert_eq!(
        response_recovery(&response),
        ShareFetchResponseRecovery::Route([9; 16])
    );
    response.topics[0].partitions[0]
        .rejection
        .as_mut()
        .unwrap_or_else(|| panic!("partition rejection"))
        .acknowledge_error = NonZeroI16::new(29);
    assert_eq!(
        response_recovery(&response),
        ShareFetchResponseRecovery::Terminal
    );
}

#[test]
fn only_transport_or_deadline_with_route_evidence_can_refresh_a_fetch_route() {
    assert!(driver_recovery_authorized(ShareFetchFailureKind::Transport));
    assert!(driver_recovery_authorized(
        ShareFetchFailureKind::DeadlineElapsed
    ));
    for terminal in [
        ShareFetchFailureKind::Compatibility,
        ShareFetchFailureKind::DriverRejected,
        ShareFetchFailureKind::InvalidResponse,
        ShareFetchFailureKind::ResponseTooLarge,
    ] {
        assert!(!driver_recovery_authorized(terminal));
    }
}

#[test]
fn route_refresh_gets_one_fresh_bound_equal_to_the_configured_attempt_duration() {
    assert_eq!(
        replacement_deadline(
            Deadline::from_tick(35),
            Moment::from_tick(5),
            Moment::from_tick(40),
        ),
        Some(Deadline::from_tick(70))
    );
    assert_eq!(
        replacement_deadline(
            Deadline::from_tick(5),
            Moment::from_tick(5),
            Moment::from_tick(40),
        ),
        None
    );
}
