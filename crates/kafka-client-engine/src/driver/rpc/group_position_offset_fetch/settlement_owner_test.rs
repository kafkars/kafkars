//! Exact begin, lossless restore, and receipt-consuming confirmation scenarios.

use super::{
    admission::GroupPositionOffsetFetchAccepted,
    calls::TrackedGroupPositionOffsetFetchCalls,
    calls_test::{fence, key},
    settlement::{
        GroupPositionOffsetFetchBeginError, GroupPositionOffsetFetchConfirmationError,
        GroupPositionOffsetFetchRestoreError,
    },
};

#[test]
fn raw_terminal_restores_and_confirms_only_against_the_exact_accepted_receipt() {
    let mut calls = TrackedGroupPositionOffsetFetchCalls::new(8);
    calls.install_terminal_for_test(
        key(7, 100),
        Some(9),
        Ok(kafka_wire::OffsetFetchResponse::default()),
    );
    let wrong = GroupPositionOffsetFetchAccepted::from_fence_for_test(fence(8));
    let error = calls
        .begin_group_position_offset_fetch_settlement(&wrong)
        .err()
        .unwrap_or_else(|| panic!("wrong fence must not begin settlement"));
    assert_eq!(
        error,
        GroupPositionOffsetFetchBeginError::FenceMismatch {
            settled: fence(7),
            supplied: fence(8),
        }
    );
    wrong.confirm_receipt();

    let accepted = GroupPositionOffsetFetchAccepted::from_fence_for_test(fence(7));
    let terminal = calls
        .begin_group_position_offset_fetch_settlement(&accepted)
        .unwrap_or_else(|error| panic!("begin exact settlement: {error:?}"));
    let wrong_confirmation = GroupPositionOffsetFetchAccepted::from_fence_for_test(fence(8));
    let failure = calls
        .confirm_group_position_offset_fetch_settlement(wrong_confirmation)
        .err()
        .unwrap_or_else(|| panic!("wrong receipt must not release confirmation"));
    let (wrong_confirmation, error) = failure.into_parts();
    assert_eq!(
        error,
        GroupPositionOffsetFetchConfirmationError::FenceMismatch {
            pending: fence(7),
            supplied: fence(8),
        }
    );
    wrong_confirmation.confirm_receipt();

    let mut other = TrackedGroupPositionOffsetFetchCalls::new(8);
    other.install_terminal_for_test(
        key(9, 200),
        Some(8),
        Ok(kafka_wire::OffsetFetchResponse::default()),
    );
    let other_accepted = GroupPositionOffsetFetchAccepted::from_fence_for_test(fence(9));
    let other_terminal = other
        .begin_group_position_offset_fetch_settlement(&other_accepted)
        .unwrap_or_else(|error| panic!("begin other settlement: {error:?}"));
    let failure = calls
        .restore_group_position_offset_fetch_settlement(other_terminal)
        .err()
        .unwrap_or_else(|| panic!("foreign terminal must return losslessly"));
    let (other_terminal, error) = failure.into_parts();
    assert_eq!(
        error,
        GroupPositionOffsetFetchRestoreError::FenceMismatch {
            pending: fence(7),
            supplied: fence(9),
        }
    );
    assert_eq!(other_terminal.key().fence(), fence(9));
    other
        .restore_group_position_offset_fetch_settlement(other_terminal)
        .unwrap_or_else(|failure| {
            let (_terminal, error) = failure.into_parts();
            panic!("restore other terminal: {error:?}");
        });
    let _other_terminal = other
        .begin_group_position_offset_fetch_settlement(&other_accepted)
        .unwrap_or_else(|error| panic!("re-begin other terminal: {error:?}"));
    other
        .confirm_group_position_offset_fetch_settlement(other_accepted)
        .unwrap_or_else(|failure| {
            let (_accepted, error) = failure.into_parts();
            panic!("confirm other receipt: {error:?}");
        });

    calls
        .restore_group_position_offset_fetch_settlement(terminal)
        .unwrap_or_else(|failure| {
            let (_terminal, error) = failure.into_parts();
            panic!("restore exact terminal: {error:?}");
        });
    let _terminal = calls
        .begin_group_position_offset_fetch_settlement(&accepted)
        .unwrap_or_else(|error| panic!("re-begin exact terminal: {error:?}"));
    calls
        .confirm_group_position_offset_fetch_settlement(accepted)
        .unwrap_or_else(|failure| {
            let (_accepted, error) = failure.into_parts();
            panic!("confirm exact receipt: {error:?}");
        });
    assert_eq!(calls.retained_group_position_offset_fetch_count(), 0);
}
