//! Linear normalized-input transfer between settled and pending values.

use kafka_client_core::{GroupOffsetCommitInput, OperationId};

use super::group_offset_commit_settlement::SettledGroupOffsetCommitCall;

#[test]
fn settled_input_moves_with_its_exact_pending_confirmation() {
    let operation_id = OperationId::from_raw(31);
    let settled = SettledGroupOffsetCommitCall::new(
        operation_id,
        GroupOffsetCommitInput::InvalidResponse,
        None,
    );
    assert_eq!(settled.operation_id(), operation_id);
    let (input, pending) = settled.into_parts();
    assert_eq!(input, GroupOffsetCommitInput::InvalidResponse);
    assert_eq!(pending.operation_id(), operation_id);
    let settled = pending.into_settled(input);
    assert_eq!(settled.operation_id(), operation_id);
}
