//! Linear storage mutation for a driver-shutdown recovery token.

use crate::driver::RecoveredRemoveConsumerGroupMembersCall;

use super::RemoveConsumerGroupMembersOperation;

pub(super) fn retain(
    operation: &mut RemoveConsumerGroupMembersOperation,
    recovered: RecoveredRemoveConsumerGroupMembersCall,
) {
    operation.recovered_call = Some(recovered);
}

pub(super) fn take(
    operation: &mut RemoveConsumerGroupMembersOperation,
) -> Option<RecoveredRemoveConsumerGroupMembersCall> {
    operation.recovered_call.take()
}
