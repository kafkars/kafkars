//! Linear storage mutation for a driver-shutdown recovery token.

use super::AlterConsumerGroupOffsetsOperation;
use crate::driver::RecoveredGroupOffsetAlterCall;

pub(super) fn retain(
    operation: &mut AlterConsumerGroupOffsetsOperation,
    recovered: Option<RecoveredGroupOffsetAlterCall>,
) {
    operation.recovered_call = recovered;
}

pub(super) fn take(
    operation: &mut AlterConsumerGroupOffsetsOperation,
) -> Option<RecoveredGroupOffsetAlterCall> {
    operation.recovered_call.take()
}
