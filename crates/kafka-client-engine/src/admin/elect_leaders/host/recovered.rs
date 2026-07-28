//! Linear storage mutation for a driver-shutdown recovery token.

use crate::driver::RecoveredElectLeadersCall;

use super::ElectLeadersOperation;

pub(super) fn retain(
    operation: &mut ElectLeadersOperation,
    recovered: Option<RecoveredElectLeadersCall>,
) {
    operation.recovered_call = recovered;
}

pub(super) fn take(operation: &mut ElectLeadersOperation) -> Option<RecoveredElectLeadersCall> {
    operation.recovered_call.take()
}
