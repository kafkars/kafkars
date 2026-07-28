//! Lifecycle scenarios for deterministic Admin `DeleteRecords` ownership.

use crate::{Deadline, OperationId};

use super::{DeleteRecordsMachine, DeleteRecordsPlan, DeleteRecordsState, DeleteRecordsTarget};

#[test]
fn accepted_machine_begins_ready_with_first_target_visible() {
    let machine = DeleteRecordsMachine::new(
        OperationId::from_raw(7),
        Deadline::from_tick(99),
        DeleteRecordsPlan::new(vec![DeleteRecordsTarget::new("orders".to_owned(), 2, 91)])
            .unwrap_or_else(|error| panic!("valid plan: {error}")),
    );

    assert_eq!(machine.state(), DeleteRecordsState::Ready);
    assert_eq!(
        machine.current_target().map(DeleteRecordsTarget::partition),
        Some(2)
    );
}
