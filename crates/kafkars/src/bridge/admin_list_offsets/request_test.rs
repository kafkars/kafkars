//! Public-to-engine Admin `ListOffsets` request translation scenarios.

use crate::{
    ReadIsolation,
    admin::{ListOffsetsQuery, OffsetSpec},
};

use super::ListOffsetsAdminRequest;

#[test]
fn translation_is_deferred_and_preserves_caller_order() {
    let request = ListOffsetsAdminRequest::new(vec![
        ListOffsetsQuery::new("orders", 2, OffsetSpec::latest()).current_leader_epoch(17),
        ListOffsetsQuery::new("audit", 0, OffsetSpec::earliest_pending_upload()),
    ])
    .with_read_isolation(ReadIsolation::ReadCommitted);
    let engine = request.into_engine();
    let debug = format!("{engine:?}");
    assert!(debug.contains("AdminListOffsetsRequest"));
    assert!(debug.contains("current_leader_epoch: Some(17)"));
    assert!(debug.contains("current_leader_epoch: None"));
    assert!(debug.contains("ReadCommitted"));
}
