//! Engine request adaptation tests for reassignment listing.

use kafka_client_core::{ListPartitionReassignmentsPlan, ListPartitionReassignmentsSelection};

use super::{ListPartitionReassignmentTarget, ListPartitionReassignmentsRequest};

#[test]
fn selected_and_all_active_remain_distinct_through_core_validation() {
    let Ok(selected) =
        ListPartitionReassignmentsRequest::selected(vec![ListPartitionReassignmentTarget::new(
            "orders".to_owned(),
            2,
        )])
        .canonicalize()
        .into_plan()
    else {
        panic!("selected plan expected");
    };
    assert!(matches!(
        selected.selection(),
        ListPartitionReassignmentsSelection::Selected(targets)
            if targets[0].topic() == "orders" && targets[0].partition() == 2
    ));
    let Ok(all_active) = ListPartitionReassignmentsRequest::all_active().into_plan() else {
        panic!("all-active plan expected");
    };
    assert_eq!(all_active, ListPartitionReassignmentsPlan::all_active());
}
