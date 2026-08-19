//! Engine request identity and canonicalization tests.

use kafka_client_core::AlterReplicaLogDirsPlan;

use super::{AlterReplicaLogDirAssignment, AlterReplicaLogDirsRequest};

#[test]
fn request_preserves_caller_order_and_all_assignment_scalars() {
    let request = AlterReplicaLogDirsRequest::new(vec![
        assignment("orders", 2, 7, "/kafka-fast"),
        assignment("audit", 0, 3, "/kafka-capacity"),
    ])
    .canonicalize();
    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid plan: {error:?}"));
    assert_plan_order(&plan);
}

fn assert_plan_order(plan: &AlterReplicaLogDirsPlan) {
    let assignments = plan.assignments();

    assert_eq!(
        assignments[0].clone().into_parts(),
        (7, "orders".to_owned(), 2, "/kafka-fast".to_owned())
    );
    assert_eq!(
        assignments[1].clone().into_parts(),
        (3, "audit".to_owned(), 0, "/kafka-capacity".to_owned())
    );
}

fn assignment(
    topic: &str,
    partition: i32,
    broker_id: i32,
    target_path: &str,
) -> AlterReplicaLogDirAssignment {
    AlterReplicaLogDirAssignment::new(
        topic.to_owned(),
        partition,
        broker_id,
        target_path.to_owned(),
    )
}
