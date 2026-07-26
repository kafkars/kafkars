//! Exact plan ownership across the engine-host to driver handoff boundary.

use std::time::{Duration, Instant};

use kafka_client_core::{AlterConsumerGroupOffsetTarget, AlterConsumerGroupOffsetsPlan};

use crate::{
    EngineConfig,
    driver::{DriverOwner, GroupOffsetAlterCall},
};

#[test]
fn driver_handoff_retains_the_complete_caller_ordered_plan() {
    let Ok(plan) = AlterConsumerGroupOffsetsPlan::new(
        "payments".to_owned(),
        vec![
            AlterConsumerGroupOffsetTarget::new(
                "orders".to_owned(),
                2,
                91,
                Some(7),
                Some("checkpoint-a".to_owned()),
            ),
            AlterConsumerGroupOffsetTarget::new("audit".to_owned(), 0, 42, None, None),
        ],
    ) else {
        panic!("fixture must be a valid alteration plan");
    };
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = GroupOffsetAlterCall::submit(
        &driver,
        plan.clone(),
        usize::MAX,
        8_192,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|error| panic!("accepted alteration call: {error}"));

    assert!(call.matches_evidence(&plan, usize::MAX, 8_192));
    drop(driver);
    call.recover_after_driver_shutdown()
        .unwrap_or_else(|_call| panic!("accepted call remains recoverable"))
        .seal();
}
