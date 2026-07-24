//! Fatal completion ownership freezes Fetch execution until driver shutdown.

use std::time::Duration;

use kafka_client_core::Moment;

use super::{
    DirectFetchExecutor, FetchExecutionError, FetchSubmission,
    settlement_test::{OUTPUT_BYTES, assignment, prepared},
};

#[test]
fn completion_corruption_freezes_every_surface_and_recovers_exact_owners() {
    let (effect, mut machine) = assignment();
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, OUTPUT_BYTES);
    let (request, output_bytes) = prepared(effect).into_parts_for_test();
    let fence = request.fence();
    executor
        .reserve_output_for_test(fence, output_bytes)
        .unwrap_or_else(|error| panic!("reserve output: {error:?}"));
    executor
        .tracked_calls_for_test()
        .install_consumed_completion_for_test(request);

    let error = executor
        .poll(&mut machine, Moment::from_tick(8))
        .err()
        .unwrap_or_else(|| panic!("completion corruption must fail"));
    let FetchExecutionError::Completion(observation) = error else {
        panic!("completion category");
    };
    assert_eq!(observation.fence(), fence);
    assert!(observation.is_consumed());
    assert_eq!(executor.retained(), (1, 1, OUTPUT_BYTES));
    assert!(matches!(
        executor.take_ready(),
        Err(FetchExecutionError::Faulted)
    ));

    let mut driver = crate::driver::DriverOwner::build(&crate::EngineConfig::new(vec![
        "127.0.0.1:1".to_owned(),
    ]))
    .unwrap_or_else(|error| panic!("build driver: {error}"));
    let submission = executor
        .submit(
            &driver,
            &mut machine,
            prepared(effect),
            Moment::from_tick(8),
        )
        .unwrap_or_else(|error| panic!("faulted admission: {error:?}"));
    let FetchSubmission::Unavailable(prepared) = submission else {
        panic!("faulted executor must return exact prepared ownership");
    };
    assert_eq!(prepared.fence(), fence);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown: {error}"));
    let recovery = executor.recover_after_driver_shutdown();
    assert!(recovery.had_fault());
    let (requests, completion) = recovery.into_driver_recovery().into_parts();
    assert_eq!(requests.len(), 1);
    assert_eq!(completion, Some(observation));
}
