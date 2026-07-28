//! Linear call completion and post-driver recovery scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{AdminListOffsetSpec, AdminListOffsetTarget, ReadIsolation};
use kafka_driver::{ApiVersion, CompletionError};
use kafka_wire::ListOffsetsResponse;

use crate::{EngineConfig, driver::DriverOwner};

use super::{
    AdminListOffsetsCall, admin_list_offsets_terminal::retain_admin_list_offsets_terminal,
};

#[test]
fn completion_fault_retains_the_accepted_call_for_recovery() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let target = target();
    let mut call = AdminListOffsetsCall::submit(
        &driver,
        target.clone(),
        ReadIsolation::ReadUncommitted,
        1_000,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain accepted ownership"));
    assert!(recovered.matches_correlation(&target, ReadIsolation::ReadUncommitted));
    recovered.seal();
}

#[test]
fn synchronous_rejection_returns_exact_target_and_isolation() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let target = target();
    let rejection = match AdminListOffsetsCall::submit(
        &driver,
        target.clone(),
        ReadIsolation::ReadCommitted,
        -1,
        Instant::now() + Duration::from_secs(1),
    ) {
        Ok(_call) => panic!("negative timeout must reject before driver ownership"),
        Err(rejection) => rejection,
    };

    assert_eq!(
        rejection.into_correlation(),
        (target, ReadIsolation::ReadCommitted)
    );
}

#[test]
fn successful_raw_terminal_retains_exact_target_and_isolation() {
    let target = target();
    let raw = retain_admin_list_offsets_terminal(
        Some(ApiVersion::new(11)),
        Ok(ListOffsetsResponse::default()),
        None,
        target.clone(),
        ReadIsolation::ReadCommitted,
    );

    assert!(raw.matches_correlation(&target, ReadIsolation::ReadCommitted));
    raw.discard();
}

fn target() -> AdminListOffsetTarget {
    AdminListOffsetTarget::new("orders".to_owned(), 2, AdminListOffsetSpec::Latest)
}
