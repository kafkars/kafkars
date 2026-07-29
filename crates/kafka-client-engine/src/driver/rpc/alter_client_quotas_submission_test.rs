//! Route and version bounds for Admin `AlterClientQuotas`.

use std::time::Instant;

use kafka_client_core::{
    AlterClientQuotaEntity, AlterClientQuotaEntityComponent, AlterClientQuotaEntry,
    AlterClientQuotaOperation, AlterClientQuotasPlan,
};
use kafka_driver::{ApiVersion, CompletionError, Route, TrafficClass};

use crate::{EngineConfig, driver::DriverOwner};

use super::{
    AlterClientQuotasCall,
    alter_client_quotas_submission::{alter_client_quotas_options, alter_client_quotas_route},
};

#[test]
fn client_quota_alterations_use_interactive_any_broker_v0_through_v1() {
    assert_eq!(alter_client_quotas_route(), Route::AnyBroker);

    let options = alter_client_quotas_options(Instant::now());
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(1)));
}

#[test]
fn completion_fault_retains_call_and_correlation_plan_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let expected = plan();
    let retained_limit = 4 * 1024 * 1024;
    let mut call = AlterClientQuotasCall::submit(
        &driver,
        expected.clone(),
        retained_limit,
        Instant::now() + std::time::Duration::from_secs(1),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|_call| panic!("completion fault must retain call and plan ownership"));
    assert!(recovered.matches(&expected, retained_limit));
    recovered.seal();
}

#[test]
fn synchronous_request_rejection_returns_exact_plan_and_capacity() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let expected = plan();
    let rejection = AlterClientQuotasCall::submit(
        &driver,
        expected.clone(),
        0,
        Instant::now() + std::time::Duration::from_secs(1),
    )
    .err()
    .unwrap_or_else(|| panic!("zero retained capacity must reject before driver ownership"));

    assert_eq!(rejection.into_correlation(), (expected, 0));
}

fn plan() -> AlterClientQuotasPlan {
    AlterClientQuotasPlan::new(
        vec![
            AlterClientQuotaEntry::new(
                AlterClientQuotaEntity::new(vec![AlterClientQuotaEntityComponent::new(
                    "user".to_owned(),
                    Some("alice".to_owned()),
                )]),
                vec![
                    AlterClientQuotaOperation::set("producer_byte_rate".to_owned(), 4096.0),
                    AlterClientQuotaOperation::remove("request_percentage".to_owned()),
                ],
            ),
            AlterClientQuotaEntry::new(
                AlterClientQuotaEntity::new(vec![AlterClientQuotaEntityComponent::new(
                    "client-id".to_owned(),
                    Some("orders".to_owned()),
                )]),
                vec![AlterClientQuotaOperation::set(
                    "consumer_byte_rate".to_owned(),
                    2048.0,
                )],
            ),
        ],
        true,
    )
    .unwrap_or_else(|error| panic!("valid alteration plan: {error}"))
}
