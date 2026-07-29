//! Exact stale-controller classification and one-shot refresh-barrier scenarios.

use kafka_client_core::{UpdateFeature, UpdateFeatureIntent, UpdateFeaturesPlan};
use kafka_driver::ApiVersion;
use kafka_wire::UpdateFeaturesResponse;

use crate::{EngineConfig, driver::DriverOwner};

use super::{
    update_features_call::UpdateFeaturesEvidence,
    update_features_terminal::{
        UpdateFeaturesControllerRefreshPoll, UpdateFeaturesTerminalFact,
        response_requires_controller_refresh, retain_update_features_terminal,
    },
};

#[test]
fn only_supported_top_level_not_controller_responses_request_refresh() {
    for selected_version in 0..=2 {
        assert!(response_requires_controller_refresh(
            Some(selected_version),
            &response(41),
        ));
    }
    assert!(!response_requires_controller_refresh(
        Some(2),
        &response(42)
    ));
    assert!(!response_requires_controller_refresh(Some(2), &response(0)));
    assert!(!response_requires_controller_refresh(None, &response(41)));
    assert!(!response_requires_controller_refresh(
        Some(-1),
        &response(41)
    ));
    assert!(!response_requires_controller_refresh(
        Some(3),
        &response(41)
    ));
}

#[test]
fn no_refresh_terminal_is_ready_without_driver_or_route_evidence() {
    let mut ordinary = terminal(2, 42);
    assert_eq!(
        ordinary.poll_controller_refresh(None),
        UpdateFeaturesControllerRefreshPoll::Ready
    );

    let mut missing_route_evidence = terminal(2, 41);
    assert_eq!(
        missing_route_evidence.poll_controller_refresh(None),
        UpdateFeaturesControllerRefreshPoll::Ready,
        "a broker code alone cannot forge an invalidation capability"
    );
}

#[test]
fn barrier_retains_known_terminal_through_driver_loss_and_completes_once() {
    let plan = plan();
    let mut terminal = terminal_with_plan(2, 41, plan.clone());
    terminal.arm_controller_refresh_for_test();

    for _attempt in 0..2 {
        assert_eq!(
            terminal.poll_controller_refresh(None),
            UpdateFeaturesControllerRefreshPoll::DriverMissing
        );
        assert!(terminal.matches_evidence(&plan, 4_096));
        let UpdateFeaturesTerminalFact::Response {
            selected_version,
            response,
        } = terminal.fact()
        else {
            panic!("known broker terminal must survive missing driver ownership");
        };
        assert_eq!(selected_version, Some(2));
        assert_eq!(response.error_code, 41);
    }

    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    assert_eq!(
        terminal.poll_controller_refresh(Some(&driver)),
        UpdateFeaturesControllerRefreshPoll::Pending
    );
    assert_eq!(
        terminal.poll_controller_refresh(Some(&driver)),
        UpdateFeaturesControllerRefreshPoll::Pending
    );
    assert_eq!(
        terminal.poll_controller_refresh(Some(&driver)),
        UpdateFeaturesControllerRefreshPoll::Ready
    );
    assert_eq!(
        terminal.poll_controller_refresh(Some(&driver)),
        UpdateFeaturesControllerRefreshPoll::Ready,
        "completed refresh authority cannot submit a second invalidation"
    );
    terminal.discard();
}

fn terminal(
    selected_version: i16,
    error_code: i16,
) -> super::update_features_terminal::UpdateFeaturesRawTerminal {
    terminal_with_plan(selected_version, error_code, plan())
}

fn terminal_with_plan(
    selected_version: i16,
    error_code: i16,
    plan: UpdateFeaturesPlan,
) -> super::update_features_terminal::UpdateFeaturesRawTerminal {
    retain_update_features_terminal(
        Some(ApiVersion::new(selected_version)),
        response(error_code),
        None,
        UpdateFeaturesEvidence::new(plan, 4_096),
    )
}

fn response(error_code: i16) -> Result<UpdateFeaturesResponse, kafka_driver::RequestError> {
    let mut response = UpdateFeaturesResponse::default();
    response.error_code = error_code;
    Ok(response)
}

fn plan() -> UpdateFeaturesPlan {
    UpdateFeaturesPlan::new(
        vec![UpdateFeature::new(
            "metadata.version".to_owned(),
            7,
            UpdateFeatureIntent::Upgrade,
        )],
        false,
    )
    .unwrap_or_else(|error| panic!("plan: {error}"))
}
