//! Exact stale-controller classification for retained election terminals.

use kafka_wire::ElectLeadersResponse;

use super::elect_leaders_terminal::response_requires_controller_refresh;

#[test]
fn only_supported_not_controller_responses_request_route_refresh() {
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

fn response(error_code: i16) -> Result<ElectLeadersResponse, kafka_driver::RequestError> {
    let mut response = ElectLeadersResponse::default();
    response.error_code = error_code;
    Ok(response)
}
