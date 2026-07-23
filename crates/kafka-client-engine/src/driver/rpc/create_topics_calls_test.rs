//! Linear settled ownership for tracked `CreateTopics` calls.

use kafka_client_core::CreateTopicsInput;

use super::create_topics_calls::SettledCreateTopicsCall;

#[test]
fn settled_input_moves_once_while_route_authority_remains_owned() {
    let mut settled =
        SettledCreateTopicsCall::from_input_for_test(CreateTopicsInput::InvalidResponse);
    assert_eq!(
        settled.take_input(),
        Some(CreateTopicsInput::InvalidResponse)
    );
    assert_eq!(settled.take_input(), None);
}
