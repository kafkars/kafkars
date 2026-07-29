//! Conservative `CreateTopics` retained-byte charge scenarios.

use super::retention::{
    RESULT_DIAGNOSTIC_BYTES_PER_TOPIC, create_topics_request_charge, result_fixed_charge,
};

#[test]
fn request_reservation_covers_fixed_results_and_explicit_diagnostics() {
    let topic_bytes = "orders".len();
    let request = create_topics_request_charge(1, 0, 0, 0, topic_bytes)
        .unwrap_or_else(|| panic!("small request charge fits"));
    let result = result_fixed_charge(1, topic_bytes)
        .and_then(|fixed| fixed.checked_add(RESULT_DIAGNOSTIC_BYTES_PER_TOPIC))
        .unwrap_or_else(|| panic!("small result charge fits"));

    assert!(request >= result);
}
