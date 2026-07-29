//! Conservative assigned-admin retained-byte charge scenarios.

use super::retention::{
    RESULT_DIAGNOSTIC_BYTES_PER_TOPIC, request_with_assignments_charge, result_fixed_charge,
};

#[test]
fn request_reservation_covers_fixed_results_and_explicit_diagnostics() {
    let topic_bytes = "orders".len();
    let request = request_with_assignments_charge(1, 0, 0, 0, topic_bytes)
        .unwrap_or_else(|| panic!("small request charge fits"));
    let result = result_fixed_charge(1, topic_bytes)
        .and_then(|fixed| fixed.checked_add(RESULT_DIAGNOSTIC_BYTES_PER_TOPIC))
        .unwrap_or_else(|| panic!("small result charge fits"));

    assert!(request >= result);
}

#[test]
fn explicit_assignment_headers_and_broker_ids_are_charged() {
    let automatic = request_with_assignments_charge(1, 0, 0, 0, "orders".len())
        .unwrap_or_else(|| panic!("automatic request charge fits"));
    let explicit = request_with_assignments_charge(1, 0, 2, 3, "orders".len())
        .unwrap_or_else(|| panic!("explicit request charge fits"));

    assert_eq!(explicit - automatic, 2 * 64 + 3 * 16);
}
