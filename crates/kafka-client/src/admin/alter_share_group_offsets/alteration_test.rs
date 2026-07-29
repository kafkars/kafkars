//! Stable ShareGroup offset-alteration value tests.

use super::ShareGroupOffsetAlteration;

#[test]
fn alteration_preserves_exact_caller_intent_without_validation() {
    let alteration = ShareGroupOffsetAlteration::new("orders", -1, -2);

    assert_eq!(alteration.topic(), "orders");
    assert_eq!(alteration.partition(), -1);
    assert_eq!(alteration.start_offset(), -2);
    assert_eq!(alteration.into_parts(), ("orders".to_owned(), -1, -2),);
}
