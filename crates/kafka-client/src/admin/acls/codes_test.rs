//! ACL exact-code vocabulary and binding-versus-filter validation tests.

use super::{AclOperation, AclPatternType, AclPermissionType, AclResourceType};

#[test]
fn named_constants_retain_kafka_signed_codes() {
    assert_eq!(AclResourceType::USER.code(), 7);
    assert_eq!(AclPatternType::PREFIXED.code(), 4);
    assert_eq!(AclOperation::TWO_PHASE_COMMIT.code(), 15);
    assert_eq!(AclPermissionType::ALLOW.code(), 3);
}

#[test]
fn future_codes_round_trip_without_collapsing_to_unknown() {
    assert_eq!(AclResourceType::from_code(91).code(), 91);
    assert_eq!(AclPatternType::from_code(92).code(), 92);
    assert_eq!(AclOperation::from_code(93).code(), 93);
    assert_eq!(AclPermissionType::from_code(94).code(), 94);

    assert!(AclResourceType::from_code(91).is_valid_for_binding());
    assert!(AclPatternType::from_code(92).is_valid_for_binding());
    assert!(AclOperation::from_code(93).is_valid_for_binding());
    assert!(AclPermissionType::from_code(94).is_valid_for_binding());
}

#[test]
fn filter_only_sentinels_are_rejected_from_concrete_bindings() {
    assert!(!AclResourceType::ANY.is_valid_for_binding());
    assert!(AclResourceType::ANY.is_valid_for_filter());

    assert!(!AclPatternType::MATCH.is_valid_for_binding());
    assert!(AclPatternType::MATCH.is_valid_for_filter());

    assert!(!AclOperation::ANY.is_valid_for_binding());
    assert!(AclOperation::ANY.is_valid_for_filter());

    assert!(!AclPermissionType::ANY.is_valid_for_binding());
    assert!(AclPermissionType::ANY.is_valid_for_filter());
}

#[test]
fn unknown_and_negative_codes_are_invalid_but_still_preserved_exactly() {
    assert!(!AclResourceType::UNKNOWN.is_valid_for_filter());
    assert!(!AclPatternType::UNKNOWN.is_valid_for_filter());
    assert!(!AclOperation::UNKNOWN.is_valid_for_filter());
    assert!(!AclPermissionType::UNKNOWN.is_valid_for_filter());

    assert_eq!(AclResourceType::from_code(-8).code(), -8);
    assert!(!AclResourceType::from_code(-8).is_valid_for_binding());
    assert!(!AclPatternType::from_code(-9).is_valid_for_binding());
    assert!(!AclOperation::from_code(-10).is_valid_for_binding());
    assert!(!AclPermissionType::from_code(-11).is_valid_for_binding());
}
