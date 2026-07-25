//! Exact classic broker error code preservation evidence.

use super::ClassicBrokerError;

#[test]
fn success_is_reserved_while_known_and_future_rejections_round_trip() {
    assert_eq!(ClassicBrokerError::try_from_code(0), None);
    for code in [-1, 14, 79, i16::MIN, i16::MAX] {
        assert_eq!(
            ClassicBrokerError::try_from_code(code).map(ClassicBrokerError::code),
            Some(code)
        );
    }
}
