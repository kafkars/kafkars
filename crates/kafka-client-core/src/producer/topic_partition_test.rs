//! Scenarios for normalized scalar leader facts and selection identities.

use super::LeaderEpoch;

#[test]
fn kafka_absent_leader_epoch_is_distinct_from_malformed_negative_values() {
    assert_eq!(LeaderEpoch::try_from_raw(-1), Ok(None));
    match LeaderEpoch::try_from_raw(-2) {
        Err(error) => assert_eq!(error.value(), -2),
        Ok(value) => panic!("malformed negative leader epoch was accepted as {value:?}"),
    }
    assert_eq!(
        LeaderEpoch::try_from_raw(0)
            .unwrap_or_else(|error| panic!("zero is a valid leader epoch: {error}"))
            .map(LeaderEpoch::get),
        Some(0)
    );
}
