//! Stable seek-position conversion tests.

use kafka_client_core::StartPosition;

use super::GroupConsumerSeekPosition;

#[test]
fn every_supported_position_maps_without_policy_loss() {
    assert_eq!(
        GroupConsumerSeekPosition::Beginning.try_into_core(),
        Some(StartPosition::Beginning)
    );
    assert_eq!(
        GroupConsumerSeekPosition::End.try_into_core(),
        Some(StartPosition::End)
    );
    assert!(matches!(
        GroupConsumerSeekPosition::Offset(17).try_into_core(),
        Some(StartPosition::Offset(offset)) if offset.get() == 17
    ));
    assert_eq!(GroupConsumerSeekPosition::Offset(-1).try_into_core(), None);
}
