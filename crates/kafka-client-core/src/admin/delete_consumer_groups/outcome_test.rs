//! Terminal value scenarios for deterministic Admin `DeleteConsumerGroups`.

use core::num::NonZeroI16;

use super::{
    DeleteConsumerGroupsBatch, DeleteConsumerGroupsBrokerError, DeleteConsumerGroupsOutcome,
    DeleteConsumerGroupsResult,
};

#[test]
fn outcomes_preserve_group_order_and_exact_broker_code() {
    let code = NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("code is nonzero"));
    let batch = DeleteConsumerGroupsBatch::new(
        9,
        vec![
            DeleteConsumerGroupsOutcome::deleted("orders".to_owned()),
            DeleteConsumerGroupsOutcome::failed(
                "audit".to_owned(),
                DeleteConsumerGroupsBrokerError::with_bounded_message(
                    code,
                    Some("coordinator rejected deletion".to_owned()),
                    true,
                ),
            ),
        ],
    );

    assert_eq!(batch.outcomes()[0].group_id(), "orders");
    let DeleteConsumerGroupsResult::Failed(error) = batch.outcomes()[1].result() else {
        panic!("expected broker failure");
    };
    assert_eq!(error.code(), -31_999);
    assert_eq!(error.message(), Some("coordinator rejected deletion"));
    assert!(error.message_truncated());
}
