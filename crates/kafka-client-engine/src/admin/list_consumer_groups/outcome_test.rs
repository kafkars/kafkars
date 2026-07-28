//! Stable translation scenarios for merged groups and exact broker errors.

use core::num::NonZeroI16;

use kafka_client_core::{
    AdminConsumerGroupListing, AdminListConsumerGroupsBatch, AdminListConsumerGroupsBrokerError,
    AdminListConsumerGroupsTerminal,
};

use super::{ConsumerGroupListing, ListConsumerGroupsOutcome, outcome::translate_terminal};

#[test]
fn groups_and_exact_broker_errors_cross_losslessly() {
    let terminal = AdminListConsumerGroupsTerminal::Listed(AdminListConsumerGroupsBatch::new(
        8,
        vec![AdminConsumerGroupListing::new(
            "alpha".to_owned(),
            "consumer".to_owned(),
            Some("Stable".to_owned()),
            Some("classic".to_owned()),
        )],
        vec![AdminListConsumerGroupsBrokerError::new(
            9,
            NonZeroI16::new(-17).unwrap_or_else(|| panic!("nonzero")),
        )],
    ));
    let ListConsumerGroupsOutcome::Groups(batch) = translate_terminal(terminal) else {
        panic!("groups");
    };
    let (throttle, groups, errors) = batch.into_parts();
    assert_eq!(throttle, 8);
    assert_eq!(
        groups
            .into_iter()
            .next()
            .map(ConsumerGroupListing::into_parts),
        Some((
            "alpha".to_owned(),
            "consumer".to_owned(),
            Some("Stable".to_owned()),
            Some("classic".to_owned()),
        ))
    );
    assert_eq!(errors[0].into_parts(), (9, -17));
}
