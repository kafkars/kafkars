//! Lossless core-to-engine member-removal terminal translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    ConsumerGroupMemberRemovalBrokerError as CoreBrokerError,
    ConsumerGroupMemberRemovalOutcome as CoreOutcome, RemoveConsumerGroupMembersBatch as CoreBatch,
    RemoveConsumerGroupMembersTerminal as CoreTerminal,
};

use super::{RemoveConsumerGroupMembersOutcome, outcome::translate_terminal};

#[test]
fn preserves_caller_order_throttle_and_signed_member_codes() {
    let Some(code) = NonZeroI16::new(-9) else {
        panic!("test code must be nonzero");
    };
    let terminal = CoreTerminal::Removed(CoreBatch::new(
        17,
        vec![
            CoreOutcome::removed(String::from("instance-b")),
            CoreOutcome::failed(String::from("instance-a"), CoreBrokerError::new(code)),
        ],
    ));

    let RemoveConsumerGroupMembersOutcome::Removed(batch) = translate_terminal(terminal) else {
        panic!("expected removed batch");
    };
    let (throttle_time_ms, members) = batch.into_parts();
    assert_eq!(throttle_time_ms, 17);
    let (first_id, first) = members[0].clone().into_parts();
    assert_eq!(first_id, "instance-b");
    assert_eq!(first, Ok(()));
    let (second_id, second) = members[1].clone().into_parts();
    assert_eq!(second_id, "instance-a");
    let Err(error) = second else {
        panic!("member error expected");
    };
    assert_eq!(error.code(), -9);
}
