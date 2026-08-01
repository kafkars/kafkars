//! Broker Fetch-session membership across assignment replacement.

use kafka_client_core::AssignedConsumerEffect;

use crate::protocol::fetch::FetchSessionUpdate;

use super::broker_session_test::{
    assignment, broker, established, fetch_fence, incremental, member,
};

#[test]
fn carried_partition_is_active_while_only_removed_partition_is_forgotten() {
    let (effects, _machine) = assignment();
    let broker = broker(3);
    let mut sessions = established(&effects, broker);
    for effect in effects.iter().copied() {
        let position = fetch_fence(effect).position();
        sessions.observe_control(AssignedConsumerEffect::Revoke {
            assignment_epoch: position.assignment_epoch(),
            partition: position.partition(),
        });
    }

    let carried = member(effects[1], "beta");
    let plan = sessions
        .try_begin(broker, vec![carried.clone()])
        .unwrap_or_else(|(error, _active)| panic!("replacement epoch: {error:?}"));
    assert_eq!(plan.active(), &[carried]);
    assert_eq!(plan.forgotten(), &[member(effects[0], "alpha")]);
    sessions
        .complete(plan, FetchSessionUpdate::Continue(incremental(91, 2)))
        .unwrap_or_else(|error| panic!("complete replacement epoch: {error:?}"));
    assert_eq!(sessions.retained(), (1, 1));
}
