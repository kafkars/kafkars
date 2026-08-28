//! Bounded-turn fairness under sustained producer pressure.

#[path = "runner_fairness_fixture.rs"]
mod fixture;

use self::fixture::FairnessFixture;
use super::runner::{HostTurnState, drive_host_turn};

const PRODUCER_TURN_BUDGET: usize = 64;
const PRODUCER_BACKLOG: usize = PRODUCER_TURN_BUDGET * 2 + 1;

impl FairnessFixture {
    fn prepare_producer_route(&mut self, state: &mut HostTurnState) {
        let submitted = drive_host_turn(self.resources(), state)
            .unwrap_or_else(|error| panic!("submit fairness route lookup: {error}"));
        assert_eq!(submitted.producer_admissions, 0);
        assert!(submitted.driver_turned);

        let Self {
            resources: Some(resources),
            broker,
            ..
        } = self
        else {
            panic!("fairness resources already finalized")
        };
        let driver = resources
            .driver
            .as_mut()
            .unwrap_or_else(|| panic!("fairness driver remains owned"));
        broker.install_topic(driver);

        let completed = drive_host_turn(resources, state)
            .unwrap_or_else(|error| panic!("complete fairness route lookup: {error}"));
        assert_eq!(completed.producer_admissions, 0);
        assert!(completed.driver_turned);
        assert!(
            completed.producer_completions_progressed,
            "the asynchronous route lookup must become ready before the measured turns",
        );
    }
}

#[test]
fn producer_saturation_does_not_starve_control_lanes() {
    let mut fixture = FairnessFixture::new(PRODUCER_BACKLOG);
    let producer_observers = fixture.prepare_producer_backlog(PRODUCER_BACKLOG);
    let mut state = HostTurnState::default();
    fixture.prepare_producer_route(&mut state);
    let admin_observer = fixture.admit_admin();
    let group = fixture.start_group_consumer();
    fixture.start_share_consumer();
    let transaction_observer = fixture.admit_transaction();
    let before = fixture.producer_stats();
    let driver_turns_before = fixture.driver_turns();

    let first = drive_host_turn(fixture.resources(), &mut state)
        .unwrap_or_else(|error| panic!("first saturated host turn: {error}"));
    let after_first = fixture.producer_stats();

    assert_eq!(first.producer_admissions, PRODUCER_TURN_BUDGET);
    assert_eq!(
        before.host.prepared_batches - after_first.host.prepared_batches,
        PRODUCER_TURN_BUDGET,
    );
    assert_eq!(
        after_first.host.prepared_batches,
        PRODUCER_BACKLOG - PRODUCER_TURN_BUDGET,
    );
    assert!(first.producer_unsettled >= PRODUCER_BACKLOG);
    assert!(
        first.admin_progressed,
        "admin must advance in the same turn"
    );
    assert!(
        first.group_consumer_progressed,
        "group membership must advance in the same turn",
    );
    assert!(
        first.share_consumer_progressed,
        "share membership must advance in the same turn",
    );
    assert!(
        first.transaction_progressed,
        "transaction control must advance in the same turn",
    );
    assert!(first.driver_turned);
    assert!(!first.should_terminate);
    assert_eq!(fixture.driver_turns(), driver_turns_before + 1,);

    fixture.script_one_producer_completion();
    let second = drive_host_turn(fixture.resources(), &mut state)
        .unwrap_or_else(|error| panic!("second saturated host turn: {error}"));
    assert_eq!(second.producer_admissions, PRODUCER_TURN_BUDGET);
    assert!(second.driver_turned);
    assert!(
        second.producer_completions_progressed,
        "one scripted terminal must be applied in the next exact host turn",
    );

    drop((
        producer_observers,
        admin_observer,
        group,
        transaction_observer,
    ));
}
