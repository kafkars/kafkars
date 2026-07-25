//! Exact scalar ownership carried by one prepared classic Join.

use std::time::{Duration, Instant};

use kafka_client_core::{ClassicGroupTiming, ClassicProtocol, Deadline, GroupId, MembershipCycle};

use crate::clock::OperationDeadline;

use super::classic_group_join::PreparedClassicGroupJoin;

#[test]
fn prepared_join_preserves_every_core_and_transport_fence() {
    let group_id = GroupId::try_from_raw(7).unwrap_or_else(|| panic!("nonzero group identity"));
    let cycle = MembershipCycle::try_from_raw(11).unwrap_or_else(|| panic!("nonzero cycle"));
    let transport = Instant::now() + Duration::from_secs(2);
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(23), transport);
    let timing = ClassicGroupTiming::try_new(12_345, 54_321)
        .unwrap_or_else(|error| panic!("valid classic group timing: {error}"));

    let prepared =
        PreparedClassicGroupJoin::new(group_id, cycle, ClassicProtocol::Range, timing, deadline);

    assert_eq!(prepared.group_id(), group_id);
    assert_eq!(prepared.cycle(), cycle);
    assert_eq!(prepared.protocol(), ClassicProtocol::Range);
    assert_eq!(prepared.timing(), timing);
    assert_eq!(prepared.deadline().core(), Deadline::from_tick(23));
    assert_eq!(prepared.deadline().transport(), transport);
}
