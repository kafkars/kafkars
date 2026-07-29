//! Metadata-quorum voter-addition builder surface tests.

use std::time::Duration;

use super::{AddRaftVoter, AddRaftVoterBuilder};

#[test]
fn builder_names_cluster_deadline_and_submission_controls() {
    let deadline_after: fn(AddRaftVoterBuilder, Duration) -> AddRaftVoterBuilder =
        AddRaftVoterBuilder::deadline_after;
    let ack_when_committed: fn(AddRaftVoterBuilder, bool) -> AddRaftVoterBuilder =
        AddRaftVoterBuilder::ack_when_committed;
    let submit: fn(AddRaftVoterBuilder) -> AddRaftVoter = AddRaftVoterBuilder::submit;

    let _ = (deadline_after, ack_when_committed, submit);
}
