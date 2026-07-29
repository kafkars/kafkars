//! Metadata-quorum voter-addition builder surface tests.

use std::time::Duration;

use super::{AddRaftVoter, AddRaftVoterBuilder};

#[test]
fn builder_names_cluster_deadline_and_submission_controls() {
    let deadline_after: fn(AddRaftVoterBuilder, Duration) -> AddRaftVoterBuilder =
        AddRaftVoterBuilder::deadline_after;
    let submit: fn(AddRaftVoterBuilder) -> AddRaftVoter = AddRaftVoterBuilder::submit;

    let _ = (deadline_after, submit);
}
