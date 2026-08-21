//! Metadata-quorum voter-removal builder surface tests.

use std::time::Duration;

use super::{RemoveRaftVoter, RemoveRaftVoterBuilder};

#[test]
fn builder_names_cluster_deadline_and_submission_controls() {
    let deadline_after: fn(RemoveRaftVoterBuilder, Duration) -> RemoveRaftVoterBuilder =
        RemoveRaftVoterBuilder::deadline_after;
    let submit: fn(RemoveRaftVoterBuilder) -> RemoveRaftVoter = RemoveRaftVoterBuilder::submit;

    let _ = (deadline_after, submit);
}
