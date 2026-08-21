//! Private `ShareGroup` offset-listing request tests.

use crate::{StartPosition, TopicPartition};

use super::ListShareGroupOffsetsAdminRequest;

#[test]
fn request_retains_all_or_selected_intent_until_submission() {
    let all = ListShareGroupOffsetsAdminRequest::all("workers".to_owned());
    assert!(format!("{all:?}").contains("All"));

    let selected =
        ListShareGroupOffsetsAdminRequest::all("workers".to_owned()).with_partitions(vec![
            TopicPartition::new("orders", 2),
            TopicPartition::new("audit", 1).start_at(StartPosition::Beginning),
        ]);
    assert!(format!("{selected:?}").contains("Selected"));
}
