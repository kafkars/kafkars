//! Admin `ListOffsets` response-version evidence for leader-epoch fencing.

use kafka_client_core::{AdminListOffsetSpec, AdminListOffsetTarget, ReadIsolation};
use kafka_wire::{
    ListOffsetsResponse,
    list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
};

use super::{AdminListOffsetsResponseFailure, normalize_admin_list_offsets_response};

#[test]
fn selected_version_must_represent_current_leader_epoch_fence() {
    let target = AdminListOffsetTarget::new("audit".to_owned(), 3, AdminListOffsetSpec::Latest)
        .with_current_leader_epoch(Some(19));
    let generated = successful_response();

    assert_eq!(
        normalize_admin_list_offsets_response(
            &target,
            ReadIsolation::ReadUncommitted,
            3,
            &generated,
        ),
        Err(AdminListOffsetsResponseFailure::UnsupportedApiVersion { actual: 3 })
    );
    assert!(
        normalize_admin_list_offsets_response(
            &target,
            ReadIsolation::ReadUncommitted,
            4,
            &generated,
        )
        .is_ok()
    );
}

fn successful_response() -> ListOffsetsResponse {
    let mut partition = ListOffsetsPartitionResponse::default();
    partition.partition_index = 3;
    partition.offset = 1;

    let mut topic = ListOffsetsTopicResponse::default();
    topic.name = "audit".into();
    topic.partitions = vec![partition];

    let mut response = ListOffsetsResponse::default();
    response.topics = vec![topic];
    response
}
