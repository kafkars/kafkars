//! Focused generated Fetch terminals installed by sibling unit evidence.

use bytes::Bytes;
use kafka_wire_core::Uuid;

use super::{PartitionFetchRequest, TrackedFetchCalls};

impl TrackedFetchCalls {
    #[expect(
        clippy::too_many_arguments,
        reason = "the test helper exposes every exact generated Fetch terminal field to focused scenarios"
    )]
    pub(crate) fn install_success_terminal_for_test(
        &mut self,
        request: PartitionFetchRequest,
        observed_at: kafka_client_core::Moment,
        selected_version: i16,
        session_id: i32,
        partition_index: Option<i32>,
        records: Option<Bytes>,
        partition_error_code: i16,
    ) {
        let mut response = kafka_wire::FetchResponse::default();
        response.session_id = session_id;
        if let Some(partition_index) = partition_index {
            let mut partition = kafka_wire::fetch_response::PartitionData::default();
            partition.partition_index = partition_index;
            partition.error_code = partition_error_code;
            partition.records = records;
            if selected_version == 16 {
                partition.high_watermark = 90;
                partition.last_stable_offset = 80;
                partition.log_start_offset = 4;
            }
            let mut topic = kafka_wire::fetch_response::FetchableTopicResponse::default();
            topic.topic = request.topic().into();
            if selected_version == 16 {
                topic.topic_id = Uuid::from_bytes(
                    request
                        .topic_id()
                        .unwrap_or_else(|| panic!("Fetch v16 fixture requires a topic UUID")),
                );
            }
            topic.partitions = vec![partition];
            response.responses = vec![topic];
        }
        self.install_terminal_result_for_test(
            request,
            observed_at,
            Some(selected_version),
            Ok(response),
        );
    }
}
