//! Fresh-time sequencing between destructive group-offset admin owners.

use kafka_client_core::Moment;

use super::{
    super::EngineHostError, delete_consumer_group_offsets::DeleteConsumerGroupOffsetsProgress,
};

pub(super) fn drive_group_offset_delete_then_capture_alter(
    delete_now: Moment,
    drive_delete: impl FnOnce(Moment) -> Result<DeleteConsumerGroupOffsetsProgress, EngineHostError>,
    capture_alter_now: impl FnOnce() -> Result<Moment, EngineHostError>,
) -> Result<(DeleteConsumerGroupOffsetsProgress, Moment), EngineHostError> {
    let delete = drive_delete(delete_now)?;
    let alter_now = capture_alter_now()?;
    Ok((delete, alter_now))
}
