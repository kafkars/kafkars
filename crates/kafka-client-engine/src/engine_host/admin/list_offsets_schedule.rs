//! Closed aggregation of Admin `ListOffsets` progress into the shared schedule.

use super::{
    list_offsets::AdminListOffsetsProgress, schedule::AdminProgress, schedule_deadline::earliest,
};

pub(super) const fn extend(progress: &mut AdminProgress, list_offsets: &AdminListOffsetsProgress) {
    progress.unsettled = progress.unsettled.saturating_add(list_offsets.unsettled);
    progress.driver_progress = progress.driver_progress || list_offsets.driver_progress;
    progress.next_deadline = earliest(progress.next_deadline, list_offsets.next_deadline);
}
