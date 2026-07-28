//! Bounded structural admission validation retaining caller ownership.

use super::{
    input::TransactionOffsetCommitRequest, model::TransactionOffsetCommitAdmissionErrorKind,
};

pub(super) fn validate_request(
    request: &TransactionOffsetCommitRequest,
    offset_limit: usize,
    retained_byte_limit: usize,
) -> Option<TransactionOffsetCommitAdmissionErrorKind> {
    let actual = request.offsets().len();
    if actual > offset_limit {
        return Some(TransactionOffsetCommitAdmissionErrorKind::OffsetCount {
            actual,
            limit: offset_limit,
        });
    }
    let Some(retained_bytes) = request.retained_bytes() else {
        return Some(TransactionOffsetCommitAdmissionErrorKind::RetainedBytes {
            actual: usize::MAX,
            limit: retained_byte_limit,
        });
    };
    if retained_bytes > retained_byte_limit {
        return Some(TransactionOffsetCommitAdmissionErrorKind::RetainedBytes {
            actual: retained_bytes,
            limit: retained_byte_limit,
        });
    }
    invalid_input(request).then_some(TransactionOffsetCommitAdmissionErrorKind::InvalidInput)
}

fn invalid_input(request: &TransactionOffsetCommitRequest) -> bool {
    let group = request.group();
    if group.group_id().is_empty()
        || group.generation_id() < 0
        || group.member_id().is_empty()
        || group.group_instance_id().is_some_and(str::is_empty)
        || request.offsets().is_empty()
    {
        return true;
    }
    request.offsets().iter().enumerate().any(|(index, offset)| {
        offset.topic().is_empty()
            || offset.partition() < 0
            || offset.next_offset() < 0
            || offset.leader_epoch().is_some_and(|epoch| epoch < 0)
            || request.offsets()[..index].iter().any(|previous| {
                previous.topic() == offset.topic() && previous.partition() == offset.partition()
            })
    })
}
