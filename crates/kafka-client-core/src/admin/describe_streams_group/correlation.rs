//! Strict bounded canonicalization for one API-89 response.

mod topology;
mod value;

use core::mem::size_of;

use super::{
    DESCRIBE_STREAMS_GROUP_DIAGNOSTIC_BYTES, DESCRIBE_STREAMS_GROUP_MAX_COLLECTION_ITEMS,
    DESCRIBE_STREAMS_GROUP_MAX_RESPONSE_TEXT_BYTES, DESCRIBE_STREAMS_GROUP_MAX_RETAINED_BYTES,
    DESCRIBE_STREAMS_GROUP_MAX_SCALAR_BYTES, DescribeStreamsGroupBrokerError,
    DescribeStreamsGroupDescription, DescribeStreamsGroupMember, DescribeStreamsGroupPlan,
    DescribeStreamsGroupResult, DescribeStreamsGroupTopologyDescriptionStatus,
};
use topology::canonical_topology_description;
use value::{
    canonical_assignment, canonical_endpoint, canonical_key_values, canonical_task_offsets,
    canonical_topology,
};

pub(super) struct CanonicalResponse {
    pub(super) result: DescribeStreamsGroupResult,
    pub(super) text_bytes: usize,
    pub(super) retained_bytes: usize,
}

pub(super) enum ResponseValidation {
    Valid(CanonicalResponse),
    TooLarge,
    Invalid,
}

pub(super) fn canonicalize_response(
    plan: &DescribeStreamsGroupPlan,
    result: DescribeStreamsGroupResult,
) -> ResponseValidation {
    let (throttle_time_ms, description) = result.into_parts();
    let (
        group_id,
        state,
        group_epoch,
        assignment_epoch,
        topology,
        members,
        authorized_operations,
        topology_description,
        topology_description_status,
    ) = description.into_parts();
    if group_id != plan.group_id()
        || state.is_empty()
        || group_epoch < 0
        || assignment_epoch < 0
        || (!plan.include_authorized_operations() && authorized_operations.is_some())
        || !valid_topology_description_pair(
            plan.include_topology_description(),
            topology_description.is_some(),
            topology_description_status,
        )
    {
        return ResponseValidation::Invalid;
    }

    let mut charge = Charge::new();
    if !charge.scalar(&group_id) || !charge.scalar(&state) {
        return ResponseValidation::TooLarge;
    }
    let topology = match topology {
        Some(topology) => match canonical_topology(topology, &mut charge) {
            Some(topology) => Some(topology),
            None => return charge.failure(),
        },
        None => None,
    };
    if !charge.items::<DescribeStreamsGroupMember>(members.len()) {
        return charge.failure();
    }
    let mut members = members;
    for member in &mut members {
        let Some(canonical) = canonical_member(member.clone(), &mut charge) else {
            return charge.failure();
        };
        *member = canonical;
    }
    members.sort_unstable_by(|left, right| {
        left.member_id()
            .as_bytes()
            .cmp(right.member_id().as_bytes())
    });
    if members
        .windows(2)
        .any(|pair| pair[0].member_id() == pair[1].member_id())
    {
        return ResponseValidation::Invalid;
    }
    let topology_description = match topology_description {
        Some(description) => match canonical_topology_description(description, &mut charge) {
            Some(description) => Some(description),
            None => return charge.failure(),
        },
        None => None,
    };
    if !charge.within_limits() {
        return ResponseValidation::TooLarge;
    }
    ResponseValidation::Valid(CanonicalResponse {
        result: DescribeStreamsGroupResult::new(
            throttle_time_ms,
            DescribeStreamsGroupDescription::new(
                group_id,
                state,
                group_epoch,
                assignment_epoch,
                topology,
                members,
                authorized_operations,
                topology_description,
                topology_description_status,
            ),
        ),
        text_bytes: charge.text,
        retained_bytes: charge.retained,
    })
}

fn canonical_member(
    member: DescribeStreamsGroupMember,
    charge: &mut Charge,
) -> Option<DescribeStreamsGroupMember> {
    let (
        member_id,
        member_epoch,
        instance_id,
        rack_id,
        client_id,
        client_host,
        topology_epoch,
        process_id,
        user_endpoint,
        client_tags,
        task_offsets,
        task_end_offsets,
        assignment,
        target_assignment,
        is_classic,
    ) = member.into_parts();
    if member_id.is_empty()
        || member_epoch < 0
        || topology_epoch < 0
        || process_id.is_empty()
        || instance_id.as_ref().is_some_and(String::is_empty)
        || rack_id.as_ref().is_some_and(String::is_empty)
    {
        charge.invalid = true;
        return None;
    }
    for scalar in [&member_id, &client_id, &client_host, &process_id] {
        if !charge.scalar(scalar) {
            return None;
        }
    }
    if !charge.optional_scalar(instance_id.as_deref())
        || !charge.optional_scalar(rack_id.as_deref())
    {
        return None;
    }
    let user_endpoint = match user_endpoint {
        Some(endpoint) => Some(canonical_endpoint(endpoint, charge)?),
        None => None,
    };
    Some(DescribeStreamsGroupMember::new(
        member_id,
        member_epoch,
        instance_id,
        rack_id,
        client_id,
        client_host,
        topology_epoch,
        process_id,
        user_endpoint,
        canonical_key_values(client_tags, charge)?,
        canonical_task_offsets(task_offsets, charge)?,
        canonical_task_offsets(task_end_offsets, charge)?,
        canonical_assignment(assignment, charge)?,
        canonical_assignment(target_assignment, charge)?,
        is_classic,
    ))
}

const fn valid_topology_description_pair(
    requested: bool,
    present: bool,
    status: Option<DescribeStreamsGroupTopologyDescriptionStatus>,
) -> bool {
    let Some(status) = status else {
        return !requested && !present;
    };
    if !requested {
        return !present && status.raw() == 0;
    }
    if status.raw() == 0 {
        return false;
    }
    match status.raw() {
        0..=2 => !present,
        3 => present,
        _ => true,
    }
}

pub(super) fn broker_error_charge(
    group_id: &str,
    error: &DescribeStreamsGroupBrokerError,
) -> Option<(usize, usize)> {
    if error
        .message()
        .is_some_and(|message| message.len() > DESCRIBE_STREAMS_GROUP_DIAGNOSTIC_BYTES)
        || (error.message().is_none() && error.message_truncated())
    {
        return None;
    }
    let diagnostic_bytes = error.message().map_or(0, str::len);
    let text_bytes = group_id.len().checked_add(diagnostic_bytes)?;
    Some((text_bytes, text_bytes))
}

pub(super) struct Charge {
    text: usize,
    retained: usize,
    overflow: bool,
    invalid: bool,
}

impl Charge {
    const fn new() -> Self {
        Self {
            text: 0,
            retained: size_of::<DescribeStreamsGroupResult>(),
            overflow: false,
            invalid: false,
        }
    }

    pub(super) fn scalar(&mut self, value: &str) -> bool {
        if value.len() > DESCRIBE_STREAMS_GROUP_MAX_SCALAR_BYTES {
            self.overflow = true;
            return false;
        }
        self.text = if let Some(total) = self.text.checked_add(value.len()) {
            total
        } else {
            self.overflow = true;
            return false;
        };
        self.retained = if let Some(total) = self.retained.checked_add(value.len()) {
            total
        } else {
            self.overflow = true;
            return false;
        };
        self.within_limits()
    }

    pub(super) fn optional_scalar(&mut self, value: Option<&str>) -> bool {
        value.is_none_or(|value| self.scalar(value))
    }

    pub(super) fn items<T>(&mut self, count: usize) -> bool {
        if count > DESCRIBE_STREAMS_GROUP_MAX_COLLECTION_ITEMS {
            self.overflow = true;
            return false;
        }
        self.retained = if let Some(total) = count
            .checked_mul(size_of::<T>())
            .and_then(|bytes| self.retained.checked_add(bytes))
        {
            total
        } else {
            self.overflow = true;
            return false;
        };
        self.within_limits()
    }

    pub(super) fn partition_items(&mut self, count: usize) -> bool {
        if count > super::DESCRIBE_STREAMS_GROUP_MAX_PARTITIONS_PER_TASK {
            self.overflow = true;
            return false;
        }
        self.retained = if let Some(total) = count
            .checked_mul(size_of::<i32>())
            .and_then(|bytes| self.retained.checked_add(bytes))
        {
            total
        } else {
            self.overflow = true;
            return false;
        };
        self.within_limits()
    }

    pub(super) const fn within_limits(&self) -> bool {
        !self.overflow
            && self.text <= DESCRIBE_STREAMS_GROUP_MAX_RESPONSE_TEXT_BYTES
            && self.retained <= DESCRIBE_STREAMS_GROUP_MAX_RETAINED_BYTES
    }

    fn failure(&self) -> ResponseValidation {
        if self.invalid {
            ResponseValidation::Invalid
        } else {
            ResponseValidation::TooLarge
        }
    }
}
