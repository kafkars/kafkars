//! Strict bounded canonicalization for one API-77 response.

use core::mem::size_of;
use std::collections::BTreeSet;

use super::{
    DESCRIBE_SHARE_GROUP_DIAGNOSTIC_BYTES, DESCRIBE_SHARE_GROUP_MAX_ASSIGNMENT_TOPICS,
    DESCRIBE_SHARE_GROUP_MAX_MEMBERS, DESCRIBE_SHARE_GROUP_MAX_PARTITIONS_PER_TOPIC,
    DESCRIBE_SHARE_GROUP_MAX_RESPONSE_TEXT_BYTES, DESCRIBE_SHARE_GROUP_MAX_RETAINED_BYTES,
    DESCRIBE_SHARE_GROUP_MAX_SCALAR_BYTES, DESCRIBE_SHARE_GROUP_MAX_SUBSCRIPTIONS,
    DescribeShareGroupAssignment, DescribeShareGroupBrokerError, DescribeShareGroupDescription,
    DescribeShareGroupMember, DescribeShareGroupPlan, DescribeShareGroupResult,
    DescribeShareGroupTopicAssignment,
};

pub(super) enum ResponseValidation {
    Valid(DescribeShareGroupResult, usize, usize),
    TooLarge,
    Invalid,
}

pub(super) fn canonicalize_response(
    plan: &DescribeShareGroupPlan,
    result: DescribeShareGroupResult,
) -> ResponseValidation {
    let (throttle_time_ms, description) = result.into_parts();
    let (
        group_id,
        state,
        group_epoch,
        assignment_epoch,
        assignor_name,
        members,
        authorized_operations,
    ) = description.into_parts();
    if group_id != plan.group_id()
        || state.is_empty()
        || group_epoch < 0
        || assignment_epoch < 0
        || (!plan.include_authorized_operations() && authorized_operations.is_some())
    {
        return ResponseValidation::Invalid;
    }
    let mut charge = Charge::new();
    if !charge.scalar(&group_id) || !charge.scalar(&state) || !charge.scalar(&assignor_name) {
        return ResponseValidation::TooLarge;
    }
    if members.len() > DESCRIBE_SHARE_GROUP_MAX_MEMBERS
        || !charge.items::<DescribeShareGroupMember>(members.len())
    {
        return ResponseValidation::TooLarge;
    }

    let mut member_ids = BTreeSet::new();
    let mut canonical_members = Vec::with_capacity(members.len());
    for member in members {
        let Some(member) = canonical_member(member, &mut charge) else {
            return charge.failure();
        };
        if !member_ids.insert(member.member_id().to_owned()) {
            return ResponseValidation::Invalid;
        }
        canonical_members.push(member);
    }
    canonical_members.sort_unstable_by(|left, right| {
        left.member_id()
            .as_bytes()
            .cmp(right.member_id().as_bytes())
    });
    if !charge.within_limits() {
        return ResponseValidation::TooLarge;
    }
    let result = DescribeShareGroupResult::new(
        throttle_time_ms,
        DescribeShareGroupDescription::new(
            group_id,
            state,
            group_epoch,
            assignment_epoch,
            assignor_name,
            canonical_members,
            authorized_operations,
        ),
    );
    ResponseValidation::Valid(result, charge.text, charge.retained)
}

fn canonical_member(
    member: DescribeShareGroupMember,
    charge: &mut Charge,
) -> Option<DescribeShareGroupMember> {
    let (member_id, rack_id, member_epoch, client_id, client_host, subscriptions, assignment) =
        member.into_parts();
    if member_id.is_empty()
        || member_epoch < 0
        || rack_id.as_ref().is_some_and(String::is_empty)
        || !charge.scalar(&member_id)
        || !charge.optional_scalar(rack_id.as_deref())
        || !charge.scalar(&client_id)
        || !charge.scalar(&client_host)
    {
        charge.invalid = member_id.is_empty()
            || member_epoch < 0
            || rack_id.as_ref().is_some_and(String::is_empty);
        return None;
    }
    if subscriptions.len() > DESCRIBE_SHARE_GROUP_MAX_SUBSCRIPTIONS
        || !charge.items::<String>(subscriptions.len())
    {
        return None;
    }
    let mut unique_subscriptions = BTreeSet::new();
    for topic in &subscriptions {
        if topic.is_empty() || !charge.scalar(topic) {
            charge.invalid = topic.is_empty();
            return None;
        }
        if !unique_subscriptions.insert(topic.as_str()) {
            charge.invalid = true;
            return None;
        }
    }
    let mut subscriptions = subscriptions;
    subscriptions.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    let assignment = canonical_assignment(assignment, charge)?;
    Some(DescribeShareGroupMember::new(
        member_id,
        rack_id,
        member_epoch,
        client_id,
        client_host,
        subscriptions,
        assignment,
    ))
}

fn canonical_assignment(
    assignment: DescribeShareGroupAssignment,
    charge: &mut Charge,
) -> Option<DescribeShareGroupAssignment> {
    let topics = assignment.into_topics();
    if topics.len() > DESCRIBE_SHARE_GROUP_MAX_ASSIGNMENT_TOPICS
        || !charge.items::<DescribeShareGroupTopicAssignment>(topics.len())
    {
        return None;
    }
    let mut topic_ids = BTreeSet::new();
    let mut topic_names = BTreeSet::new();
    let mut canonical = Vec::with_capacity(topics.len());
    for topic in topics {
        let (topic_id, topic_name, partitions) = topic.into_parts();
        if topic_id == [0; 16] || topic_name.is_empty() || !charge.scalar(&topic_name) {
            charge.invalid = topic_id == [0; 16] || topic_name.is_empty();
            return None;
        }
        if !topic_ids.insert(topic_id) || !topic_names.insert(topic_name.clone()) {
            charge.invalid = true;
            return None;
        }
        if partitions.len() > DESCRIBE_SHARE_GROUP_MAX_PARTITIONS_PER_TOPIC
            || !charge.items::<i32>(partitions.len())
        {
            return None;
        }
        let mut partitions = partitions;
        if partitions.iter().any(|partition| *partition < 0) {
            charge.invalid = true;
            return None;
        }
        partitions.sort_unstable();
        if partitions.windows(2).any(|pair| pair[0] == pair[1]) {
            charge.invalid = true;
            return None;
        }
        canonical.push(DescribeShareGroupTopicAssignment::new(
            topic_id, topic_name, partitions,
        ));
    }
    canonical.sort_unstable_by(|left, right| {
        left.topic_id().cmp(right.topic_id()).then_with(|| {
            left.topic_name()
                .as_bytes()
                .cmp(right.topic_name().as_bytes())
        })
    });
    Some(DescribeShareGroupAssignment::new(canonical))
}

pub(super) fn broker_error_is_valid(error: &DescribeShareGroupBrokerError) -> bool {
    error
        .message()
        .is_none_or(|message| message.len() <= DESCRIBE_SHARE_GROUP_DIAGNOSTIC_BYTES)
        && (error.message().is_some() || !error.message_truncated())
}

struct Charge {
    text: usize,
    retained: usize,
    overflow: bool,
    invalid: bool,
}

impl Charge {
    const fn new() -> Self {
        Self {
            text: 0,
            retained: size_of::<DescribeShareGroupResult>(),
            overflow: false,
            invalid: false,
        }
    }

    fn scalar(&mut self, value: &str) -> bool {
        if value.len() > DESCRIBE_SHARE_GROUP_MAX_SCALAR_BYTES {
            self.overflow = true;
            return false;
        }
        self.text = match self.text.checked_add(value.len()) {
            Some(total) => total,
            None => {
                self.overflow = true;
                return false;
            }
        };
        self.retained = match self.retained.checked_add(value.len()) {
            Some(total) => total,
            None => {
                self.overflow = true;
                return false;
            }
        };
        self.within_limits()
    }

    fn optional_scalar(&mut self, value: Option<&str>) -> bool {
        value.is_none_or(|value| self.scalar(value))
    }

    fn items<T>(&mut self, count: usize) -> bool {
        self.retained = match count
            .checked_mul(size_of::<T>())
            .and_then(|bytes| self.retained.checked_add(bytes))
        {
            Some(total) => total,
            None => {
                self.overflow = true;
                return false;
            }
        };
        self.within_limits()
    }

    const fn within_limits(&self) -> bool {
        !self.overflow
            && self.text <= DESCRIBE_SHARE_GROUP_MAX_RESPONSE_TEXT_BYTES
            && self.retained <= DESCRIBE_SHARE_GROUP_MAX_RETAINED_BYTES
    }

    fn failure(&self) -> ResponseValidation {
        if self.invalid {
            ResponseValidation::Invalid
        } else {
            ResponseValidation::TooLarge
        }
    }
}
