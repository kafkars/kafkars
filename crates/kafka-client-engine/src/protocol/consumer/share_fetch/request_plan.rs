//! Canonical full assignment and incremental `ShareFetch` session changes.

use super::{SHARE_FETCH_MAX_PARTITIONS, SHARE_FETCH_MAX_TOPICS, ShareFetchRequestFailure};

/// One canonical UUID topic and ordered partition set.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ShareFetchRequestTopic {
    pub(super) topic_id: [u8; 16],
    pub(super) partitions: Vec<u32>,
}

impl ShareFetchRequestTopic {
    pub(crate) fn try_new(
        topic_id: [u8; 16],
        mut partitions: Vec<u32>,
    ) -> Result<Self, ShareFetchRequestFailure> {
        validate_topic(topic_id, &partitions)?;
        partitions.sort_unstable();
        Ok(Self {
            topic_id,
            partitions,
        })
    }
}

/// Full correlation plus incremental session changes for one request.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ShareFetchRequestPlan {
    active: Vec<ShareFetchRequestTopic>,
    included: Vec<ShareFetchRequestTopic>,
    forgotten: Vec<ShareFetchRequestTopic>,
}

impl ShareFetchRequestPlan {
    pub(crate) fn try_new(
        mut active: Vec<ShareFetchRequestTopic>,
        mut included: Vec<ShareFetchRequestTopic>,
        mut forgotten: Vec<ShareFetchRequestTopic>,
    ) -> Result<Self, ShareFetchRequestFailure> {
        canonicalize_topics(&mut active)?;
        canonicalize_topics(&mut included)?;
        canonicalize_topics(&mut forgotten)?;
        for topic in &included {
            if !topic
                .partitions
                .iter()
                .all(|partition| contains_partition(&active, topic.topic_id, *partition))
            {
                return Err(ShareFetchRequestFailure::IncludedPartitionNotActive);
            }
        }
        for topic in &forgotten {
            if topic
                .partitions
                .iter()
                .any(|partition| contains_partition(&active, topic.topic_id, *partition))
            {
                return Err(ShareFetchRequestFailure::ForgottenPartitionStillActive);
            }
        }
        Ok(Self {
            active,
            included,
            forgotten,
        })
    }

    pub(super) fn is_complete_initial(&self) -> bool {
        self.forgotten.is_empty() && same_partitions(&self.active, &self.included)
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        Vec<ShareFetchRequestTopic>,
        Vec<ShareFetchRequestTopic>,
        Vec<ShareFetchRequestTopic>,
    ) {
        (self.active, self.included, self.forgotten)
    }
}

fn canonicalize_topics(
    topics: &mut [ShareFetchRequestTopic],
) -> Result<(), ShareFetchRequestFailure> {
    if topics.len() > SHARE_FETCH_MAX_TOPICS {
        return Err(ShareFetchRequestFailure::TopicCount {
            actual: topics.len(),
            limit: SHARE_FETCH_MAX_TOPICS,
        });
    }
    let total = topics
        .iter()
        .try_fold(0usize, |total, topic| {
            total.checked_add(topic.partitions.len())
        })
        .ok_or(ShareFetchRequestFailure::PartitionCount {
            actual: usize::MAX,
            limit: SHARE_FETCH_MAX_PARTITIONS,
        })?;
    if total > SHARE_FETCH_MAX_PARTITIONS {
        return Err(ShareFetchRequestFailure::PartitionCount {
            actual: total,
            limit: SHARE_FETCH_MAX_PARTITIONS,
        });
    }
    topics.sort_unstable_by_key(|topic| topic.topic_id);
    if topics
        .windows(2)
        .any(|pair| pair[0].topic_id == pair[1].topic_id)
    {
        return Err(ShareFetchRequestFailure::DuplicateTopic);
    }
    Ok(())
}

fn validate_topic(topic_id: [u8; 16], partitions: &[u32]) -> Result<(), ShareFetchRequestFailure> {
    if topic_id == [0; 16] {
        return Err(ShareFetchRequestFailure::ZeroTopicId);
    }
    if partitions.is_empty() {
        return Err(ShareFetchRequestFailure::EmptyTopic);
    }
    for (index, partition) in partitions.iter().copied().enumerate() {
        if i32::try_from(partition).is_err() {
            return Err(ShareFetchRequestFailure::PartitionOutOfRange(partition));
        }
        if partitions[..index].contains(&partition) {
            return Err(ShareFetchRequestFailure::DuplicatePartition(partition));
        }
    }
    Ok(())
}

fn contains_partition(
    topics: &[ShareFetchRequestTopic],
    topic_id: [u8; 16],
    partition: u32,
) -> bool {
    topics
        .iter()
        .find(|topic| topic.topic_id == topic_id)
        .is_some_and(|topic| topic.partitions.contains(&partition))
}

fn same_partitions(left: &[ShareFetchRequestTopic], right: &[ShareFetchRequestTopic]) -> bool {
    left.len() == right.len()
        && left.iter().all(|topic| {
            right
                .iter()
                .find(|candidate| candidate.topic_id == topic.topic_id)
                .is_some_and(|candidate| candidate.partitions == topic.partitions)
        })
}
