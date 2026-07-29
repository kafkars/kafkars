//! Strict API-92 v0 response validation, correlation, and bounded materialization.

use core::{cmp::Ordering, num::NonZeroI16};

use kafka_client_core::{
    DeleteShareGroupOffsetsBatch, DeleteShareGroupOffsetsBrokerError, DeleteShareGroupOffsetsPlan,
    DeleteShareGroupOffsetsTopicBrokerError, DeleteShareGroupOffsetsTopicOutcome,
};
use kafka_wire::DeleteShareGroupOffsetsResponse;

use super::{
    ValidatedDeleteShareGroupOffsetsResponse,
    retention::{
        MAX_NORMALIZED_BYTES, MAX_RESPONSE_TEXT_BYTES, MAX_RESPONSE_TOPICS, MAX_TOPIC_NAME_BYTES,
        actual_batch_retained_bytes, actual_broker_error_retained_bytes, batch_required_bytes,
        bounded_diagnostic, broker_error_required_bytes, correlation_scratch_bytes,
        topic_error_required_bytes,
    },
};

/// Generated response facts unsafe to bind to the accepted API-92 plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteShareGroupOffsetsProtocolFailure {
    MissingSelectedVersion,
    UnsupportedApiVersion {
        actual: i16,
    },
    NegativeThrottleTime {
        actual: i32,
    },
    TooManyTopics {
        actual: usize,
        max: usize,
    },
    TopicCount {
        expected: usize,
        actual: usize,
    },
    EmptyTopicName,
    TopicNameTooLong {
        actual: usize,
        max: usize,
    },
    ResponseTextBytesExceeded {
        required: usize,
        max: usize,
    },
    DuplicateTopic,
    MissingTopic,
    UnexpectedTopic,
    DiagnosticOnSuccess,
    ZeroTopicId,
    TopicsOnTopLevelError,
    NormalizedBytesExceeded {
        required: usize,
        max: usize,
    },
    RetainedBytes {
        required: usize,
        limit: usize,
    },
    Allocation {
        field: &'static str,
        requested: usize,
    },
}

/// Validates every bounded fact and restores the exact caller topic order.
pub(crate) fn normalize_delete_share_group_offsets_response(
    plan: &DeleteShareGroupOffsetsPlan,
    selected_version: Option<i16>,
    response: &DeleteShareGroupOffsetsResponse,
    retained_limit: usize,
) -> Result<ValidatedDeleteShareGroupOffsetsResponse, DeleteShareGroupOffsetsProtocolFailure> {
    validate_version(selected_version)?;
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        DeleteShareGroupOffsetsProtocolFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    validate_source_shape(response)?;

    if let Some(code) = NonZeroI16::new(response.error_code) {
        if !response.responses.is_empty() {
            return Err(DeleteShareGroupOffsetsProtocolFailure::TopicsOnTopLevelError);
        }
        let required = broker_error_required_bytes(response.error_message.as_deref())
            .ok_or(bytes_overflow())?;
        ensure_normalized_limit(required)?;
        ensure_retained_limit(required, retained_limit)?;
        let (message, truncated) = bounded_diagnostic(response.error_message.as_deref());
        let error =
            DeleteShareGroupOffsetsBrokerError::new(throttle_time_ms, code, message, truncated);
        let retained_bytes = actual_broker_error_retained_bytes(&error).ok_or(bytes_overflow())?;
        ensure_normalized_limit(retained_bytes)?;
        ensure_retained_limit(retained_bytes, retained_limit)?;
        return Ok(ValidatedDeleteShareGroupOffsetsResponse::BrokerRejected {
            error,
            retained_bytes,
        });
    }
    if response.error_message.is_some() {
        return Err(DeleteShareGroupOffsetsProtocolFailure::DiagnosticOnSuccess);
    }

    let required = batch_required_bytes(response.responses.iter().map(|topic| {
        (
            topic.topic_name.as_str(),
            NonZeroI16::new(topic.error_code).and_then(|_| topic.error_message.as_deref()),
        )
    }))
    .ok_or(bytes_overflow())?;
    ensure_normalized_limit(required)?;
    let peak_required = required
        .checked_add(correlation_scratch_bytes(response.responses.len()).ok_or(bytes_overflow())?)
        .ok_or(bytes_overflow())?;
    ensure_retained_limit(peak_required, retained_limit)?;

    let mapping = correlate_topics(plan, response)?;
    let outcomes = materialize_outcomes(plan, response, &mapping)?;
    let batch = DeleteShareGroupOffsetsBatch::new(throttle_time_ms, outcomes);
    let retained_bytes = actual_batch_retained_bytes(&batch).ok_or(bytes_overflow())?;
    ensure_normalized_limit(retained_bytes)?;
    ensure_retained_limit(retained_bytes, retained_limit)?;
    Ok(ValidatedDeleteShareGroupOffsetsResponse::Batch {
        batch,
        retained_bytes,
    })
}

fn validate_version(
    selected_version: Option<i16>,
) -> Result<(), DeleteShareGroupOffsetsProtocolFailure> {
    match selected_version {
        None => Err(DeleteShareGroupOffsetsProtocolFailure::MissingSelectedVersion),
        Some(0) => Ok(()),
        Some(actual) => {
            Err(DeleteShareGroupOffsetsProtocolFailure::UnsupportedApiVersion { actual })
        }
    }
}

fn validate_source_shape(
    response: &DeleteShareGroupOffsetsResponse,
) -> Result<(), DeleteShareGroupOffsetsProtocolFailure> {
    if response.responses.len() > MAX_RESPONSE_TOPICS {
        return Err(DeleteShareGroupOffsetsProtocolFailure::TooManyTopics {
            actual: response.responses.len(),
            max: MAX_RESPONSE_TOPICS,
        });
    }
    let mut text_bytes = response
        .error_message
        .as_ref()
        .map_or(0, |message| message.len());
    if text_bytes > MAX_RESPONSE_TEXT_BYTES {
        return Err(
            DeleteShareGroupOffsetsProtocolFailure::ResponseTextBytesExceeded {
                required: text_bytes,
                max: MAX_RESPONSE_TEXT_BYTES,
            },
        );
    }
    for topic in &response.responses {
        let name = topic.topic_name.as_str();
        if name.is_empty() {
            return Err(DeleteShareGroupOffsetsProtocolFailure::EmptyTopicName);
        }
        if name.len() > MAX_TOPIC_NAME_BYTES {
            return Err(DeleteShareGroupOffsetsProtocolFailure::TopicNameTooLong {
                actual: name.len(),
                max: MAX_TOPIC_NAME_BYTES,
            });
        }
        text_bytes = text_bytes
            .checked_add(name.len())
            .and_then(|bytes| {
                bytes.checked_add(
                    topic
                        .error_message
                        .as_ref()
                        .map_or(0, |message| message.len()),
                )
            })
            .ok_or(bytes_overflow())?;
        if text_bytes > MAX_RESPONSE_TEXT_BYTES {
            return Err(
                DeleteShareGroupOffsetsProtocolFailure::ResponseTextBytesExceeded {
                    required: text_bytes,
                    max: MAX_RESPONSE_TEXT_BYTES,
                },
            );
        }
        if topic.error_code == 0 {
            if topic.error_message.is_some() {
                return Err(DeleteShareGroupOffsetsProtocolFailure::DiagnosticOnSuccess);
            }
            if topic.topic_id.is_zero() {
                return Err(DeleteShareGroupOffsetsProtocolFailure::ZeroTopicId);
            }
        }
        let _ = topic_error_required_bytes(
            NonZeroI16::new(topic.error_code).and_then(|_| topic.error_message.as_deref()),
        )
        .ok_or(bytes_overflow())?;
    }
    Ok(())
}

fn correlate_topics(
    plan: &DeleteShareGroupOffsetsPlan,
    response: &DeleteShareGroupOffsetsResponse,
) -> Result<Vec<usize>, DeleteShareGroupOffsetsProtocolFailure> {
    if response.responses.len() != plan.topics().len() {
        return Err(DeleteShareGroupOffsetsProtocolFailure::TopicCount {
            expected: plan.topics().len(),
            actual: response.responses.len(),
        });
    }
    let mut expected = reserved_indices("expected topic correlation", plan.topics().len())?;
    let mut returned = reserved_indices("returned topic correlation", response.responses.len())?;
    expected.sort_unstable_by(|left, right| {
        plan.topics()[*left]
            .as_bytes()
            .cmp(plan.topics()[*right].as_bytes())
    });
    returned.sort_unstable_by(|left, right| {
        response.responses[*left]
            .topic_name
            .as_bytes()
            .cmp(response.responses[*right].topic_name.as_bytes())
    });
    if returned.windows(2).any(|pair| {
        response.responses[pair[0]].topic_name == response.responses[pair[1]].topic_name
    }) {
        return Err(DeleteShareGroupOffsetsProtocolFailure::DuplicateTopic);
    }

    let mut mapping = Vec::new();
    mapping
        .try_reserve_exact(plan.topics().len())
        .map_err(|_| DeleteShareGroupOffsetsProtocolFailure::Allocation {
            field: "caller topic mapping",
            requested: plan.topics().len(),
        })?;
    mapping.resize(plan.topics().len(), usize::MAX);
    for (expected_index, returned_index) in expected.into_iter().zip(returned) {
        match response.responses[returned_index]
            .topic_name
            .as_bytes()
            .cmp(plan.topics()[expected_index].as_bytes())
        {
            Ordering::Less => {
                return Err(DeleteShareGroupOffsetsProtocolFailure::UnexpectedTopic);
            }
            Ordering::Greater => {
                return Err(DeleteShareGroupOffsetsProtocolFailure::MissingTopic);
            }
            Ordering::Equal => mapping[expected_index] = returned_index,
        }
    }
    Ok(mapping)
}

fn materialize_outcomes(
    plan: &DeleteShareGroupOffsetsPlan,
    response: &DeleteShareGroupOffsetsResponse,
    mapping: &[usize],
) -> Result<Vec<DeleteShareGroupOffsetsTopicOutcome>, DeleteShareGroupOffsetsProtocolFailure> {
    let mut outcomes = Vec::new();
    outcomes.try_reserve_exact(mapping.len()).map_err(|_| {
        DeleteShareGroupOffsetsProtocolFailure::Allocation {
            field: "normalized topic outcomes",
            requested: mapping.len(),
        }
    })?;
    for (caller_index, response_index) in mapping.iter().copied().enumerate() {
        let returned = &response.responses[response_index];
        let topic = plan.topics()[caller_index].clone();
        let outcome = match NonZeroI16::new(returned.error_code) {
            None => {
                DeleteShareGroupOffsetsTopicOutcome::deleted(topic, returned.topic_id.to_bytes())
            }
            Some(code) => {
                let (message, truncated) = bounded_diagnostic(returned.error_message.as_deref());
                DeleteShareGroupOffsetsTopicOutcome::failed(
                    topic,
                    DeleteShareGroupOffsetsTopicBrokerError::new(code, message, truncated),
                )
            }
        };
        outcomes.push(outcome);
    }
    Ok(outcomes)
}

fn reserved_indices(
    field: &'static str,
    count: usize,
) -> Result<Vec<usize>, DeleteShareGroupOffsetsProtocolFailure> {
    let mut indices = Vec::new();
    indices.try_reserve_exact(count).map_err(|_| {
        DeleteShareGroupOffsetsProtocolFailure::Allocation {
            field,
            requested: count,
        }
    })?;
    indices.extend(0..count);
    Ok(indices)
}

fn ensure_normalized_limit(required: usize) -> Result<(), DeleteShareGroupOffsetsProtocolFailure> {
    (required <= MAX_NORMALIZED_BYTES).then_some(()).ok_or(
        DeleteShareGroupOffsetsProtocolFailure::NormalizedBytesExceeded {
            required,
            max: MAX_NORMALIZED_BYTES,
        },
    )
}

fn ensure_retained_limit(
    required: usize,
    limit: usize,
) -> Result<(), DeleteShareGroupOffsetsProtocolFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(DeleteShareGroupOffsetsProtocolFailure::RetainedBytes { required, limit })
}

const fn bytes_overflow() -> DeleteShareGroupOffsetsProtocolFailure {
    DeleteShareGroupOffsetsProtocolFailure::NormalizedBytesExceeded {
        required: usize::MAX,
        max: MAX_NORMALIZED_BYTES,
    }
}
