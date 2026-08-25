//! One fresh broker topic-identity validation before transaction commit.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use crate::{ErrorKind, KafkaError, TopicUuid, admin::BatchResult};

use super::TransactionEngine;
use crate::bridge::{
    admin_topics_operation::AdminDescribeTopics, admin_topics_request::DescribeTopicsAdminRequest,
};

enum TransactionValidationState {
    Ready(Option<Result<(), KafkaError>>),
    Topics(AdminDescribeTopics),
}

/// Private linear validation observer retaining the active transaction borrow.
#[must_use = "dropping abandons validation and never installs a commit seal"]
pub(crate) struct TransactionValidationEngine<'validation, 'producer> {
    transaction: &'validation mut TransactionEngine<'producer>,
    revision: u64,
    state: TransactionValidationState,
}

impl<'producer> TransactionEngine<'producer> {
    pub(crate) fn validate_for_commit<'validation>(
        &'validation mut self,
        deadline: Instant,
    ) -> Result<TransactionValidationEngine<'validation, 'producer>, KafkaError> {
        TransactionValidationEngine::begin(self, deadline)
    }
}

impl<'validation, 'producer> TransactionValidationEngine<'validation, 'producer> {
    pub(crate) fn begin(
        transaction: &'validation mut TransactionEngine<'producer>,
        deadline: Instant,
    ) -> Result<Self, KafkaError> {
        if deadline <= Instant::now() {
            return Err(KafkaError::new(
                ErrorKind::Timeout,
                "transaction validation deadline elapsed at admission",
            ));
        }
        transaction
            .inner
            .preflight_commit()
            .map_err(|error| super::result::translate_control_kind(error.kind()))?;
        let revision = transaction.identity.revision();
        let topics = transaction.identity.topic_names()?;
        let state = if topics.is_empty() {
            TransactionValidationState::Ready(Some(Ok(())))
        } else if deadline <= Instant::now() {
            TransactionValidationState::Ready(Some(Err(KafkaError::new(
                ErrorKind::Timeout,
                "transaction validation deadline elapsed before submission",
            ))))
        } else {
            TransactionValidationState::Topics(transaction.admin.submit_describe_topics_until(
                DescribeTopicsAdminRequest::from_topics(topics),
                deadline,
            ))
        };
        Ok(Self {
            transaction,
            revision,
            state,
        })
    }

    pub(crate) fn wait(mut self) -> Result<(), KafkaError> {
        let state = core::mem::replace(&mut self.state, TransactionValidationState::Ready(None));
        let result = match state {
            TransactionValidationState::Ready(Some(result)) => result,
            TransactionValidationState::Ready(None) => Err(already_observed()),
            TransactionValidationState::Topics(operation) => operation.wait().and_then(|result| {
                validate_topic_descriptions(&self.transaction.identity, &result)
            }),
        };
        self.finish(result)
    }

    fn finish(self, result: Result<(), KafkaError>) -> Result<(), KafkaError> {
        match result {
            Ok(()) => self.transaction.identity.install_seal(self.revision),
            Err(error) if error.kind() == ErrorKind::Identity => {
                self.transaction.identity.mark_topic_mismatch();
                Err(error)
            }
            Err(error) => Err(error),
        }
    }
}

impl Future for TransactionValidationEngine<'_, '_> {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = match &mut this.state {
            TransactionValidationState::Ready(result) => {
                Poll::Ready(result.take().unwrap_or_else(|| Err(already_observed())))
            }
            TransactionValidationState::Topics(operation) => {
                let Poll::Ready(result) = Pin::new(operation).poll(context) else {
                    return Poll::Pending;
                };
                Poll::Ready(result.and_then(|result| {
                    validate_topic_descriptions(&this.transaction.identity, &result)
                }))
            }
        };
        result.map(|result| match result {
            Ok(()) => this.transaction.identity.install_seal(this.revision),
            Err(error) if error.kind() == ErrorKind::Identity => {
                this.transaction.identity.mark_topic_mismatch();
                Err(error)
            }
            Err(error) => Err(error),
        })
    }
}

pub(super) fn validate_topic_descriptions(
    identity: &super::identity::TransactionIdentityState,
    result: &BatchResult<String, crate::admin::TopicDescription>,
) -> Result<(), KafkaError> {
    let expected_count = identity
        .topics()
        .iter()
        .filter(|binding| binding.topic_uuid().is_some())
        .count();
    if result.entries().len() != expected_count {
        return Err(identity_mismatch(
            "DescribeTopics did not return every transaction topic exactly once",
        ));
    }
    for (binding, (key, description)) in identity
        .topics()
        .iter()
        .filter(|binding| binding.topic_uuid().is_some())
        .zip(result.entries())
    {
        if key != binding.topic() {
            return Err(identity_mismatch(
                "DescribeTopics returned transaction topics out of correlation",
            ));
        }
        let description = description.as_ref().map_err(Clone::clone)?;
        let observed = description.topic_id().and_then(TopicUuid::try_from_bytes);
        if description.name() != binding.topic() || observed != binding.topic_uuid() {
            return Err(identity_mismatch(
                "transaction topic UUID changed before commit",
            ));
        }
    }
    Ok(())
}

fn identity_mismatch(message: &'static str) -> KafkaError {
    KafkaError::new(ErrorKind::Identity, message).with_transaction_abort_required()
}

fn already_observed() -> KafkaError {
    KafkaError::new(
        ErrorKind::State,
        "transaction identity validation was already observed",
    )
}

impl core::fmt::Debug for TransactionValidationEngine<'_, '_> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("TransactionValidationEngine")
            .field("revision", &self.revision)
            .finish_non_exhaustive()
    }
}
