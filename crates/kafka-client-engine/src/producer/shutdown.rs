//! Producer terminal drain, verification, and notifier handoff.

use std::{error::Error, fmt, thread::ThreadId};

use crate::completion::{CompletionRegistryError, NotifierJoin};

use super::ProducerHost;

/// Cleanup ownership retained even when terminal recovery remains damaged.
pub(crate) struct ProducerNotifierRecovery {
    pub(crate) notifier: Option<NotifierJoin>,
    pub(crate) error: Option<CompletionRegistryError>,
}

/// Stage whose ownership verification found retained terminal resources.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerTerminalCleanupPhase {
    ReleaseBeforeCompletion,
    Final,
}

/// Terminal verification found retained resources or unpublished completions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerTerminalCleanupError {
    phase: ProducerTerminalCleanupPhase,
    retained_mechanisms: usize,
    unsettled_completions: usize,
}

impl fmt::Display for ProducerTerminalCleanupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let phase = match self.phase {
            ProducerTerminalCleanupPhase::ReleaseBeforeCompletion => {
                "release-before-completion verification"
            }
            ProducerTerminalCleanupPhase::Final => "final terminal verification",
        };
        write!(
            formatter,
            "{phase} found {} retained mechanisms and {} unsettled completions",
            self.retained_mechanisms, self.unsettled_completions
        )
    }
}

impl Error for ProducerTerminalCleanupError {}

impl ProducerHost {
    /// Permanently closes core admission before terminal host drain.
    pub(crate) const fn close_admission(&mut self) {
        self.core.close_admission();
    }

    /// Returns accepted operations that still owe terminal publication.
    pub(crate) fn unsettled_completions(&self) -> usize {
        self.completions.unsettled_len()
    }

    /// Stops notification without joining on the reactor.
    pub(crate) fn begin_notification_shutdown(
        &mut self,
    ) -> Result<NotifierJoin, CompletionRegistryError> {
        self.completions.stop_notifier()
    }

    /// Transfers notifier ownership after catastrophic recovery.
    pub(crate) fn recover_notifier(&mut self) -> ProducerNotifierRecovery {
        let mut error = (self.unsettled_completions() != 0)
            .then_some(CompletionRegistryError::UnsettledCompletion)
            .or_else(|| {
                self.completions
                    .notifier_thread_id()
                    .is_none()
                    .then_some(CompletionRegistryError::NotifierStopped)
            });
        let notifier = self.completions.take_notifier();
        if notifier.is_none() && error.is_none() {
            error = Some(CompletionRegistryError::NotifierStopped);
        }
        ProducerNotifierRecovery { notifier, error }
    }

    /// Returns the thread that exclusively executes completion wakeups.
    pub(crate) fn notifier_thread_id(&self) -> Option<ThreadId> {
        self.completions.notifier_thread_id()
    }

    /// Drops live mechanisms outside-in and replaces all old core state.
    pub(crate) fn drain_terminal_mechanisms(&mut self) {
        self.drain_terminal_mechanisms_preserving_completions();
        if self.terminal_backlog.is_empty() {
            self.bindings.clear_terminal();
        }
    }

    /// Drops mechanism owners while exact terminal bindings remain recoverable.
    pub(super) fn drain_terminal_mechanisms_preserving_completions(&mut self) {
        self.pending_effects.clear();
        self.timers.clear_terminal();
        self.execution.clear_terminal();
        self.reclaimer.clear_terminal();
        self.store.clear_terminal();
        let mut core = self.core_config.machine();
        core.close_admission();
        self.core = core;
    }

    /// Releases quarantined effect tokens only after completion settlement.
    pub(super) fn clear_terminal_evidence(&mut self) {
        self.terminal_poison.clear_terminal();
        self.terminal_quarantine.clear_terminal();
        self.terminal_refusals.clear_terminal();
        self.fatal_transition.clear_terminal();
    }

    /// Verifies exact effect interpretation released bytes before completion.
    pub(crate) fn verify_release_before_completion(
        &self,
    ) -> Result<(), ProducerTerminalCleanupError> {
        let retained_mechanisms = self.release_owned_mechanisms();
        if retained_mechanisms == 0 {
            Ok(())
        } else {
            Err(ProducerTerminalCleanupError {
                phase: ProducerTerminalCleanupPhase::ReleaseBeforeCompletion,
                retained_mechanisms,
                unsettled_completions: self.unsettled_completions(),
            })
        }
    }

    /// Confirms the drain and terminal publication completed before `Closed`.
    pub(crate) fn verify_terminal_cleanup(&self) -> Result<(), ProducerTerminalCleanupError> {
        let retained_mechanisms = self.final_retained_mechanisms();
        let unsettled_completions = self.unsettled_completions();
        if retained_mechanisms == 0 && unsettled_completions == 0 {
            Ok(())
        } else {
            Err(ProducerTerminalCleanupError {
                phase: ProducerTerminalCleanupPhase::Final,
                retained_mechanisms,
                unsettled_completions,
            })
        }
    }

    fn release_owned_mechanisms(&self) -> usize {
        let stats = self.stats();
        let core_bytes = usize::try_from(stats.core_retained_bytes.get()).unwrap_or(usize::MAX);
        stats
            .store
            .records
            .saturating_add(stats.store.bytes)
            .saturating_add(stats.store.batches)
            .saturating_add(stats.store.topics)
            .saturating_add(stats.active_timers)
            .saturating_add(stats.prepared_batches)
            .saturating_add(stats.prepared_bytes)
            .saturating_add(stats.submission_deadlines)
            .saturating_add(stats.pending_effects)
            .saturating_add(core_bytes)
    }

    fn final_retained_mechanisms(&self) -> usize {
        self.release_owned_mechanisms()
            .saturating_add(self.bindings.len())
            .saturating_add(self.terminal_backlog.len())
            .saturating_add(self.terminal_poison.len())
            .saturating_add(self.terminal_quarantine.retained_len())
            .saturating_add(self.terminal_refusals.retained_len())
            .saturating_add(self.fatal_transition.retained_len())
            .saturating_add(self.core.completion_slots())
    }

    #[cfg(test)]
    pub(crate) fn terminal_resources_empty(&self) -> bool {
        let stats = self.stats();
        stats.store.records == 0
            && stats.store.bytes == 0
            && stats.store.batches == 0
            && stats.store.topics == 0
            && stats.active_timers == 0
            && stats.prepared_batches == 0
            && stats.prepared_bytes == 0
            && stats.submission_deadlines == 0
            && stats.completion_bindings == 0
            && stats.pending_effects == 0
            && stats.terminal_backlog == 0
            && self.terminal_poison.len() == 0
            && self.terminal_quarantine.retained_len() == 0
            && self.terminal_refusals.retained_len() == 0
            && self.fatal_transition.retained_len() == 0
            && stats.core_retained_bytes == kafka_client_core::ByteCount::new(0)
            && stats.core_completion_slots == 0
            && self.unsettled_completions() == 0
    }
}
