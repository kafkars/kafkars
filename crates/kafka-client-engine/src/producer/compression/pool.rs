//! Fixed worker, queue, deadline, and byte ownership for compression jobs.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel},
    },
    thread::{self, JoinHandle},
};

use kafka_client_core::{BatchExecutionId, Deadline, OperationId};

use super::{
    CompressionCompletion, CompressionJob,
    deadline::{InFlight, ScheduledDeadline},
};
use crate::producer::ingress::ProducerShardWake;

const WORKER_THREAD_NAME: &str = "kafka-client-compression";

/// Native worker and retained-work bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CompressionWorkerLimits {
    pub(crate) workers: usize,
    pub(crate) jobs: usize,
    pub(crate) bytes: usize,
}

/// Bounded producer compression owner joined during host teardown.
pub(crate) struct CompressionWorkers {
    sender: Option<SyncSender<CompressionJob>>,
    results: Receiver<CompressionCompletion>,
    handles: Vec<JoinHandle<()>>,
    pub(super) entries: BTreeMap<BatchExecutionId, InFlight>,
    pub(super) schedule: BTreeSet<ScheduledDeadline>,
    job_capacity: usize,
    byte_capacity: usize,
    reserved_bytes: usize,
}

impl std::fmt::Debug for CompressionWorkers {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompressionWorkers")
            .field("worker_count", &self.handles.len())
            .field("retained_jobs", &self.entries.len())
            .field("reserved_bytes", &self.reserved_bytes)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) enum CompressionSchedule {
    Accepted,
    Full(CompressionJob),
    Disconnected(CompressionJob),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompressionPollError {
    UnknownExecution(BatchExecutionId),
    ByteAccounting,
    JobDisconnected,
    ResultDisconnected,
}

impl CompressionWorkers {
    pub(crate) fn start<W>(
        limits: CompressionWorkerLimits,
        wake: &Arc<W>,
    ) -> Result<Self, io::Error>
    where
        W: ProducerShardWake,
    {
        let (sender, jobs) = sync_channel(limits.jobs);
        let jobs = Arc::new(Mutex::new(jobs));
        let (results, receiver) = sync_channel(limits.jobs);
        let mut handles = Vec::with_capacity(limits.workers);
        for _worker in 0..limits.workers {
            let jobs = Arc::clone(&jobs);
            let worker_results = results.clone();
            let wake = Arc::clone(wake);
            match thread::Builder::new()
                .name(WORKER_THREAD_NAME.to_owned())
                .spawn(move || super::worker::run(&jobs, &worker_results, wake.as_ref()))
            {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    drop(sender);
                    drop(results);
                    for handle in handles {
                        let _joined = handle.join();
                    }
                    return Err(error);
                }
            }
        }
        drop(results);
        Ok(Self {
            sender: Some(sender),
            results: receiver,
            handles,
            entries: BTreeMap::new(),
            schedule: BTreeSet::new(),
            job_capacity: limits.jobs,
            byte_capacity: limits.bytes,
            reserved_bytes: 0,
        })
    }

    pub(crate) fn try_submit(
        &mut self,
        job: CompressionJob,
        deadline_operation_id: OperationId,
        deadline: Deadline,
    ) -> CompressionSchedule {
        let execution = job.execution();
        let reservation_bytes = job.reservation_bytes();
        let Some(next_bytes) = self.reserved_bytes.checked_add(reservation_bytes) else {
            return CompressionSchedule::Full(job);
        };
        if self.entries.len() >= self.job_capacity || next_bytes > self.byte_capacity {
            return CompressionSchedule::Full(job);
        }
        let Some(sender) = self.sender.as_ref() else {
            return CompressionSchedule::Disconnected(job);
        };
        match sender.try_send(job) {
            Ok(()) => {}
            Err(TrySendError::Full(job)) => return CompressionSchedule::Full(job),
            Err(TrySendError::Disconnected(job)) => {
                return CompressionSchedule::Disconnected(job);
            }
        }
        self.entries.insert(
            execution,
            InFlight {
                deadline_operation_id,
                deadline,
                reservation_bytes,
                cancelled: false,
            },
        );
        self.schedule.insert(ScheduledDeadline {
            deadline,
            execution,
        });
        self.reserved_bytes = next_bytes;
        CompressionSchedule::Accepted
    }

    pub(crate) fn try_complete(
        &mut self,
    ) -> Result<Option<(CompressionCompletion, bool)>, CompressionPollError> {
        let completion = match self.results.try_recv() {
            Ok(completion) => completion,
            Err(TryRecvError::Empty) => return Ok(None),
            Err(TryRecvError::Disconnected) if self.entries.is_empty() => return Ok(None),
            Err(TryRecvError::Disconnected) => {
                return Err(CompressionPollError::ResultDisconnected);
            }
        };
        let execution = completion.execution();
        let Some(entry) = self.entries.remove(&execution) else {
            return Err(CompressionPollError::UnknownExecution(execution));
        };
        self.schedule.remove(&ScheduledDeadline {
            deadline: entry.deadline,
            execution,
        });
        self.reserved_bytes = self
            .reserved_bytes
            .checked_sub(entry.reservation_bytes)
            .ok_or(CompressionPollError::ByteAccounting)?;
        Ok(Some((completion, entry.cancelled)))
    }

    pub(crate) fn shutdown(&mut self) {
        drop(self.sender.take());
        for handle in self.handles.drain(..) {
            let _joined = handle.join();
        }
        while self.results.try_recv().is_ok() {}
        self.entries.clear();
        self.schedule.clear();
        self.reserved_bytes = 0;
    }

    pub(crate) fn retained_jobs(&self) -> usize {
        self.entries.len()
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.reserved_bytes
    }

    #[cfg(test)]
    pub(crate) fn worker_count(&self) -> usize {
        self.handles.len()
    }

    #[cfg(test)]
    pub(crate) fn complete_with_timeout(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<Option<(CompressionCompletion, bool)>, CompressionPollError> {
        let completion = match self.results.recv_timeout(timeout) {
            Ok(completion) => completion,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => return Ok(None),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err(CompressionPollError::ResultDisconnected);
            }
        };
        let execution = completion.execution();
        let Some(entry) = self.entries.remove(&execution) else {
            return Err(CompressionPollError::UnknownExecution(execution));
        };
        self.schedule.remove(&ScheduledDeadline {
            deadline: entry.deadline,
            execution,
        });
        self.reserved_bytes = self
            .reserved_bytes
            .checked_sub(entry.reservation_bytes)
            .ok_or(CompressionPollError::ByteAccounting)?;
        Ok(Some((completion, entry.cancelled)))
    }
}

impl Drop for CompressionWorkers {
    fn drop(&mut self) {
        self.shutdown();
    }
}
