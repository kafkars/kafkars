//! Borrowed per-broker assignments and generated-free response facts.

/// One caller-ordered replica move after exact broker routing is selected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AlterReplicaLogDirAssignmentRef<'a> {
    topic: &'a str,
    partition: i32,
    log_dir: &'a str,
}

impl<'a> AlterReplicaLogDirAssignmentRef<'a> {
    pub(crate) const fn new(topic: &'a str, partition: i32, log_dir: &'a str) -> Self {
        Self {
            topic,
            partition,
            log_dir,
        }
    }

    pub(crate) const fn topic(self) -> &'a str {
        self.topic
    }

    pub(crate) const fn partition(self) -> i32 {
        self.partition
    }

    pub(crate) const fn log_dir(self) -> &'a str {
        self.log_dir
    }
}

/// One caller-ordered partition result with Kafka's exact signed error code.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedAlterReplicaLogDirOutcome {
    pub(super) topic: String,
    pub(super) partition: i32,
    pub(super) error_code: i16,
}

impl NormalizedAlterReplicaLogDirOutcome {
    #[cfg(test)]
    pub(crate) const fn fixture(topic: String, partition: i32, error_code: i16) -> Self {
        Self {
            topic,
            partition,
            error_code,
        }
    }

    #[cfg(test)]
    pub(crate) fn topic(&self) -> &str {
        &self.topic
    }

    #[cfg(test)]
    pub(crate) const fn partition(&self) -> i32 {
        self.partition
    }

    #[cfg(test)]
    pub(crate) const fn error_code(&self) -> i16 {
        self.error_code
    }

    pub(crate) fn into_parts(self) -> (String, i32, i16) {
        (self.topic, self.partition, self.error_code)
    }
}

/// One normalized API-key 34 response retaining the authoritative version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedAlterReplicaLogDirsResponse {
    pub(super) selected_version: i16,
    pub(super) throttle_time_ms: u32,
    pub(super) outcomes: Vec<NormalizedAlterReplicaLogDirOutcome>,
    pub(super) retained_bytes: usize,
}

impl NormalizedAlterReplicaLogDirsResponse {
    #[cfg(test)]
    pub(crate) const fn fixture(
        selected_version: i16,
        throttle_time_ms: u32,
        outcomes: Vec<NormalizedAlterReplicaLogDirOutcome>,
        retained_bytes: usize,
    ) -> Self {
        Self {
            selected_version,
            throttle_time_ms,
            outcomes,
            retained_bytes,
        }
    }

    #[cfg(test)]
    pub(crate) const fn selected_version(&self) -> i16 {
        self.selected_version
    }

    #[cfg(test)]
    pub(crate) const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    #[cfg(test)]
    pub(crate) fn outcomes(&self) -> &[NormalizedAlterReplicaLogDirOutcome] {
        &self.outcomes
    }

    pub(crate) fn into_parts(self) -> (i16, u32, Vec<NormalizedAlterReplicaLogDirOutcome>, usize) {
        (
            self.selected_version,
            self.throttle_time_ms,
            self.outcomes,
            self.retained_bytes,
        )
    }
}
