//! Borrowed request selection and generated-free log-directory response facts.

/// One borrowed topic and explicit partition selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DescribeLogDirsTopicSelectionRef<'a> {
    topic: &'a str,
    partitions: &'a [i32],
}

impl<'a> DescribeLogDirsTopicSelectionRef<'a> {
    pub(crate) const fn new(topic: &'a str, partitions: &'a [i32]) -> Self {
        Self { topic, partitions }
    }

    pub(crate) const fn topic(self) -> &'a str {
        self.topic
    }

    pub(crate) const fn partitions(self) -> &'a [i32] {
        self.partitions
    }
}

/// Kafka's nullable topic selection without conflating empty with all topics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeLogDirsSelectionRef<'a> {
    AllTopics,
    Selected(&'a [DescribeLogDirsTopicSelectionRef<'a>]),
}

/// One normalized replica fact in a broker log directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeLogDirsPartition {
    pub(super) partition_index: i32,
    pub(super) partition_size: i64,
    pub(super) offset_lag: i64,
    pub(super) is_future: bool,
}

impl NormalizedDescribeLogDirsPartition {
    #[cfg(test)]
    pub(crate) const fn fixture(
        partition_index: i32,
        partition_size: i64,
        offset_lag: i64,
        is_future: bool,
    ) -> Self {
        Self {
            partition_index,
            partition_size,
            offset_lag,
            is_future,
        }
    }

    #[cfg(test)]
    pub(crate) const fn partition_index(&self) -> i32 {
        self.partition_index
    }

    #[cfg(test)]
    pub(crate) const fn partition_size(&self) -> i64 {
        self.partition_size
    }

    #[cfg(test)]
    pub(crate) const fn offset_lag(&self) -> i64 {
        self.offset_lag
    }

    #[cfg(test)]
    pub(crate) const fn is_future(&self) -> bool {
        self.is_future
    }

    pub(crate) const fn into_parts(self) -> (i32, i64, i64, bool) {
        (
            self.partition_index,
            self.partition_size,
            self.offset_lag,
            self.is_future,
        )
    }
}

/// One normalized topic within a broker log directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeLogDirsTopic {
    pub(super) name: String,
    pub(super) partitions: Vec<NormalizedDescribeLogDirsPartition>,
}

impl NormalizedDescribeLogDirsTopic {
    #[cfg(test)]
    pub(crate) const fn fixture(
        name: String,
        partitions: Vec<NormalizedDescribeLogDirsPartition>,
    ) -> Self {
        Self { name, partitions }
    }

    #[cfg(test)]
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn partitions(&self) -> &[NormalizedDescribeLogDirsPartition] {
        &self.partitions
    }

    pub(crate) fn into_parts(self) -> (String, Vec<NormalizedDescribeLogDirsPartition>) {
        (self.name, self.partitions)
    }
}

/// One normalized broker log directory with exact Kafka error and capacity facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeLogDir {
    pub(super) error_code: i16,
    pub(super) path: String,
    pub(super) topics: Vec<NormalizedDescribeLogDirsTopic>,
    pub(super) total_bytes: Option<i64>,
    pub(super) usable_bytes: Option<i64>,
    pub(super) is_cordoned: Option<bool>,
}

impl NormalizedDescribeLogDir {
    #[cfg(test)]
    pub(crate) const fn fixture(
        error_code: i16,
        path: String,
        topics: Vec<NormalizedDescribeLogDirsTopic>,
        total_bytes: Option<i64>,
        usable_bytes: Option<i64>,
        is_cordoned: Option<bool>,
    ) -> Self {
        Self {
            error_code,
            path,
            topics,
            total_bytes,
            usable_bytes,
            is_cordoned,
        }
    }

    #[cfg(test)]
    pub(crate) const fn error_code(&self) -> i16 {
        self.error_code
    }

    #[cfg(test)]
    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    #[cfg(test)]
    pub(crate) fn topics(&self) -> &[NormalizedDescribeLogDirsTopic] {
        &self.topics
    }

    #[cfg(test)]
    pub(crate) const fn total_bytes(&self) -> Option<i64> {
        self.total_bytes
    }

    #[cfg(test)]
    pub(crate) const fn usable_bytes(&self) -> Option<i64> {
        self.usable_bytes
    }

    #[cfg(test)]
    pub(crate) const fn is_cordoned(&self) -> Option<bool> {
        self.is_cordoned
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        i16,
        String,
        Vec<NormalizedDescribeLogDirsTopic>,
        Option<i64>,
        Option<i64>,
        Option<bool>,
    ) {
        (
            self.error_code,
            self.path,
            self.topics,
            self.total_bytes,
            self.usable_bytes,
            self.is_cordoned,
        )
    }
}

/// One broker's normalized API-key 35 response and authoritative selected version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeLogDirsResponse {
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "retained as protocol evidence after version-aware normalization"
        )
    )]
    pub(super) selected_version: i16,
    pub(super) throttle_time_ms: u32,
    pub(super) error_code: i16,
    pub(super) log_dirs: Vec<NormalizedDescribeLogDir>,
    pub(super) retained_bytes: usize,
}

impl NormalizedDescribeLogDirsResponse {
    #[cfg(test)]
    pub(crate) const fn fixture(
        selected_version: i16,
        throttle_time_ms: u32,
        error_code: i16,
        log_dirs: Vec<NormalizedDescribeLogDir>,
        retained_bytes: usize,
    ) -> Self {
        Self {
            selected_version,
            throttle_time_ms,
            error_code,
            log_dirs,
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
    pub(crate) const fn error_code(&self) -> i16 {
        self.error_code
    }

    #[cfg(test)]
    pub(crate) fn log_dirs(&self) -> &[NormalizedDescribeLogDir] {
        &self.log_dirs
    }

    pub(crate) fn into_parts(self) -> (u32, i16, Vec<NormalizedDescribeLogDir>, usize) {
        (
            self.throttle_time_ms,
            self.error_code,
            self.log_dirs,
            self.retained_bytes,
        )
    }
}
