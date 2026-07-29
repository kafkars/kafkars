//! Wire-free replica progress facts for one metadata quorum.

/// One voter or observer in the fixed metadata quorum.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeMetadataQuorumReplica {
    replica_id: i32,
    replica_directory_id: Option<[u8; 16]>,
    log_end_offset: Option<i64>,
    last_fetch_timestamp_ms: Option<i64>,
    last_caught_up_timestamp_ms: Option<i64>,
}

impl DescribeMetadataQuorumReplica {
    /// Creates one protocol-normalized replica fact.
    ///
    /// Optional values preserve explicit protocol sentinel normalization and
    /// version absence without exposing generated UUID or message types.
    pub const fn new(
        replica_id: i32,
        replica_directory_id: Option<[u8; 16]>,
        log_end_offset: Option<i64>,
        last_fetch_timestamp_ms: Option<i64>,
        last_caught_up_timestamp_ms: Option<i64>,
    ) -> Self {
        Self {
            replica_id,
            replica_directory_id,
            log_end_offset,
            last_fetch_timestamp_ms,
            last_caught_up_timestamp_ms,
        }
    }

    /// Returns the nonnegative replica identity.
    pub const fn replica_id(&self) -> i32 {
        self.replica_id
    }

    /// Returns the v2 directory identity after zero-sentinel normalization.
    pub const fn replica_directory_id(&self) -> Option<[u8; 16]> {
        self.replica_directory_id
    }

    /// Returns the log-end offset, or absence for Kafka's unknown sentinel.
    pub const fn log_end_offset(&self) -> Option<i64> {
        self.log_end_offset
    }

    /// Returns the last fetch timestamp, or absence when unknown/unrepresented.
    pub const fn last_fetch_timestamp_ms(&self) -> Option<i64> {
        self.last_fetch_timestamp_ms
    }

    /// Returns the last caught-up timestamp, or absence when unknown/unrepresented.
    pub const fn last_caught_up_timestamp_ms(&self) -> Option<i64> {
        self.last_caught_up_timestamp_ms
    }

    /// Consumes the replica into stable adapter-owned scalar parts.
    pub fn into_parts(self) -> (i32, Option<[u8; 16]>, Option<i64>, Option<i64>, Option<i64>) {
        (
            self.replica_id,
            self.replica_directory_id,
            self.log_end_offset,
            self.last_fetch_timestamp_ms,
            self.last_caught_up_timestamp_ms,
        )
    }
}
