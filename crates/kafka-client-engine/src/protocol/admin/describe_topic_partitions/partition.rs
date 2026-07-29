//! Generated-free exact partition facts for one API-key 75 page.

/// One normalized partition retaining broker order and nullable ELR arrays.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeTopicPartition {
    error_code: i16,
    partition_index: i32,
    leader_id: Option<i32>,
    leader_epoch: Option<i32>,
    replicas: Vec<i32>,
    isr: Vec<i32>,
    eligible_leader_replicas: Option<Vec<i32>>,
    last_known_elr: Option<Vec<i32>>,
    offline_replicas: Vec<i32>,
}

impl NormalizedDescribeTopicPartition {
    #[allow(clippy::too_many_arguments)]
    pub(super) const fn new(
        error_code: i16,
        partition_index: i32,
        leader_id: Option<i32>,
        leader_epoch: Option<i32>,
        replicas: Vec<i32>,
        isr: Vec<i32>,
        eligible_leader_replicas: Option<Vec<i32>>,
        last_known_elr: Option<Vec<i32>>,
        offline_replicas: Vec<i32>,
    ) -> Self {
        Self {
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replicas,
            isr,
            eligible_leader_replicas,
            last_known_elr,
            offline_replicas,
        }
    }

    /// Consumes every exact partition fact into host-owned parts.
    #[allow(clippy::type_complexity)]
    pub(crate) fn into_parts(
        self,
    ) -> (
        i16,
        i32,
        Option<i32>,
        Option<i32>,
        Vec<i32>,
        Vec<i32>,
        Option<Vec<i32>>,
        Option<Vec<i32>>,
        Vec<i32>,
    ) {
        (
            self.error_code,
            self.partition_index,
            self.leader_id,
            self.leader_epoch,
            self.replicas,
            self.isr,
            self.eligible_leader_replicas,
            self.last_known_elr,
            self.offline_replicas,
        )
    }

    pub(super) fn broker_capacities(&self) -> [usize; 5] {
        [
            self.replicas.capacity(),
            self.isr.capacity(),
            self.eligible_leader_replicas
                .as_ref()
                .map_or(0, Vec::capacity),
            self.last_known_elr.as_ref().map_or(0, Vec::capacity),
            self.offline_replicas.capacity(),
        ]
    }

    #[cfg(test)]
    pub(crate) const fn scalar_parts(&self) -> (i16, i32, Option<i32>, Option<i32>) {
        (
            self.error_code,
            self.partition_index,
            self.leader_id,
            self.leader_epoch,
        )
    }

    #[cfg(test)]
    pub(crate) fn replicas(&self) -> &[i32] {
        &self.replicas
    }

    #[cfg(test)]
    pub(crate) fn isr(&self) -> &[i32] {
        &self.isr
    }

    #[cfg(test)]
    pub(crate) fn eligible_leader_replicas(&self) -> Option<&[i32]> {
        self.eligible_leader_replicas.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn last_known_elr(&self) -> Option<&[i32]> {
        self.last_known_elr.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn offline_replicas(&self) -> &[i32] {
        &self.offline_replicas
    }
}
