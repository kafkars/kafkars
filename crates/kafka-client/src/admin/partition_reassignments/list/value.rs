//! Stable ordered broker sets for one active partition reassignment.

/// One active reassignment without generated wire or engine values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionReassignment {
    replicas: Vec<i32>,
    adding_replicas: Vec<i32>,
    removing_replicas: Vec<i32>,
}

impl PartitionReassignment {
    pub(crate) const fn new(
        replicas: Vec<i32>,
        adding_replicas: Vec<i32>,
        removing_replicas: Vec<i32>,
    ) -> Self {
        Self {
            replicas,
            adding_replicas,
            removing_replicas,
        }
    }

    /// Returns Kafka's ordered current replica list.
    pub fn replicas(&self) -> &[i32] {
        &self.replicas
    }

    /// Returns Kafka's ordered adding-replica list.
    pub fn adding_replicas(&self) -> &[i32] {
        &self.adding_replicas
    }

    /// Returns Kafka's ordered removing-replica list.
    pub fn removing_replicas(&self) -> &[i32] {
        &self.removing_replicas
    }

    /// Consumes this description into its ordered broker lists.
    pub fn into_parts(self) -> (Vec<i32>, Vec<i32>, Vec<i32>) {
        (self.replicas, self.adding_replicas, self.removing_replicas)
    }
}
