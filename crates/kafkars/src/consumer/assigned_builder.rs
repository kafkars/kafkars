//! Inert construction of the engine's unique assigned-consumer capability.

use crate::bridge::ClientEngine;

use super::{AssignedConsumer, AssignedConsumerBuildError};

/// Builder for a consumer with direct partition ownership.
#[derive(Debug, Clone)]
pub struct AssignedConsumerBuilder {
    source: AssignedConsumerSource,
}

#[derive(Debug, Clone)]
enum AssignedConsumerSource {
    Shared(ClientEngine),
    Independent(crate::client::ClientBuilder),
}

impl AssignedConsumerBuilder {
    pub(crate) const fn new(engine: ClientEngine) -> Self {
        Self {
            source: AssignedConsumerSource::Shared(engine),
        }
    }

    pub(crate) const fn independent(client: crate::client::ClientBuilder) -> Self {
        Self {
            source: AssignedConsumerSource::Independent(client),
        }
    }

    /// Builds the selected directly assigned consumer owner.
    ///
    /// A shared builder claims its client's sole capability. An independent
    /// builder starts and claims a private execution owner. Rejection returns
    /// this exact builder because no unique capability transferred to the call.
    pub fn build(self) -> Result<AssignedConsumer, AssignedConsumerBuildError> {
        let result = match self.source.clone() {
            AssignedConsumerSource::Shared(engine) => {
                engine.claim_assigned_consumer().map(AssignedConsumer::new)
            }
            AssignedConsumerSource::Independent(configuration) => {
                configuration.build().and_then(|client| {
                    client
                        .assigned_consumer()
                        .build()
                        .map_err(|error| error.into_parts().1)
                })
            }
        };
        result.map_err(|error| AssignedConsumerBuildError::new(self, error))
    }
}
