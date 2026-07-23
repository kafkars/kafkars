//! Explicit join between `CreateTopics` admission and the embedded reactor wake.

use crate::{
    admin::{CreateTopicsShardWake, CreateTopicsShardWakeError},
    driver::ReactorWake,
};

impl CreateTopicsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), CreateTopicsShardWakeError> {
        self.request()
            .map_err(|error| CreateTopicsShardWakeError::from_io(error.into_io()))
    }
}
