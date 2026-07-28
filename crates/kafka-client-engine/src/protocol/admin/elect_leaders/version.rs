//! Exact generated version window for selected leader elections.

use kafka_client_core::LeaderElectionType;
use kafka_wire_core::ApiVersion;

pub(crate) const ELECT_LEADERS_PREFERRED_MIN_VERSION: ApiVersion = ApiVersion::new(0);
pub(crate) const ELECT_LEADERS_UNCLEAN_MIN_VERSION: ApiVersion = ApiVersion::new(1);
pub(crate) const ELECT_LEADERS_MAX_VERSION: ApiVersion = ApiVersion::new(2);

pub(crate) const fn minimum_version(election_type: LeaderElectionType) -> ApiVersion {
    match election_type {
        LeaderElectionType::Preferred => ELECT_LEADERS_PREFERRED_MIN_VERSION,
        LeaderElectionType::Unclean => ELECT_LEADERS_UNCLEAN_MIN_VERSION,
    }
}

pub(super) fn validate_selected_version(
    actual: i16,
    election_type: LeaderElectionType,
) -> Result<(), SelectedVersionFailure> {
    let minimum = minimum_version(election_type).value();
    let maximum = ELECT_LEADERS_MAX_VERSION.value();
    if actual < minimum || actual > maximum {
        return Err(SelectedVersionFailure {
            minimum,
            maximum,
            actual,
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SelectedVersionFailure {
    pub(super) minimum: i16,
    pub(super) maximum: i16,
    pub(super) actual: i16,
}
