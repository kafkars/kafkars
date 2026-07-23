//! Bootstrap text translation into the driver's bounded endpoint vocabulary.

use std::{error::Error, fmt, num::NonZeroU16};

use kafka_driver::{BrokerEndpoint, HostName, HostNameError};

/// Converts one validated client bootstrap entry into driver-owned identity.
pub(super) fn parse(value: &str) -> Result<BrokerEndpoint, EndpointError> {
    let (host, port) = split(value).ok_or(EndpointError::Shape)?;
    let host = HostName::new(host.to_owned()).map_err(EndpointError::Host)?;
    let port = port
        .parse::<u16>()
        .ok()
        .and_then(NonZeroU16::new)
        .ok_or(EndpointError::Port)?;
    Ok(BrokerEndpoint::new(host, port))
}

fn split(value: &str) -> Option<(&str, &str)> {
    if let Some(rest) = value.strip_prefix('[') {
        let (host, port) = rest.split_once("]:")?;
        return (!host.is_empty() && !port.is_empty()).then_some((host, port));
    }

    let (host, port) = value.rsplit_once(':')?;
    (!host.is_empty() && !port.is_empty() && !host.contains(':')).then_some((host, port))
}

/// Why one facade-owned bootstrap entry could not enter driver ownership.
#[derive(Debug)]
pub(crate) enum EndpointError {
    /// The value was not `host:port` or `[ipv6]:port`.
    Shape,
    /// The driver rejected the logical host identity.
    Host(HostNameError),
    /// The port was not a nonzero `u16`.
    Port,
}

impl fmt::Display for EndpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shape => formatter.write_str("expected host:port or bracketed [ipv6]:port"),
            Self::Host(source) => write!(formatter, "{source}"),
            Self::Port => formatter.write_str("port must be between 1 and 65535"),
        }
    }
}

impl Error for EndpointError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Host(source) => Some(source),
            Self::Shape | Self::Port => None,
        }
    }
}
