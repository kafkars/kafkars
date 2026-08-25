//! Facade-to-engine security and producer configuration conversion.

use kafka_client_engine::{
    EngineProducerLimits, EngineSasl, EngineSecurity, EngineTls,
    ProducerCompression as EngineCompression,
};

use crate::{
    producer::{Compression, ProducerLimits},
    security::{Sasl, SaslMechanism, Security},
};

pub(in crate::bridge) fn engine_security(security: &Security) -> EngineSecurity {
    match security {
        Security::Plaintext => EngineSecurity::plaintext(),
        Security::Tls(tls) => EngineSecurity::tls(engine_tls(tls)),
        Security::SaslPlaintext(sasl) => EngineSecurity::sasl_plaintext(engine_sasl(sasl)),
        Security::SaslTls { tls, sasl } => {
            EngineSecurity::sasl_tls(engine_tls(tls), engine_sasl(sasl))
        }
    }
}

fn engine_tls(tls: &crate::Tls) -> EngineTls {
    tls.custom_roots_pem_bytes()
        .map_or_else(EngineTls::system_roots, |pem| {
            EngineTls::custom_roots_pem(pem.to_vec())
        })
}

fn engine_sasl(sasl: &Sasl) -> EngineSasl {
    let (username, password) = sasl.credentials();
    match sasl.mechanism() {
        SaslMechanism::Plain => EngineSasl::plain(username, password),
        SaslMechanism::ScramSha256 => EngineSasl::scram_sha_256(username, password),
        SaslMechanism::ScramSha512 => EngineSasl::scram_sha_512(username, password),
    }
}

pub(super) fn engine_producer_limits(limits: ProducerLimits) -> EngineProducerLimits {
    let (
        retained,
        active,
        waiting,
        waiting_bytes,
        batch,
        batch_bytes,
        request_bytes,
        max_in_flight_requests_per_broker,
        linger,
    ) = limits.into_parts();
    EngineProducerLimits::new(
        retained,
        active,
        waiting,
        waiting_bytes,
        batch,
        batch_bytes,
        linger,
    )
    .with_request_bytes(request_bytes)
    .with_max_in_flight_requests_per_broker(max_in_flight_requests_per_broker)
}

pub(super) const fn engine_compression(compression: Compression) -> EngineCompression {
    match compression {
        Compression::None => EngineCompression::None,
        Compression::Gzip => EngineCompression::Gzip,
        Compression::Snappy => EngineCompression::Snappy,
        Compression::Lz4 => EngineCompression::Lz4,
        Compression::Zstd => EngineCompression::Zstd,
    }
}
