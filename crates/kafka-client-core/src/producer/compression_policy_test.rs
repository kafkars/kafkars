//! Closed compression policy propagation into exact materialization effects.

use crate::{
    ByteCount, CompressionPolicy, ProducerBatchPolicy, ProducerEffect, ProducerMachine,
    ProducerRetryPolicy,
};

use super::scenario_support::idempotence::{accumulate, admit};

#[test]
fn every_configured_codec_crosses_core_only_as_an_explicit_effect_fact() {
    let policies = [
        CompressionPolicy::None,
        CompressionPolicy::Gzip,
        CompressionPolicy::Snappy,
        CompressionPolicy::Lz4,
        CompressionPolicy::Zstd,
    ];
    for (payload, policy) in (1_u64..).zip(policies) {
        let mut producer = ProducerMachine::with_batch_retry_and_compression_policy(
            ByteCount::new(64),
            1,
            ProducerBatchPolicy::single_record(),
            ProducerRetryPolicy::none(),
            policy,
        );
        producer.install_identity_for_test();
        let (operation_id, batch_id) = admit(&mut producer, payload, 0, 100);
        let transition = accumulate(&mut producer, operation_id, batch_id, 1);
        assert!(matches!(
            transition.effects(),
            [
                ProducerEffect::CancelBatchTimer { .. },
                ProducerEffect::MaterializeBatch {
                    compression,
                    deadline_operation_id,
                    ..
                }
            ] if *compression == policy && *deadline_operation_id == operation_id
        ));
    }
}

#[test]
fn compression_policy_defaults_to_uncompressed() {
    assert_eq!(CompressionPolicy::default(), CompressionPolicy::None);
}
