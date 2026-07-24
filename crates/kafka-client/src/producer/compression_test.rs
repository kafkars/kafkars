//! Public compression default and closed-choice scenarios.

use super::Compression;

#[test]
fn producer_compression_defaults_to_none() {
    assert_eq!(Compression::default(), Compression::None);
}

#[test]
fn every_public_codec_is_a_distinct_closed_choice() {
    let codecs = [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ];

    for (index, codec) in codecs.iter().enumerate() {
        assert!(!codecs[..index].contains(codec));
    }
}
