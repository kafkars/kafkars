//! Deliberately widened read-isolation vocabularies.

enum ReadIsolation {
    ReadUncommitted,
    ReadCommitted,
    Unknown,
}

enum ConsumerReadIsolation {
    ReadUncommitted,
    ReadCommitted,
    Unknown,
}
