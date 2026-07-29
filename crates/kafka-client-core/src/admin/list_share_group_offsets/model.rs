//! Declarative facade for validated singular and batched API-90 request intent.

mod error;
mod plan;
mod query;

pub use error::ListShareGroupOffsetsPlanError;
pub(crate) use plan::ListShareGroupOffsetsPlanShape;
pub use plan::{LIST_SHARE_GROUP_OFFSETS_MAX_GROUPS, ListShareGroupOffsetsPlan};
pub use query::{
    LIST_SHARE_GROUP_OFFSETS_MAX_GROUP_ID_BYTES, LIST_SHARE_GROUP_OFFSETS_MAX_REQUEST_TEXT_BYTES,
    LIST_SHARE_GROUP_OFFSETS_MAX_SELECTED_PARTITIONS,
    LIST_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES, ListShareGroupOffsetTarget,
    ListShareGroupOffsetsQuery, ListShareGroupOffsetsSelection,
};
