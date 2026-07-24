//! Curated crate-private admin execution exports.

pub(crate) use super::completion::{
    AdminCompletionNotifier, AdminCompletionPorts, CreatePartitionsPublisher,
    CreateTopicsPublisher, DeleteTopicsPublisher, DescribeClusterPublisher,
    DescribeTopicsPublisher,
};
