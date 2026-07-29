//! Shared startup handoff for ShareGroup and StreamsGroup Admin owners.

use std::sync::Arc;

use crate::{
    admin::{
        AdminAlterShareGroupOffsetsPublisher, AdminDeleteShareGroupOffsetsPublisher,
        AdminDescribeShareGroupPublisher, AdminDescribeStreamsGroupPublisher,
        AdminListShareGroupOffsetsPublisher,
        alter_share_group_offsets::{
            AlterShareGroupOffsetsAdmissionPort, AlterShareGroupOffsetsHost,
            AlterShareGroupOffsetsShardOwner,
        },
        delete_share_group_offsets::{
            DeleteShareGroupOffsetsAdmissionPort, DeleteShareGroupOffsetsHost,
            DeleteShareGroupOffsetsShardOwner,
        },
        describe_share_group::{
            DescribeShareGroupAdmissionPort, DescribeShareGroupHost, DescribeShareGroupShardOwner,
        },
        describe_streams_group::{
            DescribeStreamsGroupAdmissionPort, DescribeStreamsGroupHost,
            DescribeStreamsGroupShardOwner,
        },
        list_share_group_offsets::{
            ListShareGroupOffsetsAdmissionPort, ListShareGroupOffsetsHost,
            ListShareGroupOffsetsShardOwner,
        },
    },
    driver::ReactorWake,
};

pub(super) struct StartedShareGroupOffsets {
    pub(super) delete: DeleteShareGroupOffsetsShardOwner,
    pub(super) delete_admission: DeleteShareGroupOffsetsAdmissionPort,
    pub(super) list: ListShareGroupOffsetsShardOwner,
    pub(super) list_admission: ListShareGroupOffsetsAdmissionPort,
    pub(super) alter: AlterShareGroupOffsetsShardOwner,
    pub(super) alter_admission: AlterShareGroupOffsetsAdmissionPort,
    pub(super) describe: DescribeShareGroupShardOwner,
    pub(super) describe_admission: DescribeShareGroupAdmissionPort,
    pub(super) describe_streams: DescribeStreamsGroupShardOwner,
    pub(super) describe_streams_admission: DescribeStreamsGroupAdmissionPort,
}

pub(super) fn start(
    delete_publisher: AdminDeleteShareGroupOffsetsPublisher,
    list_publisher: AdminListShareGroupOffsetsPublisher,
    alter_publisher: AdminAlterShareGroupOffsetsPublisher,
    describe_publisher: AdminDescribeShareGroupPublisher,
    describe_streams_publisher: AdminDescribeStreamsGroupPublisher,
    wake: Arc<ReactorWake>,
) -> StartedShareGroupOffsets {
    let delete = DeleteShareGroupOffsetsShardOwner::new(
        DeleteShareGroupOffsetsHost::new(delete_publisher),
        Arc::clone(&wake),
    );
    let delete_admission = delete.admission_port();
    let list = ListShareGroupOffsetsShardOwner::new(
        ListShareGroupOffsetsHost::new(list_publisher),
        Arc::clone(&wake),
    );
    let list_admission = list.admission_port();
    let alter = AlterShareGroupOffsetsShardOwner::new(
        AlterShareGroupOffsetsHost::new(alter_publisher),
        Arc::clone(&wake),
    );
    let alter_admission = alter.admission_port();
    let describe = DescribeShareGroupShardOwner::new(
        DescribeShareGroupHost::new(describe_publisher),
        Arc::clone(&wake),
    );
    let describe_admission = describe.admission_port();
    let describe_streams = DescribeStreamsGroupShardOwner::new(
        DescribeStreamsGroupHost::new(describe_streams_publisher),
        wake,
    );
    let describe_streams_admission = describe_streams.admission_port();
    StartedShareGroupOffsets {
        delete,
        delete_admission,
        list,
        list_admission,
        alter,
        alter_admission,
        describe,
        describe_admission,
        describe_streams,
        describe_streams_admission,
    }
}
