//! Deliberate raw generated request escape into composition.

use kafka_wire::{JoinGroupRequest, SyncGroupRequest};

fn escape(join: JoinGroupRequest, sync: SyncGroupRequest) {
    drop((join, sync));
}
