//! Deliberate raw generated request escape into composition.

use kafka_wire::{HeartbeatRequest, JoinGroupRequest, SyncGroupRequest};

fn escape(heartbeat: HeartbeatRequest, join: JoinGroupRequest, sync: SyncGroupRequest) {
    drop((heartbeat, join, sync));
}
