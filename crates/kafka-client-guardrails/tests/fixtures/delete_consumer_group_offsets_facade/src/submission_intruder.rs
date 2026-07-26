//! Forbidden second direct engine submission owner.

fn steal<T>(engine: &T) {
    engine.try_delete_consumer_group_offsets();
}
