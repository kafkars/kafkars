//! Forbidden second direct engine submission owner.

fn steal<T>(engine: &T) {
    engine.try_alter_consumer_group_offsets();
}
