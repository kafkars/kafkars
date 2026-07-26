//! Deliberate foreign construction and attempt-preparation authority.

fn steal<T>(clock: &T) {
    let _ = DirectFetchExecutor::create_unbound(1, 1, 1);
    let _ = FetchAttemptDeadline::capture_for_fetch(todo!(), clock, todo!());
    let _ = PreparedFetchExecution::new_retaining_attempt(
        todo!(),
        todo!(),
        todo!(),
        todo!(),
        todo!(),
        1,
    );
    let _ = PartitionFetchRequest::from_fetch_ready_parts(
        todo!(),
        todo!(),
        todo!(),
        todo!(),
        todo!(),
        todo!(),
    );
}
