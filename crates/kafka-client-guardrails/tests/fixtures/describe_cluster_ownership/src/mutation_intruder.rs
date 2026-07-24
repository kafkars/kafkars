//! Forbidden `DescribeCluster` lifecycle and accounting mutation fixture.

struct DescribeClusterMachine {
    state: usize,
}

impl DescribeClusterMachine {
    fn advance_outside_owner(&mut self) {
        self.state += 1;
    }
}

struct DescribeClusterHost {
    operations: usize,
    completions: usize,
    retained_bytes: usize,
    published_bytes: usize,
    next_operation_id: usize,
}

impl DescribeClusterHost {
    fn update_outside_owner(&mut self) {
        self.operations += 1;
        self.completions += 1;
        self.retained_bytes += 1;
        self.published_bytes += 1;
        self.next_operation_id += 1;
    }
}

struct DescribeClusterCalls {
    calls: usize,
    settled: usize,
}

impl DescribeClusterCalls {
    fn update_outside_owner(&mut self) {
        self.calls += 1;
        self.settled += 1;
    }
}
