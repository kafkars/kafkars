//! Forbidden cloneable `DescribeCluster` lifecycle-owner fixture.

#[derive(Clone, Copy)]
struct DescribeClusterMachine;

#[derive(Clone, Copy)]
struct DescribeClusterHost;

#[derive(Clone, Copy)]
struct DescribeClusterOperation;

#[derive(Clone, Copy)]
struct DescribeClusterShardOwner;

#[derive(Clone, Copy)]
struct DescribeClusterObserver;

#[derive(Clone, Copy)]
struct DescribeClusterCalls;

#[derive(Clone, Copy)]
struct DescribeClusterCall;

#[derive(Clone, Copy)]
struct DescribeClusterCallPermit;

#[derive(Clone, Copy)]
struct SettledDescribeClusterCall;
