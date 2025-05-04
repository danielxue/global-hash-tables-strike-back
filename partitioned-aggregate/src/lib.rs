pub mod ad_hoc_resizing_basic_partitioned_agg;
pub mod partitioned_aggregator;
pub mod basic_partitioned_agg;
pub mod linear_probing_partitioned_agg;
pub mod no_spill_partitioned_agg;
pub mod paginated_partitioned_agg;

pub use partitioned_aggregator::{PartitionedAggregator, PartitionedAggregatorFinalizer};

#[cfg(test)]
mod tests {
    use std::thread;
    use itertools::Itertools;
    use fastrand::Rng;

    use super::*;

    fn basic_count_test_partitioned<A: PartitionedAggregator<i32, i32, Agg=usize>>() {
        let keys = vec![1, 2, 2, 3, 3, 3, 4, 4, 4, 4];
        let values = vec![0, 0, 0, 0, 0, 0, 0, 0, 0];

        let agg_local = A::with_capacity_and_threads(8, 1);
        agg_local.aggregate_vec(&keys, &values);
        agg_local.finalize_thread();
        let agg_partition = agg_local.into_finalizer();
        agg_partition.finalize_thread();

        let vals_col = agg_partition.into_vec();
        let vals = vals_col.0.into_iter()
            .zip(vals_col.1.into_iter())
            .map(|(k, v)| (k, v))
            .collect_vec();
        assert_eq!(vals.len(), 4);
        assert_eq!(vals.into_iter().sorted().collect_vec(), [(1, 1), (2, 2), (3, 3), (4, 4)]);
    }

    fn thread_count_test_partitioned<A: PartitionedAggregator<i32, i32, Agg=usize>>() {
        let agg_local = A::with_capacity_and_threads(10_000, 4);

        thread::scope(|s| {
            let agg_ref = &agg_local;
            for t_id in 0..4 {
                s.spawn(move || {
                    let mut rng = Rng::with_seed(t_id);
                    for _ in 0..10 {
                        let keys = (0..10_000).map(|_| rng.i32(0..10_000)).collect_vec();
                        let values = (0..10_000).map(|_| rng.i32(0..1_000_00)).collect_vec();
                        agg_ref.aggregate_vec(&keys, &values);
                    }
                    agg_ref.finalize_thread();
                });
            }
        });

        let agg_partition = agg_local.into_finalizer();
        thread::scope(|s| {
            let agg_ref = &agg_partition;
            for _ in 0..4 {
                s.spawn(move || {
                    agg_ref.finalize_thread()
                });
            }
        });

        let vals_col = agg_partition.into_vec();
        let vals = vals_col.0.into_iter()
            .zip(vals_col.1.into_iter())
            .map(|(k, v)| (k, v))
            .collect_vec();
        assert_eq!(vals.len(), 10_000);
        assert_eq!(vals.iter().map(|(_k, v)| v).sum::<usize>(), 400_000);
    }

    // Basic.
    #[test]
    fn basic_count_test_basic() {
        basic_count_test_partitioned::<basic_partitioned_agg::CountAgg<i32, i32>>()
    }

    #[test]
    fn thread_count_test_basic() {
        thread_count_test_partitioned::<basic_partitioned_agg::CountAgg<i32, i32>>()
    }

    // Linear Probing.
    #[test]
    fn basic_count_test_linear_probing() {
        basic_count_test_partitioned::<linear_probing_partitioned_agg::CountAgg<i32, i32>>()
    }

    #[test]
    fn thread_count_test_linear_probing() {
        thread_count_test_partitioned::<linear_probing_partitioned_agg::CountAgg<i32, i32>>()
    }

    // No Spill.
    #[test]
    fn basic_count_test_no_spill() {
        basic_count_test_partitioned::<no_spill_partitioned_agg::CountAgg<i32, i32>>()
    }

    #[test]
    fn thread_count_test_no_spill() {
        thread_count_test_partitioned::<no_spill_partitioned_agg::CountAgg<i32, i32>>()
    }

    // Paginated.
    #[test]
    fn basic_count_test_paginated() {
        basic_count_test_partitioned::<paginated_partitioned_agg::CountAgg<i32, i32>>()
    }

    #[test]
    fn thread_count_test_paginated() {
        thread_count_test_partitioned::<paginated_partitioned_agg::CountAgg<i32, i32>>()
    }
}