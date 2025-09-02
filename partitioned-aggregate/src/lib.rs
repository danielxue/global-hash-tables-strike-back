pub mod partitioned_aggregator;
pub mod basic_partitioned_agg;
mod base_agg;

pub use partitioned_aggregator::{PartitionedAggregator, PartitionedAggregatorFinalizer};

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::thread;
    use itertools::Itertools;
    use fastrand::Rng;

    use super::*;

    fn basic_count_test_partitioned<A: PartitionedAggregator<i32, i32, Agg=usize>>() {
        let keys = vec![1, 2, 2, 3, 3, 3, 4, 4, 4, 4];
        let values = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

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

    fn basic_max_test_partitioned<A: PartitionedAggregator<i32, i32, Agg=i32>>() {
        let keys = vec![1, 2, 2, 3, 3, 3, 4, 4, 4, 4];
        let values = vec![2, 4, 6, 8, 9, 7, 5, 3, 1, 0];

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
        assert_eq!(vals.into_iter().sorted().collect_vec(), [(1, 2), (2, 6), (3, 9), (4, 5)]);
    }

    fn thread_max_test_partitioned<A: PartitionedAggregator<i32, i32, Agg=i32>>() {
        let agg_local = A::with_capacity_and_threads(10_000, 4);

        thread::scope(|s| {
            let agg_ref = &agg_local;
            for t_id in 0..4 {
                s.spawn(move || {
                    let mut rng = Rng::with_seed(t_id);
                    for i in 0..10 {
                        let mut keys = (0..10_000).collect_vec();
                        rng.shuffle(keys.as_mut_slice());
                        let values_min = i * 1_000_000;
                        let values = (0..10_000).map(|_| rng.i32(values_min..(values_min + 1_000_000))).collect_vec();
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
        assert!(vals.iter().all(|val| { val.0 < 10_000 && val.1 >= 9_000_000 }));
    }


    fn basic_sum_test_partitioned<A: PartitionedAggregator<i32, i32, Agg=i32>>() {
        let keys = vec![1, 2, 2, 3, 3, 3, 4, 4, 4, 4];
        let values = vec![2, 4, 6, 8, 9, 7, 5, 3, 1, 0];

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
        assert_eq!(vals.into_iter().sorted().collect_vec(), [(1, 2), (2, 10), (3, 24), (4, 9)]);
    }

    fn thread_sum_test_partitioned<A: PartitionedAggregator<i32, i32, Agg=i32>>() {
        let agg_local = A::with_capacity_and_threads(10_000, 4);
        let sum = AtomicI32::new(0);
        let sum_ref = &sum;

        thread::scope(|s| {
            let agg_ref = &agg_local;
            for t_id in 0..4 {
                s.spawn(move || {
                    let mut rng = Rng::with_seed(t_id);
                    for _ in 0..10 {
                        let mut keys = (0..10_000).collect_vec();
                        rng.shuffle(keys.as_mut_slice());
                        let values = (0..10_000).map(|_| rng.i32(-10_000..10_000)).collect_vec();
                        agg_ref.aggregate_vec(&keys, &values);
                        sum_ref.fetch_add(values.iter().sum::<i32>(), Ordering::Relaxed);
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
        assert!(vals.iter().map(|(k, _)| k).all(|val| { val < &10_000 }));
        assert_eq!(vals.iter().map(|(_, v)| v).sum::<i32>(), sum.into_inner());
    }

    #[test]
    fn basic_count_test_basic() {
        basic_count_test_partitioned::<basic_partitioned_agg::CountAgg<i32, i32>>()
    }

    #[test]
    fn thread_count_test_basic() {
        thread_count_test_partitioned::<basic_partitioned_agg::CountAgg<i32, i32>>()
    }

    #[test]
    fn basic_max_test_basic() {
        basic_max_test_partitioned::<basic_partitioned_agg::MaxAgg<i32, i32>>()
    }

    #[test]
    fn thread_max_test_basic() {
        thread_max_test_partitioned::<basic_partitioned_agg::MaxAgg<i32, i32>>()
    }

    #[test]
    fn basic_sum_test_basic() {
        basic_sum_test_partitioned::<basic_partitioned_agg::SumAgg<i32, i32>>()
    }

    #[test]
    fn thread_sum_test_basic() {
        thread_sum_test_partitioned::<basic_partitioned_agg::SumAgg<i32, i32>>()
    }
}