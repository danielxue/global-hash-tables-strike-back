#![feature(portable_simd)]

mod updater;
pub mod atomic_pau;
pub mod global_locking_agg;
pub mod locking_agg;
pub mod thread_local_agg;

pub use updater::Updater;

#[cfg(test)]
mod tests {
    use std::thread;
    use std::sync::atomic::AtomicI32;
    use itertools::Itertools;
    use fastrand::Rng;
    use super::*;

    fn basic_count_test<A: Updater<i32, Agg=usize>>() {
        let tickets = vec![1, 2, 2, 3, 3, 3, 4, 4, 4, 4];
        let keys = vec![2, 4, 6, 8, 9, 7, 5, 3, 1, 0];

        let agg = A::with_capacity_and_threads(8, 1);
        agg.update_vec(&tickets, &keys);

        let vals = agg.into_vec();
        assert_eq!(vals.len(), 8);
        assert_eq!(vals, [0, 1, 2, 3, 4, 0, 0, 0]);
    }

    fn thread_count_test<A: Updater<i32, Agg=usize>>() {
        let agg = A::with_capacity_and_threads(10_000, 4);

        thread::scope(|s| {
            let agg_ref = &agg;
            for t_id in 0..4 {
                s.spawn(move || {
                    let mut rng = Rng::with_seed(t_id);
                    for _ in 0..10 {
                        let tickets = (0..10_000).map(|_| rng.i32(0..10_000) as usize).collect_vec();
                        let keys = (0..10_000).map(|_| rng.i32(0..1_000_00)).collect_vec();
                        agg_ref.update_vec(&tickets, &keys);
                    }
                });
            }
        });

        let vals = agg.into_vec();
        assert_eq!(vals.len(), 10_000);
        assert_eq!(vals.iter().sum::<usize>(), 400_000);
    }

    fn basic_max_test<A: Updater<i32, Agg=i32>>() {
        let tickets = vec![1, 2, 2, 3, 3, 3, 4, 4, 4, 4];
        let keys = vec![2, 4, 6, 8, 9, 7, 5, 3, 1, 0];

        let agg = A::with_capacity_and_threads(8, 1);
        agg.update_vec(&tickets, &keys);

        let vals = agg.into_vec();
        assert_eq!(vals.len(), 8);
        assert_eq!(vals, [0, 2, 6, 9, 5, 0, 0, 0]);
    }

    fn thread_max_test<A: Updater<i32, Agg=i32>>() {
        let agg = A::with_capacity_and_threads(10_000, 4);

        thread::scope(|s| {
            let agg_ref = &agg;
            for t_id in 0..4 {
                s.spawn(move || {
                    let mut rng = Rng::with_seed(t_id);
                    for i in 0..10 {
                        let mut tickets = (0..10_000).collect_vec();
                        rng.shuffle(tickets.as_mut_slice());
                        let key_min = i * 1_000_000;
                        let keys = (0..10_000).map(|_| rng.i32(key_min..(key_min + 1_000_000))).collect_vec();
                        agg_ref.update_vec(&tickets, &keys);
                    }
                });
            }
        });

        let vals = agg.into_vec();
        assert_eq!(vals.len(), 10_000);
        assert!(vals.iter().all(|val| { *val >= 9_000_000 }));
    }

    // Atomic.
    #[test]
    fn basic_count_test_atomic() {
        basic_count_test::<atomic_pau::CountUpdater<i32>>();
    }

    #[test]
    fn thread_count_test_atomic() {
        thread_count_test::<atomic_pau::CountUpdater<i32>>();
    }

    #[test]
    fn basic_max_test_atomic() {
        basic_max_test::<atomic_pau::MaxUpdater<AtomicI32>>();
    }

    #[test]
    fn thread_max_test_atomic() {
        thread_max_test::<atomic_pau::MaxUpdater<AtomicI32>>();
    }

    // Locking.
    #[test]
    fn basic_count_test_locking() {
        basic_count_test::<locking_agg::CountUpdater<i32>>();
    }

    #[test]
    fn thread_count_test_locking() {
        thread_count_test::<locking_agg::CountUpdater<i32>>();
    }

    #[test]
    fn basic_max_test_locking() {
        basic_max_test::<locking_agg::MaxUpdater<i32>>();
    }

    #[test]
    fn thread_max_test_locking() {
        thread_max_test::<locking_agg::MaxUpdater<i32>>();
    }

    // Global locking.
    #[test]
    fn basic_count_test_global_locking() {
        basic_count_test::<global_locking_agg::CountUpdater<i32>>();
    }

    #[test]
    fn thread_count_test_global_locking() {
        thread_count_test::<global_locking_agg::CountUpdater<i32>>();
    }

    // Thread local.
    #[test]
    fn basic_count_test_thread_local() {
        basic_count_test::<thread_local_agg::CountUpdater<i32>>();
    }

    #[test]
    fn thread_count_test_thread_local() {
        thread_count_test::<thread_local_agg::CountUpdater<i32>>();
    }
}