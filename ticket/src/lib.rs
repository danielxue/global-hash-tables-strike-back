#![feature(portable_simd)]

mod cuckoo_ticketer;
mod dash_ticketer;
mod folklore_ticketer;
mod folklore_unfuzzy_ticketer;
mod global_locking_ticketer;
mod iceberg_ticketer;
mod keyed_folklore_ticketer;
mod leap_ticketer;
mod once_lock_ticketer;
mod ticketer;

pub use cuckoo_ticketer::CuckooTicketer;
pub use dash_ticketer::DashTicketer;
pub use folklore_ticketer::FolkloreTicketer;
pub use folklore_unfuzzy_ticketer::FolkloreUnfuzzyTicketer;
pub use global_locking_ticketer::GlobalLockingTicketer;
pub use iceberg_ticketer::IcebergTicketer;
pub use keyed_folklore_ticketer::KeyedFolkloreTicketer;
pub use leap_ticketer::LeapTicketer;
pub use once_lock_ticketer::OnceLockHashMap;
pub use ticketer::{KeyedTicketer, Ticketer};

#[cfg(test)]
mod tests {
    use std::{sync::atomic::AtomicI32, thread};

    use itertools::Itertools;
    use rand::prelude::SliceRandom;
    use rand::rngs::SmallRng;
    use rand::SeedableRng;
    use common::FuzzyCounter;
    use super::*;

    fn basic_test<T: Ticketer<i32>>() {
        let keys = vec![0, 1, 2, 3, 4, 5, 6, 7];
        let mut out = vec![0; 8];

        let hm = T::with_capacity_and_threads(8, 1);
        hm.ticket(&keys, &mut out);
        assert_eq!(out.iter().sorted().dedup().count(), 8);

        let mut out2 = vec![0; 8];
        hm.ticket(&keys, &mut out2);
        assert_eq!(out, out2);

        hm.ticket(&[8, 8, 8, 8], &mut out);
        assert_eq!(out[0..4], [8, 8, 8, 8]);
    }

    fn grow_test<T: Ticketer<i32>>() {
        let hm = T::with_capacity_and_threads(8, 1);

        let keys = (0..128).collect_vec();
        let mut out = vec![0; 128];
        hm.ticket(keys.as_slice(), &mut out);
        assert_eq!(out.iter().sorted().dedup().count(), 128);

        let keys = (128..1024).collect_vec();
        let mut out = vec![0; 1024 - 128];
        keys.chunks(4)
            .zip(out.chunks_mut(4))
            .for_each(|(k, o)| hm.ticket(k, o));
        assert_eq!(out.iter().sorted().dedup().count(), 1024 - 128);

        let kvs = hm.into_kvs();
        assert_eq!(kvs.len(), 1024);
    }

    fn thread_test<T: Ticketer<i32>>() {
        const KEYS: usize = 10_000;
        const THREADS: usize = 4;
        const CHUNKS: usize = 10;
        const CHUNK_SIZE: usize = 100_000;

        let hm = T::with_capacity_and_threads(KEYS, 4);
        thread::scope(|s| {
            let hm_ref = &hm;
            for t_id in 0..THREADS {
                s.spawn(move || {
                    let mut rng = SmallRng::from_seed([t_id as u8; 32]);
                    let mut out = vec![0; CHUNK_SIZE];
                    let mut keys = (0..((CHUNKS * CHUNK_SIZE) as i32))
                        .map(|k| k % (KEYS as i32))
                        .collect_vec();
                    keys.shuffle(&mut rng);
                    keys.chunks(CHUNK_SIZE)
                        .for_each(|chunk| {
                            hm_ref.ticket(&chunk, &mut out);
                        })
                });
            }
        });

        let kvs = hm.into_kvs();
        assert!(kvs.iter().all(|(k, _v)| { *k < KEYS as i32 }));
        assert!(kvs.iter().all(|(_k, v)| { *v < KEYS + FuzzyCounter::DEFAULT_STEP_SIZE * THREADS }));
        assert_eq!(kvs.iter().map(|kv| kv.0).sorted().dedup().count(), KEYS);
        assert_eq!(kvs.iter().map(|kv| kv.1).sorted().dedup().count(), KEYS);
    }

    fn grow_thread_test<T: Ticketer<i32>>() {
        const KEYS: usize = 10_000;
        const THREADS: usize = 4;
        const CHUNKS: usize = 10;
        const CHUNK_SIZE: usize = 100_000;

        let hm = T::with_capacity_and_threads(100, 4);
        thread::scope(|s| {
            let hm_ref = &hm;
            for t_id in 0..THREADS {
                s.spawn(move || {
                    let mut rng = SmallRng::from_seed([t_id as u8; 32]);
                    let mut out = vec![0; CHUNK_SIZE];
                    let mut keys = (0..((CHUNKS * CHUNK_SIZE) as i32))
                        .map(|k| k % (KEYS as i32))
                        .collect_vec();
                    keys.shuffle(&mut rng);
                    keys.chunks(CHUNK_SIZE)
                        .for_each(|chunk| {
                            hm_ref.ticket(&chunk, &mut out);
                        })
                });
            }
        });

        let kvs = hm.into_kvs();
        assert!(kvs.iter().all(|(k, _v)| { *k < KEYS as i32 }));
        assert!(kvs.iter().all(|(_k, v)| { *v < KEYS + FuzzyCounter::DEFAULT_STEP_SIZE * THREADS }));
        assert_eq!(kvs.iter().map(|kv| kv.0).sorted().dedup().count(), KEYS);
        assert_eq!(kvs.iter().map(|kv| kv.1).sorted().dedup().count(), KEYS);
    }

    fn keyed_test<T: KeyedTicketer<i32>>() {
        const KEYS: usize = 10_000;
        const THREADS: usize = 4;
        const CHUNKS: usize = 10;
        const CHUNK_SIZE: usize = 100_000;

        let hm = T::with_capacity_and_threads(KEYS, 4);
        thread::scope(|s| {
            let hm_ref = &hm;
            for t_id in 0..THREADS {
                s.spawn(move || {
                    let mut rng = SmallRng::from_seed([t_id as u8; 32]);
                    let mut out = vec![0; CHUNK_SIZE];
                    let mut keys = (0..((CHUNKS * CHUNK_SIZE) as i32))
                        .map(|k| k % (KEYS as i32))
                        .collect_vec();
                    keys.shuffle(&mut rng);
                    keys.chunks(CHUNK_SIZE)
                        .for_each(|chunk| {
                            hm_ref.ticket(&chunk, &mut out);
                        })
                });
            }
        });

        let (keys, _finalizer) = hm.into_keys();
        assert!(keys.iter().all(|k| { *k < KEYS as i32 }));
        assert_eq!(keys.len(), KEYS);
        assert_eq!(keys.iter().sorted().dedup().count(), KEYS);
    }

    // Cuckoo. Resizing not supported yet.
    #[test]
    fn basic_test_cuckoo() {
        basic_test::<CuckooTicketer<i32>>();
    }

    #[test]
    fn thread_test_cuckoo() {
        thread_test::<CuckooTicketer<i32>>();
    }

    // Dash.
    #[test]
    fn basic_test_dash() {
        basic_test::<DashTicketer<i32>>();
    }

    #[test]
    fn grow_test_dash() {
        grow_test::<DashTicketer<i32>>();
    }

    #[test]
    fn thread_test_dash() {
        thread_test::<DashTicketer<i32>>();
    }

    #[test]
    fn grow_thread_test_dash() {
        grow_thread_test::<DashTicketer<i32>>();
    }

    // Folklore.
    #[test]
    fn basic_test_folklore() {
        basic_test::<FolkloreTicketer<AtomicI32>>();
    }

    #[test]
    fn grow_test_folklore() {
        grow_test::<FolkloreTicketer<AtomicI32>>();
    }

    #[test]
    fn thread_test_folklore() {
        thread_test::<FolkloreTicketer<AtomicI32>>();
    }

    #[test]
    fn grow_thread_test_folklore() {
        grow_thread_test::<FolkloreTicketer<AtomicI32>>();
    }

    // Folklore Unfuzzy.
    #[test]
    fn basic_test_folklore_unfuzzy() {
        basic_test::<FolkloreUnfuzzyTicketer<AtomicI32>>();
    }

    #[test]
    fn grow_test_folklore_unfuzzy() {
        grow_test::<FolkloreUnfuzzyTicketer<AtomicI32>>();
    }

    #[test]
    fn thread_test_folklore_unfuzzy() {
        thread_test::<FolkloreUnfuzzyTicketer<AtomicI32>>();
    }

    #[test]
    fn grow_thread_test_folklore_unfuzzy() {
        grow_thread_test::<FolkloreUnfuzzyTicketer<AtomicI32>>();
    }

    // Global Locking.
    #[test]
    fn basic_test_global_locking() {
        basic_test::<GlobalLockingTicketer<i32>>();
    }

    #[test]
    fn grow_test_global_locking() {
        grow_test::<GlobalLockingTicketer<i32>>();
    }

    #[test]
    fn thread_test_global_locking() {
        thread_test::<GlobalLockingTicketer<i32>>();
    }

    #[test]
    fn grow_thread_test_global_locking() {
        grow_thread_test::<GlobalLockingTicketer<i32>>();
    }

    // Iceberg. Growing not supported yet but passes tests using overflow.
    #[test]
    fn basic_test_iceberg_hash() {
        basic_test::<IcebergTicketer<i32>>();
    }

    #[test]
    fn grow_test_iceberg_hash() {
        grow_test::<IcebergTicketer<i32>>();
    }

    #[test]
    fn thread_test_iceberg_hash() {
        thread_test::<IcebergTicketer<i32>>();
    }

    #[test]
    fn grow_thread_test_iceberg_hash() {
        grow_thread_test::<IcebergTicketer<i32>>();
    }

    // Leap.
    #[test]
    fn basic_test_leap_hash() {
        basic_test::<LeapTicketer<i32>>();
    }

    #[test]
    fn grow_test_leap_hash() {
        grow_test::<LeapTicketer<i32>>();
    }

    #[test]
    fn thread_test_leap_hash() {
        thread_test::<LeapTicketer<i32>>();
    }

    #[test]
    fn grow_thread_test_leap_hash() {
        grow_thread_test::<LeapTicketer<i32>>();
    }

    // Once Lock.
    #[test]
    fn basic_test_once_lock() {
        basic_test::<OnceLockHashMap<i32>>();
    }

    #[test]
    fn grow_test_once_lock() {
        grow_test::<OnceLockHashMap<i32>>();
    }

    #[test]
    fn thread_test_once_lock() {
        thread_test::<OnceLockHashMap<i32>>();
    }

    #[test]
    fn grow_thread_test_once_lock() {
        grow_thread_test::<OnceLockHashMap<i32>>();
    }

    // Keyed Folklore
    #[test]
    fn basic_test_keyed_folklore() {
        basic_test::<KeyedFolkloreTicketer<AtomicI32>>();
    }

    #[test]
    fn grow_test_keyed_folklore() {
        grow_test::<KeyedFolkloreTicketer<AtomicI32>>();
    }

    #[test]
    fn thread_test_keyed_folklore() {
        thread_test::<KeyedFolkloreTicketer<AtomicI32>>();
    }

    #[test]
    fn grow_thread_test_keyed_folklore() {
        grow_thread_test::<KeyedFolkloreTicketer<AtomicI32>>();
    }

    #[test]
    fn keyed_test_keyed_folklore() {
        keyed_test::<KeyedFolkloreTicketer<AtomicI32>>();
    }
}
