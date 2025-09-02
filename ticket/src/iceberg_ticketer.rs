use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};
use std::simd::{LaneCount, Mask, MaskElement, Simd, SimdElement, SupportedLaneCount};
use std::simd::prelude::SimdPartialEq;
use std::simd::num::SimdInt;
use std::sync::{Mutex, RwLock, RwLockReadGuard};

use bytemuck::{zeroed_vec, Zeroable};
use fnv::FnvBuildHasher;
use itertools::Itertools;

use crate::ticketer::Ticketer;
use common::{FuzzyCounter, LocalCounter, SyncUnsafeCell};

const LV1_BUCKET_SIZE: usize = 32;
const LV2_BUCKET_SIZE: usize = 8;

#[derive(Zeroable)]
struct IcebergBucket<K: Zeroable + Default, const BUCKET_SIZE: usize> {
    metadata: [SyncUnsafeCell<u8>; BUCKET_SIZE],
    keys: [SyncUnsafeCell<K>; BUCKET_SIZE],
    tickets: [SyncUnsafeCell<usize>; BUCKET_SIZE],
}

impl<K: Default + Zeroable, const BUCKET_SIZE: usize>  Default for IcebergBucket<K, BUCKET_SIZE> {
    fn default() -> Self {
        Self {
            metadata: core::array::from_fn(|_| Default::default()),
            keys: core::array::from_fn(|_| Default::default()),
            tickets: core::array::from_fn(|_| Default::default()),
        }
    }
}

pub struct IcebergTicketer<K: Zeroable + Default, S = FnvBuildHasher>
where
    S: BuildHasher + Send + Default,
{
    lv1_table: RwLock<Vec<IcebergBucket<K, LV1_BUCKET_SIZE>>>,
    lv1_mutexes: Vec<Mutex<()>>,
    // This is slightly non-canonical because second level uses mutexes instead of atomics due to
    // issues with implementation in Rust, but not reached enough to make a big a difference for benchmark.
    lv2_table: RwLock<Vec<IcebergBucket<K, LV2_BUCKET_SIZE>>>,
    lv2_mutexes: Vec<Mutex<()>>,
    // Resizing has not been implemented, so overflow is used in place.
    overflow: RwLock<HashMap<K, usize, S>>,
    ticketer: FuzzyCounter,
    bh: S,
}

enum LookupResult {
    Some(usize),
    Overflow,
    Insert,
}

impl<K: Eq + Clone + Zeroable + Default, S: BuildHasher + Send + Sync + Default> IcebergTicketer<K, S> {
    const LF: f64 = 0.85;

    unsafe fn lookup<const BUCKET_SIZE: usize>(
        &self,
        key: &K,
        hash: usize,
        bucket: &IcebergBucket<K, BUCKET_SIZE>,
    ) -> LookupResult
    where LaneCount<BUCKET_SIZE>: SupportedLaneCount
    {
        let metadata = std::mem::transmute::<&[SyncUnsafeCell<u8>; BUCKET_SIZE], &[u8; BUCKET_SIZE]>(&bucket.metadata);
        let fps = Simd::from_slice(metadata);
        let fp = (hash as u8).saturating_add(1);
        let first_match = Simd::<u8, BUCKET_SIZE>::splat(fp)
            .simd_eq(fps.clone())
            .first_set()
            .unwrap_or(BUCKET_SIZE);
        if first_match < BUCKET_SIZE && &*bucket.keys[first_match].get() == key {
            return LookupResult::Some(*bucket.tickets[first_match].get());
        } else if first_match == BUCKET_SIZE {
            let first_empty = Simd::<u8, BUCKET_SIZE>::splat(0)
                .simd_eq(fps)
                .first_set()
                .unwrap_or(BUCKET_SIZE);
            if first_empty < BUCKET_SIZE {
                return LookupResult::Insert;
            }
        }
        LookupResult::Overflow
    }

    // Assumes calling function has mutex.
    unsafe fn insert<const BUCKET_SIZE: usize>(
        &self,
        key: &K,
        hash: usize,
        bucket: &IcebergBucket<K, BUCKET_SIZE>,
        counter: &mut LocalCounter,
    ) -> Option<usize>
    where LaneCount<BUCKET_SIZE>: SupportedLaneCount
    {
        let metadata = std::mem::transmute::<&[SyncUnsafeCell<u8>; BUCKET_SIZE], &[u8; BUCKET_SIZE]>(&bucket.metadata);
        let fps = Simd::from_slice(metadata);
        let fp = (hash as u8).saturating_add(1);
        let first_match = Simd::<u8, BUCKET_SIZE>::splat(fp)
            .simd_eq(fps)
            .first_set()
            .unwrap_or(BUCKET_SIZE);
        if first_match < BUCKET_SIZE && &*bucket.keys[first_match].get() == key {
            return Some(*bucket.tickets[first_match].get());
        } else if first_match == BUCKET_SIZE {
            let first_empty = Simd::<u8, BUCKET_SIZE>::splat(0)
                .simd_eq(fps.clone())
                .first_set()
                .unwrap_or(BUCKET_SIZE);
            if first_empty < BUCKET_SIZE {
                let ticket = self.ticketer.fetch_increment(counter);
                *bucket.keys[first_empty].get() = key.clone();
                *bucket.tickets[first_empty].get() = ticket;
                *bucket.metadata[first_empty].get() = fp;
                return Some(ticket);
            }
        }

        None
    }

    unsafe fn insert_lv1(
        &self,
        key: &K,
        hash: usize,
        bucket: &IcebergBucket<K, LV1_BUCKET_SIZE>,
        mutex: &Mutex<()>,
        counter: &mut LocalCounter,
    ) -> Option<usize> {
        let _mutex = mutex.lock();
        self.insert::<LV1_BUCKET_SIZE>(key, hash, bucket, counter)
    }

    unsafe fn insert_lv2(
        &self,
        key: &K,
        hashes: (usize, usize),
        table: &RwLockReadGuard<Vec<IcebergBucket<K, LV2_BUCKET_SIZE>>>,
        counter: &mut LocalCounter,
    ) -> Option<usize> {
        let idxs = ((hashes.0 >> 8) % table.len(), (hashes.1 >> 8) % table.len());
        if idxs.0 == idxs.1 {
            let bucket = &table[idxs.0];
            let _lock = self.lv2_mutexes[idxs.0].lock();
            return self.insert::<LV2_BUCKET_SIZE>(key, hashes.0, bucket, counter);
        }

        let mut hashes = hashes;
        let mut buckets = (&table[idxs.0], &table[idxs.1]);
        let mut mutexes = (&self.lv2_mutexes[idxs.0], &self.lv2_mutexes[idxs.1]);
        if idxs.1 < idxs.0 {
            std::mem::swap(&mut hashes.0, &mut hashes.1);
            std::mem::swap(&mut buckets.0, &mut buckets.1);
            std::mem::swap(&mut mutexes.0, &mut mutexes.1);
        }

        let _mutex0 = mutexes.0.lock();
        let _mutex1 = mutexes.1.lock();

        let metadata0 = std::mem::transmute::<&[SyncUnsafeCell<u8>; LV2_BUCKET_SIZE], &[u8; LV2_BUCKET_SIZE]>(&buckets.0.metadata);
        let metadata1 = std::mem::transmute::<&[SyncUnsafeCell<u8>; LV2_BUCKET_SIZE], &[u8; LV2_BUCKET_SIZE]>(&buckets.1.metadata);
        let fps0 = Simd::from_slice(metadata0);
        let fps1 = Simd::from_slice(metadata1);
        let cnt0 = -Simd::<u8, LV2_BUCKET_SIZE>::splat(0)
            .simd_eq(fps0.clone())
            .to_int()
            .reduce_sum();
        let cnt1 = -Simd::<u8, LV2_BUCKET_SIZE>::splat(0)
            .simd_eq(fps1.clone())
            .to_int()
            .reduce_sum();
        if cnt1 < cnt0 {
            std::mem::swap(&mut hashes.0, &mut hashes.1);
            std::mem::swap(&mut buckets.0, &mut buckets.1);
        }

        match self.insert::<LV2_BUCKET_SIZE>(key, hashes.0, buckets.0, counter) {
            None => {
                match self.insert::<LV2_BUCKET_SIZE>(key, hashes.1, buckets.1, counter) {
                    None => {}
                    Some(ticket) => { return Some(ticket); }
                }
            }
            Some(ticket) => { return Some(ticket); }
        }

        None
    }
}

impl<
    K: Eq + Hash + Default + Copy + Sync + Send + Zeroable + SimdElement + MaskElement + PartialEq,
    S: BuildHasher + Send + Sync + Default
> Ticketer<K> for IcebergTicketer<K, S>
where
    Simd<K, LV1_BUCKET_SIZE>: SimdPartialEq<Mask=Mask<K, LV1_BUCKET_SIZE>>,
{
    fn with_capacity_and_threads(capacity: usize, _threads: usize) -> Self {
        let lv1_size = (capacity.div_ceil(LV1_BUCKET_SIZE) as f64 / Self::LF).ceil() as usize;
        let lv2_size = (capacity.div_ceil(LV1_BUCKET_SIZE) as f64 / Self::LF).ceil() as usize;

        let lv1_table = (0..lv1_size).map(|_| Default::default()).collect_vec();
        let lv2_table = (0..lv2_size).map(|_| Default::default()).collect_vec();

        IcebergTicketer {
            lv1_table: RwLock::new(lv1_table),
            lv1_mutexes: (0..lv1_size).map(|_| Default::default()).collect_vec(),
            lv2_table: RwLock::new(lv2_table),
            lv2_mutexes: (0..lv2_size).map(|_| Default::default()).collect_vec(),
            overflow: Default::default(),
            ticketer: Default::default(),
            bh: Default::default(),
        }
    }

    fn with_capacity_and_threads_zeroed(capacity: usize, _threads: usize) -> Self {
        let lv1_size = (capacity.div_ceil(LV1_BUCKET_SIZE) as f64 / Self::LF).ceil() as usize;
        let lv2_size = (capacity.div_ceil(LV1_BUCKET_SIZE) as f64 / Self::LF).ceil() as usize;

        IcebergTicketer {
            lv1_table: RwLock::new(zeroed_vec(lv1_size)),
            lv1_mutexes: (0..lv1_size).map(|_| Default::default()).collect_vec(),
            lv2_table: RwLock::new(zeroed_vec(lv2_size)),
            lv2_mutexes: (0..lv2_size).map(|_| Default::default()).collect_vec(),
            overflow: Default::default(),
            ticketer: Default::default(),
            bh: Default::default(),
        }
    }

    fn ticket(&self, keys: &[K], output: &mut [usize]) {
        keys.iter()
            .zip(output.iter_mut())
            .for_each(|(k, o)| {
                let mut hasher = self.bh.build_hasher();
                k.hash(&mut hasher);
                *o = hasher.finish() as usize;
            });
        let mut counter = self.ticketer.get_thread_counter();

        let lv1_table = self.lv1_table.read().unwrap();
        let lv2_table = self.lv2_table.read().unwrap();
        'outer: for idx in 0..keys.len() {
            let hash = output[idx];
            let bucket_idx = (hash >> 8) % lv1_table.len();
            let lv1_bucket = &lv1_table[bucket_idx];
            let lv1_mutex = &self.lv1_mutexes[bucket_idx];

            // Fast path lockless lookup.
            'lookup: {
                unsafe {
                    match self.lookup::<LV1_BUCKET_SIZE>(&keys[idx], hash, lv1_bucket) {
                        LookupResult::Some(ticket) => {
                            output[idx] = ticket;
                            continue 'outer;
                        },
                        LookupResult::Insert => { break 'lookup; }
                        LookupResult::Overflow => {}
                    }

                    let mut lv2_insert = false;
                    let mut hasher = self.bh.build_hasher();
                    hash.hash(&mut hasher);
                    let h1 = hasher.finish() as usize;
                    let lv2_bucket_1 = &lv2_table[(h1 >> 8) % lv2_table.len()];
                    match self.lookup::<LV2_BUCKET_SIZE>(&keys[idx], h1, lv2_bucket_1) {
                        LookupResult::Some(ticket) => {
                            output[idx] = ticket;
                            continue 'outer;
                        },
                        LookupResult::Overflow => {},
                        LookupResult::Insert => { lv2_insert = true; }
                    }

                    let mut hasher = self.bh.build_hasher();
                    h1.hash(&mut hasher);
                    let h2 = hasher.finish() as usize;
                    let lv2_bucket_2 = &lv2_table[(h2 >> 8) % lv2_table.len()];
                    match self.lookup::<LV2_BUCKET_SIZE>(&keys[idx], h2, lv2_bucket_2) {
                        LookupResult::Some(ticket) => {
                            output[idx] = ticket;
                            continue 'outer;
                        },
                        LookupResult::Overflow => {},
                        LookupResult::Insert => { lv2_insert = true; }
                    }

                    if lv2_insert {
                        break 'lookup;
                    }

                    let overflow = self.overflow.read().unwrap();
                    match overflow.get(&keys[idx]) {
                        Some(ticket) => {
                            output[idx] = *ticket;
                            continue 'outer;
                        }
                        None => {}
                    }
                }
            }

            // Full insert/lookup.
            unsafe {
                match self.insert_lv1(&keys[idx], hash, lv1_bucket, &lv1_mutex, &mut counter) {
                    Some(ticket) => {
                        output[idx] = ticket;
                    },
                    None =>  {
                        let mut hasher = self.bh.build_hasher();
                        hash.hash(&mut hasher);
                        let h1 = hasher.finish() as usize;

                        let mut hasher = self.bh.build_hasher();
                        h1.hash(&mut hasher);
                        let h2 = hasher.finish() as usize;

                        match self.insert_lv2(&keys[idx], (h1, h2), &lv2_table, &mut counter) {
                            Some(ticket) => {
                                output[idx] = ticket;
                            },
                            None => {
                                let mut overflow = self.overflow.write().unwrap();
                                match overflow.get(&keys[idx]) {
                                    Some(ticket) => {
                                        output[idx] = *ticket;
                                    }
                                    None => {
                                        let ticket = self.ticketer.fetch_increment(&mut counter);
                                        overflow.insert(keys[idx], ticket);
                                        output[idx] = ticket;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn into_kvs(self) -> Vec<(K, usize)> {
        self.lv1_table
            .into_inner()
            .unwrap()
            .into_iter()
            .flat_map(|bucket| {
                let metadata = bucket.metadata
                    .into_iter()
                    .map(|cell| cell.into_inner())
                    .collect_vec();
                bucket.keys
                    .into_iter()
                    .zip(bucket.tickets.into_iter())
                    .enumerate()
                    .filter_map(move |(idx, kv)| {
                        return if metadata[idx] > 0 {
                            Some((kv.0.into_inner(), kv.1.into_inner()))
                        } else {
                            None
                        }
                    })
            })
            .chain(self.lv2_table
                .into_inner()
                .unwrap()
                .into_iter()
                .flat_map(|bucket| {
                    let metadata = bucket.metadata
                        .into_iter()
                        .map(|cell| cell.into_inner())
                        .collect_vec();
                    bucket.keys
                        .into_iter()
                        .zip(bucket.tickets.into_iter())
                        .enumerate()
                        .filter_map(move |(idx, kv)| {
                            return if metadata[idx] > 0 {
                                Some((kv.0.into_inner(), kv.1.into_inner()))
                            } else {
                                None
                            }
                        })
                })
            )
            .chain(self.overflow.into_inner().unwrap().into_iter())
            .collect_vec()
    }
}
