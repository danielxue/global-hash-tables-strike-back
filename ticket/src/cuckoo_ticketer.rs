#![allow(clippy::needless_return)]

//! A concurrent bucketized-cuckoo hash map which supports a "write" phase
//! and a "read" phase, perfect for hash joins or hash aggregations.
//!
//! This implementation is loosely based on the implementation from Li,
//! Andersen, Kaminsky, and Freedman in "Algorithmic Improvements for Fast
//! Concurrent Cuckoo Hashing". Specific differences include:
//! 1) No HTM support
//! 2) Evictions do not require taking a global lock (all buckets on the
//!    potential eviction path must be locked, however)
use std::{
    arch::x86_64::{_MM_HINT_T0, _mm_prefetch},
    borrow::Borrow,
    collections::{HashMap, HashSet, VecDeque},
    hash::{BuildHasher, Hash, Hasher},
};
use std::cell::RefMut;
use fnv::FnvBuildHasher;
use itertools::Itertools;

use common::{FuzzyCounter, LockVec};
use crate::ticketer::Ticketer;

#[derive(Clone, Default)]
pub struct CuckooBucket<K> {
    used_bitmap: u8,
    keys: [K; 8],
    values: [usize; 8],
}

/// A bucket containing a fixed number of slots for items. Each slot holds
/// one item.
impl<K: PartialEq + Default> CuckooBucket<K> {
    fn is_slot_empty(&self, idx: usize) -> bool {
        (self.used_bitmap & (1 << idx)) == 0
    }

    fn set_slot_free(&mut self, idx: usize) {
        self.used_bitmap &= !(1 << idx);
    }

    fn set_slot_used(&mut self, idx: usize) {
        self.used_bitmap |= 1 << idx;
    }

    pub fn insert(&mut self, key: K, value: usize) {
        let free_idx = self.used_bitmap.trailing_ones() as usize;
        debug_assert!(self.is_slot_empty(free_idx));
        self.keys[free_idx] = key;
        self.values[free_idx] = value;
        self.set_slot_used(free_idx);
        return;
    }

    pub fn insert_at(&mut self, idx: usize, key: K, value: usize) {
        debug_assert!(self.is_slot_empty(idx));
        self.keys[idx] = key;
        self.values[idx] = value;
        self.set_slot_used(idx);
    }

    /// Search for, and return a reference to, the given item. A simple
    /// linear scan.
    pub fn search<Q>(&self, key: &Q) -> Option<usize>
    where
        K: Borrow<Q>,
        Q: PartialEq + ?Sized,
    {
        for slot_idx in 0..8 {
            if self.is_slot_empty(slot_idx) {
                continue;
            }

            if self.keys[slot_idx].borrow() == key {
                return Some(self.values[slot_idx]);
            }
        }

        return None;
    }

    pub fn at(&self, idx: usize) -> Option<(&K, usize)> {
        if self.is_slot_empty(idx) {
            return None;
        }

        Some((&self.keys[idx], self.values[idx]))
    }

    /// Determines it this bucket has any empty slots.
    pub fn has_empty(&self) -> bool {
        return self.count_empty() > 0;
    }

    /// Counts the number of empty slots available in this bucket (used
    /// when resizing the table). Scans all buckets.
    pub fn count_empty(&self) -> usize {
        return self.used_bitmap.count_zeros() as usize;
    }

    /// Counts the number of full slots in this bucket. Scans all buckets.
    pub fn count_full(&self) -> usize {
        return self.used_bitmap.count_ones() as usize;
    }

    pub fn take(&mut self, idx: usize) -> (K, usize) {
        debug_assert!(!self.is_slot_empty(idx));

        let mut tmp_k: K = Default::default();
        let mut tmp_v = Default::default();
        std::mem::swap(&mut tmp_k, &mut self.keys[idx]);
        std::mem::swap(&mut tmp_v, &mut self.values[idx]);
        self.set_slot_free(idx);

        return (tmp_k, tmp_v);
    }

    #[allow(dead_code)]
    pub fn prune<F: Fn(&K) -> bool>(&mut self, f: F) -> usize {
        let mut total = 0;
        for slot_idx in 0..8 {
            if self.is_slot_empty(slot_idx) {
                continue;
            }

            if f(&self.keys[slot_idx]) {
                total += 1;
                self.set_slot_free(slot_idx);
            }
        }

        total
    }

    pub fn into_vec(self) -> Vec<(K, usize)> {
        let mut r = Vec::new();
        let indexes = (0..8).filter(|idx| !self.is_slot_empty(*idx)).collect_vec();
        for (idx, (k, v)) in self
            .keys
            .into_iter()
            .zip(self.values.into_iter())
            .enumerate()
        {
            if indexes.contains(&idx) {
                r.push((k, v));
            }
        }

        r
    }

    #[allow(dead_code)]
    pub fn iter_keys(&self) -> impl Iterator<Item = &K> {
        self.keys
            .iter()
            .enumerate()
            .filter(|(idx, _k)| !self.is_slot_empty(*idx))
            .map(|(_idx, k)| k)
    }
}

fn hash_keys<H: BuildHasher, K: Hash + ?Sized>(
    h: &H,
    key: &K,
    num_buckets: usize,
) -> (usize, usize) {
    let mut hasher = h.build_hasher();
    key.hash(&mut hasher);
    let h1h = hasher.finish();

    let mut hasher = h.build_hasher();
    h1h.hash(&mut hasher);
    let h2h = hasher.finish();

    let b1 = h1h as usize % num_buckets;
    let b2 = h2h as usize % num_buckets;

    // make sure b1 != b2 -- First, because doing so can only help give us
    // a better spread. Second, `insert` assumes that a write lock to b1
    // can be acquired after dropping a read lock to b1, so if we don't do
    // this check, we can deadlock in `insert`.
    let b2 = if b1 == b2 { (b2 + 1) % num_buckets } else { b2 };

    // return the hashes with the smaller value first, so we always
    // take locks in order
    if b1 < b2 {
        return (b1, b2);
    } else {
        return (b2, b1);
    }
}

pub type InsertResult = Result<usize, ()>;

pub struct CuckooTicketer<K: Default, S = FnvBuildHasher> {
    buckets: LockVec<CuckooBucket<K>>,
    ticketer: FuzzyCounter,
    h: S,
}

impl<K: Default + Clone, S> CuckooTicketer<K, S>
where
    K: Hash + PartialEq,
    S: Default + BuildHasher,
{
    const LF: f64 = 0.5; // Set a bit lower to decrease chance of eviction.

    /// Gets the number of items stored in the map. This is a `O(n)`
    /// operation, and requires grabbing a read lock on each bucket.
    pub fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.unwrap().count_full()).sum()
    }

    /// Determines if the map is empty. This is a `O(n)` operation, and
    /// takes a read lock on each bucket.
    pub fn is_empty(&self) -> bool {
        self.buckets.iter().all(|b| b.unwrap().count_empty() == 8)
    }

    /// Constructs a new CuckooHashMap with the given number of buckets.
    /// Capacity is approximately 80% of (`num_buckets` * 8) =
    /// `num_buckets` * 6.4.
    pub fn with_buckets(num_buckets: usize) -> CuckooTicketer<K, S> {
        let buckets = LockVec::new(num_buckets);
        return CuckooTicketer {
            buckets,
            ticketer: FuzzyCounter::new(0),
            h: Default::default(),
        };
    }

    /// The number of buckets in the hash map.
    pub fn num_buckets(&self) -> usize {
        return self.buckets.len();
    }

    fn hash_keys(&self, key: &K, num_buckets: usize) -> (usize, usize) {
        hash_keys(&self.h, key, num_buckets)
    }

    /// Insert a new value into the hash table. If the key was not previously present,
    ///  a key is inserted with the given value. If the key existed, then nothing occurs.
    pub fn insert(&self, key: K, counter: &mut RefMut<(usize, usize)>) -> InsertResult {
        let (k, b1_idx, b2_idx) = {
            let (b1_idx, b2_idx) = self.hash_keys(&key, self.buckets.len());

            // fast path for lookup.
            {
                let b1 = self.buckets.read(b1_idx).unwrap();
                match b1.search(&key) {
                    Some(value) => {
                        return Ok(value);
                    }
                    None => {
                        let b2 = self.buckets.read(b2_idx).unwrap();
                        match b2.search(&key) {
                            Some(value) => {
                                return Ok(value);
                            }
                            None => {}
                        }
                    }
                }
            }

            // acquire write lock for one bucket at a time which improves
            // performance for update-heavy workloads
            let mut b1 = self.buckets.write(b1_idx).unwrap();
            match b1.search(&key) {
                Some(value) => {
                    return Ok(value);
                }
                None => {
                    let mut b2 = self.buckets.write(b2_idx).unwrap();
                    match b2.search(&key) {
                        Some(value) => {
                            return Ok(value);
                        }
                        None => {
                            if b1.has_empty() {
                                let ticket = self.ticketer.fetch_increment(counter);
                                b1.insert(key, ticket);
                                return Ok(ticket);
                            } else if b2.has_empty() {
                                let ticket = self.ticketer.fetch_increment(counter);
                                b2.insert(key, ticket);
                                return Ok(ticket);
                            } else {
                                // do an evict
                                (key, b1_idx, b2_idx)
                            }
                        }
                    }
                },
            }
        };

        return match self.build_evict_plan(b1_idx, b2_idx) {
            Some(plan) => {
                self.execute_eviction_plan(plan);
                self.insert(k, counter)
            }
            None => Err(())
        };
    }

    /// Resize the table to contain `new_size` buckets with `SLOTS` slots.
    /// Uses power-of-2 inserting to ensure a good fill factor.
    ///
    /// It is possible the increasing or decreasing the table size can
    /// cause some items not to fit. If a resize is fully successful (all
    /// old items inserted into the resized table), `None` is returned. If
    /// some items could not fit, the non-fitting items are returned.
    pub fn resize_table(&mut self, new_size: usize) -> Option<Vec<(K, usize)>> {
        let mut new_buckets = LockVec::new(new_size);
        std::mem::swap(&mut self.buckets, &mut new_buckets);
        let old_buckets = new_buckets;

        let mut did_not_fit = Vec::new();

        // we will insert our old data into the new buckets using the
        // power-of-2 heuristic
        for old_bucket in old_buckets.into_inner() {
            for (k, v) in old_bucket.into_vec() {
                let (b1_idx, b2_idx) = self.hash_keys(&k, new_size);
                let mut b1 = self.buckets.write(b1_idx).unwrap();
                let mut b2 = self.buckets.write(b2_idx).unwrap();

                let b1_empty = b1.count_empty();
                let b2_empty = b2.count_empty();
                if b1_empty == 0 && b2_empty == 0 {
                    did_not_fit.push((k, v));
                    continue;
                }

                if b1.count_empty() >= b2.count_empty() {
                    let _ = b1.insert(k, v);
                } else {
                    let _ = b2.insert(k, v);
                }
            }
        }

        if did_not_fit.is_empty() {
            return None;
        } else {
            return Some(did_not_fit);
        }
    }

    /// verifies the execution plan, then executes it if the plan was found
    /// to be valid. Returns true if eviction were performed, otherwise
    /// returns false.
    fn execute_eviction_plan(&self, mut plan: Vec<(usize, usize)>) -> bool {
        // first, collect all the locks we need to verify the plan
        let all_required_buckets = plan
            .iter()
            .map(|(b_id, _s_id)| *b_id)
            .sorted()
            .unique()
            .collect_vec();
        let buckets = &self.buckets;
        let mut bucket_locks = HashMap::new();
        for b_id in all_required_buckets {
            bucket_locks.insert(b_id, buckets.write(b_id).unwrap());
        }

        // next, verify that the path is valid
        for ((src_b_id, src_s_id), (dst_b_id, _dst_s_id)) in plan.iter().tuple_windows() {
            let src_b = &bucket_locks[src_b_id];

            // prefetch the destination bucket
            unsafe {
                let ptr: *const CuckooBucket<K> = &*bucket_locks[dst_b_id];
                _mm_prefetch::<_MM_HINT_T0>(ptr as *const i8);
            }

            match src_b.at(*src_s_id) {
                None => return false,
                Some((k, _v)) => {
                    let (pot_b1_idx, pot_b2_idx) = self.hash_keys(k, buckets.len());
                    if pot_b1_idx != *dst_b_id && pot_b2_idx != *dst_b_id {
                        return false;
                    }
                }
            }
        }

        let final_dest = plan.pop().unwrap().0;
        if !bucket_locks[&final_dest].has_empty() {
            return false;
        }

        // the path is valid, time to execute it
        let (first_b, first_s) = plan.remove(0);
        let (mut evicted_key, mut evicted_value) =
            bucket_locks.get_mut(&first_b).unwrap().take(first_s);
        for (b_id, s_id) in plan {
            let b = bucket_locks.get_mut(&b_id).unwrap();
            let (old_key, old_value) = b.take(s_id);
            b.insert_at(s_id, evicted_key, evicted_value);
            evicted_key = old_key;
            evicted_value = old_value;
        }
        bucket_locks
            .get_mut(&final_dest)
            .unwrap()
            .insert(evicted_key, evicted_value);
        return true;
    }

    /// Search for an eviction plan that removes an element from either
    /// `b1_idx` or `b2_idx`. Performs a breadth-first-search and then
    /// returns a path containing (bucket id, slot id) pairs. The last
    /// element in the path always has a slot index of SLOTS + 1, but any slot
    /// in the last bucket is fine.
    ///
    /// Returns `None` is there is no path available (table is full).
    fn build_evict_plan(&self, b1_idx: usize, b2_idx: usize) -> Option<Vec<(usize, usize)>> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        for idx in [b1_idx, b2_idx] {
            for slot in 0..8 {
                queue.push_back((idx, slot, 0));
                visited.insert((idx, slot));
            }
        }

        let mut backptrs = HashMap::new();
        let mut final_pt = None;
        let buckets = &self.buckets;

        while let Some((b_idx, s_idx, depth)) = queue.pop_front() {
            // limit the depth of our search
            if depth >= 7 {
                continue;
            }

            let b = buckets.read(b_idx).unwrap();
            if b.at(s_idx).is_none() {
                // there was an item here before, when the bucket was full,
                // but now the path has been modified and this slot is
                // empty, so this path is no longer valid
                continue;
            }

            // the slot is still full, as we expected
            let (evict_b1, evict_b2) = self.hash_keys(b.at(s_idx).unwrap().0, buckets.len());
            let evict_bucket = if evict_b1 != b_idx {
                evict_b1
            } else {
                evict_b2
            };
            drop(b);

            if buckets.read(evict_bucket).unwrap().has_empty() {
                // we've found a path!
                final_pt = Some((evict_bucket, 8 + 1));
                backptrs.insert((evict_bucket, 8 + 1), (b_idx, s_idx));
                break;
            } else {
                // other bucket is full
                for slot in 0..8 {
                    if visited.contains(&(evict_bucket, slot)) {
                        continue;
                    }
                    visited.insert((evict_bucket, slot));
                    queue.push_back((evict_bucket, slot, depth + 1));
                    backptrs.insert((evict_bucket, slot), (b_idx, s_idx));
                }
            }
        }

        // return None if we did not find a path
        final_pt?;

        // we found a path
        let final_pt = final_pt.unwrap();
        let mut path = vec![final_pt];
        while let Some(prev) = backptrs.get(path.last().unwrap()) {
            path.push(*prev);
        }
        path.reverse();

        return Some(path);
    }
}

impl<
    K: Send + Sync + Default + Eq + Hash + Clone,
    S: BuildHasher + Send + Sync + Default,
> Ticketer<K> for CuckooTicketer<K, S> {
    fn with_capacity_and_threads(capacity: usize, _threads: usize) -> Self {
        return CuckooTicketer::with_buckets((capacity.div_ceil(8) as f64 / Self::LF).ceil() as usize);
    }

    fn ticket(&self, keys: &[K], output: &mut [usize]) {
        let mut counter = self.ticketer.get_thread_counter();

        for (idx, key) in keys.iter().enumerate() {
            match self.insert(key.clone(), &mut counter) {
                Ok(value) => {
                    output[idx] = value;
                },
                Err(()) => {}
            }
        }
    }

    fn into_kvs(self) -> Vec<(K, usize)> {
        self.buckets
            .into_inner()
            .into_iter()
            .flat_map(|bucket| {
                let used_bitmap = bucket.used_bitmap.clone();
                bucket
                    .into_vec()
                    .into_iter()
                    .enumerate()
                    .filter_map(move |(idx, (key, ticket))| {
                        if (used_bitmap & (1 << idx)) == 0 {
                            return None;
                        }
                        return Some((key, ticket));
                    })
            }).collect_vec()
    }
}
