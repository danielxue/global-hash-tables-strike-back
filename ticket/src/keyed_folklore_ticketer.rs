// Close to a carbon copy of folklore ticketer but with multithreaded resize and key vector in ticket order.

use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;
use std::thread;
use atomic_traits::Atomic;
use bytemuck::{zeroed_vec, Zeroable};
use fnv::FnvBuildHasher;
use itertools::Itertools;
use common::{FuzzyCounterFinalizer, FuzzyCounter, UnsafeVec, LocalCounter};
use crate::ticketer::{KeyedTicketer, Ticketer};

pub struct KeyedFolkloreTicketer<K: Atomic, S = FnvBuildHasher> where {
    table: RwLock<Vec<(K, AtomicUsize)>>,
    // Store keys in ticket order to avoid slow materialization at the end.
    // Only thread that can write is the one issuing the ticket, so we avoid
    // concurrency control on each cell, although we do wrap in RwLock for resizing.
    keys: RwLock<UnsafeVec<K::Type>>,
    ticketer: FuzzyCounter,
    threads: usize,
    bh: S,
}

impl<K: Atomic + Zeroable + Send + Sync, S> KeyedFolkloreTicketer<K, S>
where
    K::Type: Hash + Eq + Clone + Default,
    S: BuildHasher + Send + Sync + Default,
{
    const LF: f64 = 0.5;

    fn check_slot(
        &self,
        table: &[(K, AtomicUsize)],
        keys: &UnsafeVec<K::Type>,
        table_pos: usize,
        key: &K::Type,
        counter: &mut LocalCounter,
    ) -> Option<usize> {
        loop {
            let (curr_key, curr_ticket) = &table[table_pos];

            // read optimized path: there is already a value here!
            {
                let curr_ticket_value = curr_ticket.load(Ordering::Acquire);
                if curr_ticket_value >= 2 {
                    return if &curr_key.load(Ordering::Acquire) == key {
                        Some(curr_ticket_value - 2)
                    } else {
                        None
                    }
                }
            }

            // Slow path -- the value is empty or locked. Try to lock it.
            match curr_ticket.compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_v) => {
                    // this bucket was empty, but now we have the lock on it
                    let ticket = self.ticketer.fetch_increment(counter) + 2;
                    curr_key.store(key.clone(), Ordering::Release);
                    curr_ticket.store(ticket, Ordering::Release);
                    keys.write(ticket - 2, key.clone());
                    return Some(ticket - 2);
                }
                Err(v) if v == 1 => {
                    // this bucket is locked by another thread
                    continue;
                }
                Err(v) => {
                    // this bucket already has a ticket value in it
                    return if &curr_key.load(Ordering::Acquire) == key {
                        Some(v - 2)
                    } else {
                        None
                    }
                }
            }
        }
    }

    fn resize(&self, curr_size: usize) {
        // We need to grow the table -- first, drop our read lock and get a
        // write lock.
        let mut wtable = self.table.write().unwrap();

        // Possible race: another thread may have done inserts, or even have
        // grown the table, between us dropping our read lock and picking up
        // another write lock. This could cause extra, unnecessary table
        // grows.
        if curr_size != wtable.len() {
            return;
        }

        // Resize keys vec.
        let mut keys = self.keys.write().unwrap(); // Guaranteed no deadlock provided this always occurs after lock on table.
        keys.extend(curr_size);

        // Resize table.
        let mut tmp: Vec<(K, AtomicUsize)> = zeroed_vec(wtable.len() * 2);
        std::mem::swap(&mut tmp, &mut *wtable);

        // Multithreaded migration. Maier et al. show that when migrating clusters of values in
        // the source table there is no contention on the destination location. Therefore,
        // we compute ranges to lie between clusters to take advantage of this property.
        // However, really this doesn't matter all too much since we still use atomics during the
        // migration anyway and there's not enough contention to have an impact on performance.
        let ranges  = (0..self.threads)
            .map(|t| {
                let mut lo = if t > 0 { (tmp.len() * t).div_ceil(self.threads) - 1 } else { tmp.len() - 1 };
                let mut hi = (tmp.len() * (t + 1)).div_ceil(self.threads) - 1;

                while hi != lo && tmp[hi].1.load(Ordering::Relaxed) != 0 {
                    hi = (hi + 1) % tmp.len();
                }

                if hi == lo && t == 0 {
                    lo = 0;
                    hi = tmp.len();
                } else {
                    while lo != hi && tmp[lo].1.load(Ordering::Relaxed) != 0 {
                        lo = (lo + 1) % tmp.len();
                    }
                }

                (lo, hi)
            }).collect_vec();

        thread::scope(|s| {
            for (lo, hi) in ranges.iter() {
                s.spawn(|| {
                    let mut idx = *lo;
                    while idx != *hi {
                        let k = tmp[idx].0.load(Ordering::Relaxed);
                        let t = tmp[idx].1.load(Ordering::Relaxed);

                        let h = {
                            let mut hasher = self.bh.build_hasher();
                            k.hash(&mut hasher);
                            hasher.finish() as usize
                        };

                        let hash_mod = h % wtable.len();
                        let mut curr_pos = hash_mod;
                        loop {
                            match wtable[curr_pos].1.load(Ordering::Relaxed) {
                                0 => {
                                    wtable[curr_pos].0.store(k, Ordering::Relaxed);
                                    wtable[curr_pos].1.store(t, Ordering::Relaxed);
                                    break;
                                },
                                1 => panic!("found locked cell during resize"),
                                _ => {
                                    curr_pos = (curr_pos + 1) % wtable.len();
                                    if curr_pos == h {
                                        panic!("could not insert even after doubling table size")
                                    }
                                }
                            }
                        }

                        if *hi != tmp.len() {
                            idx = (idx + 1) % tmp.len();
                        } else {
                            idx += 1
                        }
                    }
                });
            }
        });
    }
}

impl<K: Atomic + Zeroable + Send + Sync, S> KeyedTicketer<K::Type> for KeyedFolkloreTicketer<K, S>
where
    K::Type: Hash + Eq + Clone + Default,
    S: BuildHasher + Default + Send + Sync,
{
    fn into_keys(self) -> (Vec<K::Type>, FuzzyCounterFinalizer) {
        let mut keys = self.keys.write().unwrap();

        let finalizer = self.ticketer.into_finalizer();
        let keys_slice = keys.as_mut_slice();
        let len = finalizer.reorder_slice(keys_slice);
        keys.truncate(len);
        drop(keys);

        (
            self.keys.into_inner().unwrap().into_inner(),
            finalizer,
        )
    }
}

impl<K: Atomic + Zeroable + Send + Sync, S> Ticketer<K::Type> for KeyedFolkloreTicketer<K, S>
where
    K::Type: Hash + Eq + Clone + Default,
    S: BuildHasher + Send + Sync + Default,
{
    fn with_capacity_and_threads(capacity: usize, threads: usize) -> Self {
        let array_size = (capacity as f64 / Self::LF).ceil() as usize;
        let table = (0..array_size)
            .map(|_| (K::new(K::Type::default()), AtomicUsize::new(0)))
            .collect_vec();

        KeyedFolkloreTicketer {
            table: RwLock::new(table),
            keys: RwLock::new(UnsafeVec::new(array_size + threads * FuzzyCounter::DEFAULT_STEP_SIZE)), // Overallocate capacity to account for fuzzy ticketer.
            ticketer: Default::default(),
            threads,
            bh: Default::default(),
        }
    }

    fn with_capacity_and_threads_zeroed(capacity: usize, threads: usize) -> Self {
        let array_size = (capacity as f64 / Self::LF).ceil() as usize;
        let table = zeroed_vec(array_size);

        KeyedFolkloreTicketer {
            table: RwLock::new(table),
            keys: RwLock::new(UnsafeVec::new(array_size + threads * FuzzyCounter::DEFAULT_STEP_SIZE)), // Overallocate capacity to account for fuzzy ticketer.
            ticketer: Default::default(),
            threads,
            bh: Default::default(),
        }
    }

    fn ticket(&self, keys: &[K::Type], output: &mut [usize]) {
        let mut counter = self.ticketer.get_thread_counter();

        keys.iter()
            .zip(output.iter_mut())
            .for_each(|(k, o)| {
                let mut hasher = self.bh.build_hasher();
                k.hash(&mut hasher);
                *o = hasher.finish() as usize;
            });

        let mut table = self.table.read().unwrap();
        let mut keys_internal = self.keys.read().unwrap();
        for out_pos in 0..keys.len() {
            let hash = output[out_pos];
            let key = keys[out_pos].clone();

            // this is a new value for the batch, we have to check the table
            let mut hash_mod = hash % table.len();
            let mut table_pos = hash_mod;
            loop {
                if let Some(ticket) = self.check_slot(&table, &keys_internal, table_pos, &key, &mut counter) {
                    output[out_pos] = ticket;
                    break;
                }

                table_pos += 1;
                if table_pos >= table.len() {
                    table_pos = 0;
                }

                if table_pos == hash_mod {
                    let table_len = table.len();
                    drop(table);
                    drop(keys_internal);
                    self.resize(table_len);
                    table = self.table.read().unwrap();
                    keys_internal = self.keys.read().unwrap();

                    // retry insert.
                    hash_mod = hash % table.len();
                    table_pos = hash_mod;
                    continue;
                }
            }
        }

        // Rough threshold of 10_000 to avoid unneeded resizing for lower cardinality due to
        // the error from fuzzy counter (instead will grow when full rather than based on
        // the ticketer ub).
        if table.len() > 10_000 && self.ticketer.len() > ((table.len() as f64 * Self::LF) as usize + self.threads * self.ticketer.step_size()) {
            let curr_size = table.len();
            drop(table);
            drop(keys_internal);
            self.resize(curr_size);
        }
    }

    fn into_kvs(self) -> Vec<(K::Type, usize)> {
        self.table
            .into_inner()
            .unwrap()
            .into_iter()
            .filter_map(|(k, v)| {
                let v = v.into_inner();
                if v >= 2 {
                    Some((k.into_inner(), v - 2))
                } else {
                    None
                }
            })
            .collect_vec()
    }
}
