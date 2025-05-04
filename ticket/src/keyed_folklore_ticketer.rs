// Close to a carbon copy of folklore ticketer but with multithreaded resize and key vector in ticket order.

use std::cell::RefMut;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Once, RwLock};

use atomic_traits::Atomic;
use fnv::FnvBuildHasher;
use itertools::Itertools;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use common::{FuzzyCounterFinalizer, FuzzyCounter, SyncUnsafeCell};
use crate::ticketer::{KeyedTicketer, Ticketer};

pub struct KeyedFolkloreTicketer<K: Atomic, S = FnvBuildHasher> where {
    table: RwLock<Vec<(K, AtomicUsize)>>,
    // To get keys back in ticketed order. Only thread that can write is the one issuing the ticket, so no need for concurrency control.
    keys: RwLock<Vec<SyncUnsafeCell<K::Type>>>,
    encountered_default: Once,
    ticketer: FuzzyCounter,
    threads: usize,
    bh: S,
}

impl<K: Atomic + Send + Sync, S> KeyedFolkloreTicketer<K, S>
where
    K::Type: Hash + Eq + Clone + Default,
    S: BuildHasher + Send + Sync + Default,
{
    const LF: f64 = 0.5;

    fn check_slot(
        &self,
        table: &[(K, AtomicUsize)],
        keys: &[SyncUnsafeCell<K::Type>],
        table_pos: usize,
        key: &K::Type,
        counter: &mut RefMut<(usize, usize)>,
    ) -> Option<usize> {
        loop {
            let (curr_key, curr_ticket) = &table[table_pos];

            // read optimized path: there is already a value here!
            {
                let curr_ticket_value = curr_ticket.load(Ordering::Relaxed);
                if curr_ticket_value >= 1 {
                    loop {
                        let curr_key_load = curr_key.load(Ordering::Relaxed);
                        if &curr_key_load == key {
                            return Some(curr_ticket_value);
                        } else if curr_key_load == Default::default() {
                            continue;
                        } else {
                            return None;
                        }
                    }
                }
            }

            // Slow path.
            match curr_ticket.compare_exchange(0, self.ticketer.fetch(counter), Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_v) => unsafe {
                    let ticket = self.ticketer.fetch_increment(counter);
                    curr_key.store(key.clone(), Ordering::Relaxed);
                    *keys[ticket].get() = key.clone();
                    return Some(ticket);
                }
                Err(v) => {
                    // this bucket already has a ticket value in it
                    loop {
                        let curr_key_load = curr_key.load(Ordering::Relaxed);
                        if &curr_key_load == key {
                            return Some(v);
                        } else if curr_key_load == Default::default() {
                            continue;
                        } else {
                            return None;
                        }
                    }
                }
            }
        }
    }

    fn resize(&self, curr_size: usize) {
        // we need to grow the table -- first, drop our read lock and get a
        // write lock
        let mut wtable = self.table.write().unwrap();

        // possible race: another thread may have done inserts, or even have
        // grown the table, between us dropping our read lock and picking up
        // another write lock. This could cause extra, unnecessary table
        // grows.
        if curr_size != wtable.len() {
            return;
        }

        // resize keys vec.
        let mut keys = self.keys.write().unwrap(); // Guaranteed no deadlock provided this always occurs after lock on table.
        keys.extend((0..curr_size).map(|_| Default::default()));

        // resize table.
        let mut tmp = (0..wtable.len() * 2)
            .map(|_| (K::new(K::Type::default()), AtomicUsize::new(0)))
            .collect_vec();
        std::mem::swap(&mut tmp, &mut *wtable);

        // Multithreaded migration.
        let ranges  =  (0..self.threads)
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

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.threads)
            .build()
            .unwrap();

        pool.install(|| {
            ranges.par_iter()
                .for_each(|&(lo, hi)| {
                    let mut idx = lo;
                    while idx != hi {
                        let k = tmp[idx].0.load(Ordering::Relaxed);
                        let t = tmp[idx].1.load(Ordering::Relaxed);

                        let h = {
                            let mut hasher = self.bh.build_hasher();
                            k.hash(&mut hasher);
                            hasher.finish() as usize % wtable.len()
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
                                _ => {
                                    curr_pos = (curr_pos + 1) % wtable.len();
                                    if curr_pos == h {
                                        panic!("could not insert even after doubling table size")
                                    }
                                }
                            }
                        }

                        if hi != tmp.len() {
                            idx = (idx + 1) % tmp.len();
                        } else {
                            idx += 1
                        }
                    }
                })
        });
    }
}

impl<K: Atomic + Send + Sync, S> KeyedTicketer<K::Type> for KeyedFolkloreTicketer<K, S>
where
    K::Type: Hash + Eq + Clone + Default,
    S: BuildHasher + Default + Send + Sync,
{
    fn into_keys(self) -> (Vec<K::Type>, FuzzyCounterFinalizer) {
        unsafe {
            let finalizer = self.ticketer.into_finalizer();
            let keys_inner = self.keys.into_inner().unwrap();
            let mut keys_clone = std::mem::ManuallyDrop::new(keys_inner);
            let len = finalizer.reorder_slice(&mut keys_clone);

            // We should return this to have caller fix extraneous entry for default ticket
            // in aggregates. We don't to keep the interface slightly less ad hoc but removing
            // index 0 from returned aggregates is easy and efficient with the right vector design
            // so this omission isn't a big deal from a benchmarking perspective. It is a problem
            // for correctness, so all test cases ensure the default case is in the dataset.
            let _encountered_default = self.encountered_default.is_completed();

            (
                Vec::from_raw_parts(
                    keys_clone.as_mut_ptr() as *mut K::Type,
                    len,
                    keys_clone.capacity()
                ),
                finalizer,
            )
        }
    }
}

impl<K: Atomic + Send + Sync, S> Ticketer<K::Type> for KeyedFolkloreTicketer<K, S>
where
    K::Type: Hash + Eq + Clone + Default,
    S: BuildHasher + Send + Sync + Default,
{
    fn ticket(&self, keys: &[K::Type], output: &mut [usize]) {
        let mut counter = self.ticketer.get_thread_counter();

        keys.iter()
            .zip(output.iter_mut())
            .for_each(|(k, o)| {
                let mut hasher = self.bh.build_hasher();
                k.hash(&mut hasher);
                *o = hasher.finish() as usize;
            });

        //radsort::sort_by_key(&mut hashes, |(h, _idx)| *h);
        //one_pass_8bit_radix(&mut hashes);
        //one_pass_bubble(&mut hashes);

        let empty_key = K::Type::default();
        let mut encountered_default = false;

        let mut table = self.table.read().unwrap();
        let mut keys_internal = self.keys.read().unwrap();
        for out_pos in 0..keys.len() {
            let hash = output[out_pos];
            let key = keys[out_pos].clone();

            if key == empty_key {
                encountered_default = true;
                output[out_pos] = 0;
                continue;
            }

            // this is a new value for the batch, we have to check the table
            let mut hash_mod = hash % table.len();
            let mut table_pos = hash % table.len();
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
                    std::mem::drop(table);
                    std::mem::drop(keys_internal);
                    self.resize(table_len);
                    table = self.table.read().unwrap();
                    keys_internal = self.keys.read().unwrap();

                    // retry insert.
                    hash_mod = hash % table.len();
                    table_pos = hash % table.len();
                    continue;
                }
            }
        }

        if encountered_default {
            self.encountered_default.call_once(|| {});
        }

        // Rough threshold of 10_000 to avoid unneeded resizing for lower cardinality due to
        // the error from fuzzy counter (instead will grow when full rather than based on
        // the ticketer ub).
        if table.len() > 10_000 && self.ticketer.len() > ((table.len() as f64 * Self::LF) as usize + self.threads * self.ticketer.step_size() + 1) {
            let curr_size = table.len();
            std::mem::drop(table);
            std::mem::drop(keys_internal);
            self.resize(curr_size);
        }
    }

    fn into_kvs(self) -> Vec<(K::Type, usize)> {
        let mut default: Option<(K::Type, usize)> = None;
        if self.encountered_default.is_completed() {
            default = Some((Default::default(), 0));
        }
        self.table
            .into_inner()
            .unwrap()
            .into_iter()
            .filter_map(|(k, v)| {
                let v = v.into_inner();
                if v >= 1 {
                    Some((k.into_inner(), v))
                } else {
                    None
                }
            }).chain(default.into_iter())
            .collect_vec()
    }

    fn with_capacity_and_threads(capacity: usize, threads: usize) -> Self {
        let array_size = (capacity as f64 / Self::LF).ceil() as usize;
        let table = (0..array_size)
            .map(|_| (K::new(K::Type::default()), AtomicUsize::new(0)))
            .collect_vec();

        KeyedFolkloreTicketer {
            table: RwLock::new(table),
            keys: RwLock::new((0..(array_size + threads * FuzzyCounter::DEFAULT_STEP_SIZE + 1)).map(|_| Default::default()).collect_vec()), // Hacky fix for the fact that tickets can go over capacity due to fuzzy ticketer. +1 to account for default key reserved as 0.
            encountered_default: Once::new(),
            ticketer: FuzzyCounter::new(1),
            threads,
            bh: Default::default(),
        }
    }
}
