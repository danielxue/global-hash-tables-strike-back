use std::cell::RefMut;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Once, RwLock};

use atomic_traits::Atomic;
use fnv::FnvBuildHasher;
use itertools::Itertools;
use common::{FuzzyCounter};
use crate::ticketer::Ticketer;


pub struct FolkloreTicketer<K: Atomic, S = FnvBuildHasher> {
    table: RwLock<Vec<(K, AtomicUsize)>>,
    encountered_default: Once,
    ticketer: FuzzyCounter,
    threads: usize,
    bh: S,
}

impl<K: Atomic, S> FolkloreTicketer<K, S>
where
    K::Type: Eq + Hash + Default,
    S: BuildHasher + Default,
{
    const LF: f64 = 0.5;

    fn check_slot(
        &self,
        table: &[(K, AtomicUsize)],
        table_pos: usize,
        key: K::Type,
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
                        if curr_key_load == key {
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
                Ok(_v) => {
                    curr_key.store(key, Ordering::Relaxed);
                    return Some(self.ticketer.fetch_increment(counter));
                }
                Err(v) => {
                    // this bucket already has a ticket value in it
                    loop {
                        let curr_key_load = curr_key.load(Ordering::Relaxed);
                        if curr_key_load == Default::default() {
                            continue;
                        } else if curr_key_load == key {
                            return Some(v);
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

        let mut tmp = (0..wtable.len() * 2)
            .map(|_| (K::new(K::Type::default()), AtomicUsize::new(0)))
            .collect_vec();
        std::mem::swap(&mut tmp, &mut *wtable);

        for el in tmp {
            let (mut key, ticket) = el;
            let h = {
                let mut hasher = self.bh.build_hasher();
                key.get_mut().hash(&mut hasher);
                hasher.finish() as usize % wtable.len()
            };

            let mut curr_pos = h;
            loop {
                match *wtable[curr_pos].1.get_mut() {
                    0 => {
                        wtable[curr_pos] = (key, ticket);
                        break;
                    }
                    _ => {
                        curr_pos = (curr_pos + 1) % wtable.len();
                        if curr_pos == h {
                            panic!("could not insert even after doubling table size")
                        }
                    }
                }
            }
        }
    }
}

impl<K: Atomic + Send + Sync, S> Ticketer<K::Type> for FolkloreTicketer<K, S>
where
    K::Type: Hash + Eq + Copy + Default + Send + Sync,
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

        let empty_key = K::Type::default();
        let mut encountered_default = false;

        let mut table = self.table.read().unwrap();
        for out_pos in 0..keys.len() {

            let hash = output[out_pos];
            let key = keys[out_pos];

            if key == empty_key {
                encountered_default = true;
                output[out_pos] = 0;
                continue;
            }

            // this is a new value for the batch, we have to check the table
            let mut hash_mod = hash % table.len();
            let mut table_pos = hash_mod;
            loop {
                if let Some(ticket) = self.check_slot(&table, table_pos, key, &mut counter) {
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
                    self.resize(table_len);
                    table = self.table.read().unwrap();

                    // retry insert.
                    hash_mod = hash % table.len();
                    table_pos = hash_mod;
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
        if table.len() > 10_000 && self.ticketer.len() > ((table.len() as f64 * Self::LF) as usize + self.threads * self.ticketer.step_size()) {
            let curr_size = table.len();
            std::mem::drop(table);
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
        let table = (0..(((capacity + 1) as f64 / Self::LF).ceil() as usize)) // + 1 for reserved default key at ticket 0.
            .map(|_| (K::new(K::Type::default()), AtomicUsize::new(0)))
            .collect_vec();

        FolkloreTicketer {
            table: RwLock::new(table),
            encountered_default: Once::new(),
            ticketer: FuzzyCounter::new(1),
            threads,
            bh: Default::default(),
        }
    }
}
