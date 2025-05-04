use std::{
    cell::RefMut,
    hash::{Hash, Hasher},
    sync::{RwLock, OnceLock},
};
use std::hash::BuildHasher;
use fnv::FnvBuildHasher;
use itertools::Itertools;
use common::FuzzyCounter;
use crate::ticketer::Ticketer;

pub struct OnceLockHashMap<K, S = FnvBuildHasher> {
    table: RwLock<Vec<OnceLock<(K, usize)>>>,
    ticketer: FuzzyCounter,
    threads: usize,
    bh: S,
}

impl<K, S> OnceLockHashMap<K, S>
where
    K: Eq + Hash + Default + Clone,
    S: BuildHasher + Default,
{
    const LF: f64 = 0.5;

    fn check_slot(
        &self,
        table: &[OnceLock<(K, usize)>],
        table_pos: usize,
        key: &K,
        counter: &mut RefMut<(usize, usize)>,
    ) -> Option<usize> {
        loop {
            let (curr_key, curr_ticket) = &table[table_pos].get_or_init(
                || (key.clone(), self.ticketer.fetch_increment(counter))
            );

            if curr_key == key {
                return Some(*curr_ticket);
            } else {
                return None;
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
            .map(|_| Default::default())
            .collect_vec();
        std::mem::swap(&mut tmp, &mut *wtable);

        for el in tmp {
            let kv = el.into_inner();
            match kv {
                Some((k, v)) => {
                    let h = {
                        let mut hasher = self.bh.build_hasher();
                        k.hash(&mut hasher);
                        hasher.finish() as usize % wtable.len()
                    };

                    let mut curr_pos = h;
                    loop {
                        match wtable[curr_pos].get() {
                            Some(_) => {
                                curr_pos = (curr_pos + 1) % wtable.len();
                                if curr_pos == h {
                                    panic!("could not insert even after doubling table size")
                                }
                            },
                            None => {
                                let _ = wtable[curr_pos].set((k, v));
                                break;
                            }
                        };
                    }
                },
                None => ()
            }
        }
    }
}

impl<K, S> Ticketer<K> for OnceLockHashMap<K, S>
where
    K: Hash + Eq + Clone + Send + Sync + Default,
    S: BuildHasher + Send + Sync + Default,
{
    fn ticket(&self, keys: &[K], output: &mut [usize]) {
        let mut counter = self.ticketer.get_thread_counter();

        keys.iter()
            .zip(output.iter_mut())
            .for_each(|(k, o)| {
                let mut hasher = self.bh.build_hasher();
                k.hash(&mut hasher);
                *o = hasher.finish() as usize;
            });

        let mut table = self.table.read().unwrap();
        for out_pos in 0..keys.len() {
            let hash = output[out_pos];
            let key = &keys[out_pos];

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

        // Rough threshold of 10_000 to avoid unneeded resizing for lower cardinality due to
        // the error from fuzzy counter (instead will grow when full rather than based on
        // the ticketer ub.
        if table.len() > 10_000 && self.ticketer.len() > ((table.len() as f64 * Self::LF) as usize + self.threads * self.ticketer.step_size()) {
            let curr_size = table.len();
            std::mem::drop(table);
            self.resize(curr_size);
        }
    }

    fn into_kvs(self) -> Vec<(K, usize)> {
        self.table
            .into_inner()
            .unwrap()
            .into_iter()
            .filter_map(|kv| kv.into_inner())
            .collect_vec()
    }

    fn with_capacity_and_threads(capacity: usize, threads: usize) -> Self {
        let table = (0..((capacity as f64 / Self::LF).ceil() as usize))
            .map(|_| OnceLock::<(K, usize)>::new())
            .collect_vec();

        OnceLockHashMap {
            table: RwLock::new(table),
            ticketer: Default::default(),
            threads,
            bh: Default::default(),
        }
    }
}
