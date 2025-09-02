use std::hash::{BuildHasher, Hash};

use dashmap::DashMap;
use fnv::FnvBuildHasher;
use itertools::Itertools;

use common::FuzzyCounter;
use crate::ticketer::Ticketer;

#[derive(Default)]
pub struct DashTicketer<K: Eq + Hash, S: BuildHasher + Clone = FnvBuildHasher> {
    map: DashMap<K, usize, S>,
    ticketer: FuzzyCounter,
}

impl<K: Eq + Hash + Copy + Sync + Send, S: BuildHasher + Clone + Default + Send + Sync> Ticketer<K> for DashTicketer<K, S> {
    fn with_capacity_and_threads(capacity: usize, _threads: usize) -> Self {
        DashTicketer {
            map: DashMap::with_capacity_and_hasher(capacity, S::default()),
            ticketer: FuzzyCounter::new(0),
        }
    }

    fn ticket(&self, keys: &[K], output: &mut [usize]) {
        let mut counter = self.ticketer.get_thread_counter();

        for (key, out) in keys.iter().zip(output.iter_mut()) {
            if let Some(ticket) = self.map.get(key) {
                *out = *ticket.value();
            } else {
                *out = *self.map
                    .entry(*key)
                    .or_insert_with(|| self.ticketer.fetch_increment(&mut counter));
            }
        }
    }

    fn into_kvs(self) -> Vec<(K, usize)> {
        self.map.into_iter().collect_vec()
    }
}
