use std::fmt::Debug;
use std::hash::{BuildHasher, Hash};

use fnv::FnvBuildHasher;
use itertools::Itertools;
use leapfrog::LeapMap;

use common::FuzzyCounter;
use crate::ticketer::Ticketer;

pub struct LeapTicketer<K: Default, S: BuildHasher + Clone = FnvBuildHasher> {
    map: LeapMap<K, usize, S>,
    ticketer: FuzzyCounter,
}

impl<K: Eq + Hash + Default + Copy + Sync + Send + Debug, S: BuildHasher + Clone + Default + Send + Sync> Ticketer<K> for LeapTicketer<K, S> {
    fn with_capacity_and_threads(capacity: usize, _threads: usize) -> Self {
        LeapTicketer {
            map: LeapMap::with_capacity_and_hasher(capacity, S::default()),
            ticketer: FuzzyCounter::new(0),
        }
    }

    fn ticket(&self, keys: &[K], output: &mut [usize]) {
        let mut counter = self.ticketer.get_thread_counter();

        for (key, out) in keys.iter().zip(output.iter_mut()) {
            if let Some(mut kv) = self.map.get(&key) {
                *out = kv.value().unwrap(); // Can unwrap because no deletions -> no None returns.
                continue;

            }

            match self.map.try_insert(*key, self.ticketer.fetch(&mut counter)) {
                None => {
                    *out = self.ticketer.fetch_increment(&mut counter);
                }
                Some(v) => {
                    *out = v;
                }
            }
        }
    }

    fn into_kvs(self) -> Vec<(K, usize)> {
        self.map
            .into_iter()
            .map(|(k, v)| (k, v))
            .collect_vec()
    }
}
