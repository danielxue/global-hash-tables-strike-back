use std::{
    collections::HashMap,
    hash::Hash,
    sync::Mutex,
};
use fnv::FnvBuildHasher;
use itertools::Itertools;

use crate::ticketer::Ticketer;

#[derive(Default)]
pub struct GlobalLockingTicketer<K: Default> {
    map: Mutex<HashMap<K, usize, FnvBuildHasher>>,
}

impl<K: Eq + Hash + Default + Copy + Sync + Send> Ticketer<K> for GlobalLockingTicketer<K> {
    fn with_capacity_and_threads(capacity: usize, _threads: usize) -> Self {
        GlobalLockingTicketer {
            map: Mutex::new(HashMap::with_capacity_and_hasher(
                capacity,
                FnvBuildHasher::default(),
            )),
        }
    }

    fn ticket(&self, keys: &[K], output: &mut [usize]) {
        assert!(output.len() >= keys.len());

        let mut map = self.map.lock().unwrap();

        for (key, out) in keys.iter().zip(output.iter_mut()) {
            let next_id = map.len();
            *out = *map.entry(*key).or_insert(next_id);
        }
    }

    fn into_kvs(self) -> Vec<(K, usize)> {
        self.map.into_inner().unwrap().into_iter().collect_vec()
    }
}
