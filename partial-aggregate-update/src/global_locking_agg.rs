use std::marker::PhantomData;
use std::sync::RwLock;

use crate::Updater;

pub struct CountUpdater<V> {
    aggs: RwLock<Vec<usize>>,
    pd: PhantomData<V>,
}

impl<V: Send + Sync + Default> Updater<V> for CountUpdater<V> {
    type Agg = usize;

    fn with_capacity_and_threads(capacity: usize, _threads: usize) -> Self {
        Self {
            aggs: RwLock::new(vec![0usize; capacity]),
            pd: Default::default(),
        }
    }

    fn update_vec(&self, tickets: &[usize], _values: &[V]) {
        let mut aggs = self.aggs.write().unwrap();
        for ticket in tickets {
            aggs[*ticket] += 1;
        }
    }

    fn into_vec(self) -> Vec<Self::Agg> {
        self.aggs.into_inner().unwrap()
    }
}
