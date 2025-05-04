use std::marker::PhantomData;

use crate::updater::Updater;
use common::LockVec;

pub struct CountUpdater<V> {
    aggs: LockVec<usize>,
    pd: PhantomData<V>,
}

impl<V: Send + Sync + Default> Updater<V> for CountUpdater<V> {
    type Agg = usize;

    fn with_capacity_and_threads(capacity: usize, _threads: usize) -> Self {
        Self {
            aggs: LockVec::new(capacity),
            pd: Default::default(),
        }
    }

    fn update_vec(&self, tickets: &[usize], _values: &[V]) {
        for ticket in tickets {
            *self.aggs.write(*ticket).unwrap() += 1;
        }
    }

    fn into_vec(self) -> Vec<Self::Agg> {
        self.aggs.into_inner()
    }
}

pub struct MaxUpdater<V> {
    aggs: LockVec<V>,
}

impl<V: Send + Sync + Default + Clone + Ord> Updater<V> for MaxUpdater<V> {
    type Agg = V;

    fn with_capacity_and_threads(capacity: usize, _threads: usize) -> Self {
        Self {
            aggs: LockVec::new(capacity),
        }
    }

    fn update_vec(&self, tickets: &[usize], values: &[V]) {
        for (ticket, val) in tickets.iter().zip(values.iter()) {
            let mut write = self.aggs.write(*ticket).unwrap();
            if val > &*write {
                *write = val.clone();
            }
        }
    }

    fn into_vec(self) -> Vec<Self::Agg> {
        self.aggs.into_inner()
    }
}
