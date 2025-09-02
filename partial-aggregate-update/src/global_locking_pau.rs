use std::marker::PhantomData;
use std::ops::AddAssign;
use std::sync::RwLock;

use bytemuck::{zeroed_vec, Zeroable};
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

    fn with_capacity_and_threads_zeroed(capacity: usize, _threads: usize) -> Self {
        Self {
            aggs: RwLock::new(zeroed_vec(capacity)),
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

pub struct MaxUpdater<V> {
    aggs: RwLock<Vec<V>>,
    pd: PhantomData<V>,
}

impl<V: Send + Sync + PartialOrd + Clone + Default + Zeroable> Updater<V> for MaxUpdater<V> {
    type Agg = V;

    fn with_capacity_and_threads(capacity: usize, _threads: usize) -> Self {
        Self {
            aggs: RwLock::new(vec![V::default(); capacity]),
            pd: Default::default(),
        }
    }

    fn with_capacity_and_threads_zeroed(capacity: usize, _threads: usize) -> Self {
        Self {
            aggs: RwLock::new(zeroed_vec(capacity)),
            pd: Default::default(),
        }
    }

    fn update_vec(&self, tickets: &[usize], values: &[V]) {
        let mut aggs = self.aggs.write().unwrap();
        for (ticket, val) in tickets.iter().zip(values.iter()) {
            if val > &aggs[*ticket] {
                aggs[*ticket] = val.clone();
            }
        }
    }

    fn into_vec(self) -> Vec<Self::Agg> {
        self.aggs.into_inner().unwrap()
    }
}

pub struct SumUpdater<V> {
    aggs: RwLock<Vec<V>>,
    pd: PhantomData<V>,
}

impl<V: Send + Sync + AddAssign + Clone + Default + Zeroable> Updater<V> for SumUpdater<V> {
    type Agg = V;

    fn with_capacity_and_threads(capacity: usize, _threads: usize) -> Self {
        Self {
            aggs: RwLock::new(vec![V::default(); capacity]),
            pd: Default::default(),
        }
    }

    fn with_capacity_and_threads_zeroed(capacity: usize, _threads: usize) -> Self {
        Self {
            aggs: RwLock::new(zeroed_vec(capacity)),
            pd: Default::default(),
        }
    }

    fn update_vec(&self, tickets: &[usize], values: &[V]) {
        let mut aggs = self.aggs.write().unwrap();
        for (ticket, val) in tickets.iter().zip(values.iter()) {
            aggs[*ticket] += val.clone();
        }
    }

    fn into_vec(self) -> Vec<Self::Agg> {
        self.aggs.into_inner().unwrap()
    }
}