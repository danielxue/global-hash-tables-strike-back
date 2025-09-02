use std::cell::RefCell;
use std::marker::PhantomData;
use std::ops::AddAssign;

use bytemuck::{Zeroable, zeroed_vec};
use itertools::Itertools;
use thread_local::ThreadLocal;

use crate::Updater;

pub struct CountUpdater<V> {
    aggs: ThreadLocal<RefCell<Vec<usize>>>,
    capacity: usize,
    threads: usize,
    zeroed: bool,
    pd: PhantomData<V>,
}

impl<V: Send + Sync + Default> Updater<V> for CountUpdater<V> {
    type Agg = usize;

    fn with_capacity_and_threads(capacity: usize, threads: usize) -> Self {
        Self {
            aggs: ThreadLocal::new(),
            capacity,
            threads,
            zeroed: false,
            pd: Default::default(),
        }
    }

    fn with_capacity_and_threads_zeroed(capacity: usize, threads: usize) -> Self {
        Self {
            aggs: ThreadLocal::new(),
            capacity,
            threads,
            zeroed: true,
            pd: Default::default(),
        }
    }

    fn update_vec(&self, tickets: &[usize], _values: &[V]) {
        let mut aggs = self.aggs.get_or(|| RefCell::new(
            if self.zeroed { zeroed_vec(self.capacity) } else { vec![0; self.capacity] }
        )).borrow_mut();
        for ticket in tickets {
            while *ticket >= aggs.len() {
                let len = aggs.len();
                aggs.extend(
                    if self.zeroed { zeroed_vec::<usize>(len) } else { vec![0usize; len] }
                );
            }
            aggs[*ticket] += 1;
        }
    }

    fn into_vec(self) -> Vec<Self::Agg> {
        // Did not fix for resizing, so makes bad assumption that if one thread resizes, all do.
        // This is a rare edge case even when resizing, and does not occur in experimental workloads.
        let mut iter = self.aggs.into_iter().map(|x| x.into_inner());
        let acc = iter.next().unwrap();
        let acc_ref = &acc;
        let rest = iter.collect_vec();
        let rest_ref = &rest;

        std::thread::scope(|s| {
            for t in 0..self.threads {
                s.spawn(move || {
                    for v in rest_ref.iter() {
                        for i in (acc_ref.len() * t).div_ceil(self.threads)..(acc_ref.len() * (t + 1)).div_ceil(self.threads) {
                            // Non-overlapping range for i makes mutation okay.
                            unsafe {
                                *(acc_ref.as_ptr().add(i) as *mut usize) += v[i];
                            }
                        }
                    }
                });
            }
        });

        acc
    }
}

pub struct MaxUpdater<V: Send> {
    aggs: ThreadLocal<RefCell<Vec<V>>>,
    capacity: usize,
    threads: usize,
    zeroed: bool,
}

impl<V: Send + Sync + PartialOrd + Clone + Default + Zeroable> Updater<V> for MaxUpdater<V> {
    type Agg = V;

    fn with_capacity_and_threads(capacity: usize, threads: usize) -> Self {
        Self {
            aggs: ThreadLocal::new(),
            capacity,
            threads,
            zeroed: false,
        }
    }

    fn with_capacity_and_threads_zeroed(capacity: usize, threads: usize) -> Self {
        Self {
            aggs: ThreadLocal::new(),
            capacity,
            threads,
            zeroed: true,
        }
    }

    fn update_vec(&self, tickets: &[usize], values: &[V]) {
        let mut aggs = self.aggs.get_or(|| RefCell::new(
            if self.zeroed { zeroed_vec(self.capacity) } else { vec![Default::default(); self.capacity] }
        )).borrow_mut();
        for (ticket, val) in tickets.iter().zip(values.iter()) {
            while *ticket >= aggs.len() {
                let len = aggs.len();
                aggs.extend(
                    if self.zeroed { zeroed_vec(len) } else { vec![Default::default(); len] }
                );
            }
            if val > &aggs[*ticket] {
                aggs[*ticket] = val.clone();
            }
        }
    }

    fn into_vec(self) -> Vec<Self::Agg> {
        // Did not fix for resizing, so makes bad assumption that if one thread resizes, all do.
        // This is a rare edge case even when resizing, and does not occur in experimental workloads.
        let mut iter = self.aggs.into_iter().map(|x| x.into_inner());
        let acc = iter.next().unwrap();
        let acc_ref = &acc;
        let rest = iter.collect_vec();
        let rest_ref = &rest;

        std::thread::scope(|s| {
            for t in 0..self.threads {
                s.spawn(move || {
                    for v in rest_ref.iter() {
                        for i in (acc_ref.len() * t).div_ceil(self.threads)..(acc_ref.len() * (t + 1)).div_ceil(self.threads) {
                            if v[i] > acc_ref[i] {
                                // Non-overlapping range for i makes mutation okay.
                                unsafe {
                                    *(acc_ref.as_ptr().add(i) as *mut V) = v[i].clone();
                                }
                            }
                        }
                    }
                });
            }
        });

        acc
    }
}

pub struct SumUpdater<V: Send> {
    aggs: ThreadLocal<RefCell<Vec<V>>>,
    capacity: usize,
    threads: usize,
    zeroed: bool,
}

impl<V: Send + Sync + AddAssign + Clone + Default + Zeroable> Updater<V> for SumUpdater<V> {
    type Agg = V;

    fn with_capacity_and_threads(capacity: usize, threads: usize) -> Self {
        Self {
            aggs: ThreadLocal::new(),
            capacity,
            threads,
            zeroed: false,
        }
    }

    fn with_capacity_and_threads_zeroed(capacity: usize, threads: usize) -> Self {
        Self {
            aggs: ThreadLocal::new(),
            capacity,
            threads,
            zeroed: true,
        }
    }

    fn update_vec(&self, tickets: &[usize], values: &[V]) {
        let mut aggs = self.aggs.get_or(|| RefCell::new(
            if self.zeroed { zeroed_vec(self.capacity) } else { vec![Default::default(); self.capacity] }
        )).borrow_mut();
        for (ticket, val) in tickets.iter().zip(values.iter()) {
            while *ticket >= aggs.len() {
                let len = aggs.len();
                aggs.extend(
                    if self.zeroed { zeroed_vec(len) } else { vec![Default::default(); len] }
                );
            }
            aggs[*ticket] += val.clone();
        }
    }

    fn into_vec(self) -> Vec<Self::Agg> {
        // Did not fix for resizing, so makes bad assumption that if one thread resizes, all do.
        // This is a rare edge case even when resizing, and does not occur in experimental workloads.
        let mut iter = self.aggs.into_iter().map(|x| x.into_inner());
        let acc = iter.next().unwrap();
        let acc_ref = &acc;
        let rest = iter.collect_vec();
        let rest_ref = &rest;

        std::thread::scope(|s| {
            for t in 0..self.threads {
                s.spawn(move || {
                    for v in rest_ref.iter() {
                        for i in (acc_ref.len() * t).div_ceil(self.threads)..(acc_ref.len() * (t + 1)).div_ceil(self.threads) {
                            // Non-overlapping range for i makes mutation okay.
                            unsafe {
                                *(acc_ref.as_ptr().add(i) as *mut V) += v[i].clone();
                            }
                        }
                    }
                });
            }
        });

        acc
    }
}
