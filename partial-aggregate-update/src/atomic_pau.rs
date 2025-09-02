use std::marker::PhantomData;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use atomic_traits::{Atomic, NumOps};
use bytemuck::{zeroed_vec, Zeroable};
use itertools::Itertools;
use num_traits::NumCast;
use crate::updater::Updater;

pub struct CountUpdater<V> {
    aggs: RwLock<Vec<AtomicUsize>>,
    pd: PhantomData<V>,
}

impl<V: Send + Sync + Default> Updater<V> for CountUpdater<V> {
    type Agg = usize;

    fn with_capacity_and_threads(capacity: usize, _threads: usize) -> Self {
        Self {
            aggs: RwLock::new((0..capacity).map(|_| Default::default()).collect()),
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
        let mut aggs = self.aggs.read().unwrap();
        for ticket in tickets {
            if *ticket >= aggs.len() {
                drop(aggs);
                let mut aggs_write = self.aggs.write().unwrap();
                while *ticket >= aggs_write.len() {
                    let len = aggs_write.len();
                    aggs_write.extend(zeroed_vec(len));
                }
                drop(aggs_write);
                aggs = self.aggs.read().unwrap();
            }

            aggs[*ticket].fetch_add(1, Ordering::Relaxed);
        }
    }

    fn into_vec(self) -> Vec<Self::Agg> {
        self.aggs
            .into_inner()
            .unwrap()
            .into_iter()
            .map(|v| v.into_inner())
            .collect_vec()
    }
}

pub struct MaxUpdater<V: Atomic> {
    aggs: RwLock<Vec<V>>,
}

impl<V: Atomic + Send + Sync + Zeroable> Updater<<V as Atomic>::Type> for MaxUpdater<V>
where <V as Atomic>::Type: Default + Clone + PartialOrd
{
    type Agg = <V as Atomic>::Type;

    fn with_capacity_and_threads(capacity: usize, _threads: usize) -> Self {
        Self {
            aggs: RwLock::new((0..capacity).map(|_| V::new(V::Type::default())).collect_vec()),
        }
    }

    fn with_capacity_and_threads_zeroed(capacity: usize, _threads: usize) -> Self {
        Self {
            aggs: RwLock::new(zeroed_vec(capacity)),
        }
    }

    fn update_vec(&self, tickets: &[usize], values: &[<V as Atomic>::Type]) {
        let mut aggs = self.aggs.read().unwrap();

        for (ticket, val) in tickets.iter().zip(values.iter()) {
            if *ticket >= aggs.len() {
                drop(aggs);
                let mut aggs_write = self.aggs.write().unwrap();
                while *ticket >= aggs_write.len() {
                    let len = aggs_write.len();
                    aggs_write.extend(zeroed_vec(len));
                }
                drop(aggs_write);
                aggs = self.aggs.read().unwrap();
            }

            // Atomic fetch_max is somehow slower.
            // self.aggs[*ticket].fetch_max(val.clone(), Ordering::Relaxed);
            let mut curr = aggs[*ticket].load(Ordering::Relaxed);
            while *val > curr {
                match aggs[*ticket].compare_exchange(
                    curr,
                    val.clone(),
                    Ordering::Relaxed,
                    Ordering::Relaxed
                ) {
                    Ok(_) => break,
                    Err(res) => curr = res,
                }
            }
        }
    }

    fn into_vec(self) -> Vec<Self::Agg> {
        self.aggs
            .into_inner()
            .unwrap()
            .into_iter()
            .map(|v| v.into_inner())
            .collect_vec()
    }
}

pub struct SumUpdater<V: Atomic> {
    aggs: RwLock<Vec<V>>,
}

impl<V: Atomic + Send + Sync + NumOps + Zeroable> Updater<<V as Atomic>::Type> for SumUpdater<V>
where <V as Atomic>::Type: Default + Clone
{
    type Agg = <V as Atomic>::Type;

    fn with_capacity_and_threads(capacity: usize, _threads: usize) -> Self {
        Self {
            aggs: RwLock::new((0..capacity).map(|_| V::new(<V as Atomic>::Type::default())).collect_vec()),
        }
    }

    fn with_capacity_and_threads_zeroed(capacity: usize, _threads: usize) -> Self {
        Self {
            aggs: RwLock::new(zeroed_vec(capacity)),
        }
    }

    fn update_vec(&self, tickets: &[usize], values: &[<V as Atomic>::Type]) {
        let mut aggs = self.aggs.read().unwrap();

        for (ticket, val) in tickets.iter().zip(values.iter()) {
            if *ticket >= aggs.len() {
                drop(aggs);
                let mut aggs_write = self.aggs.write().unwrap();
                while *ticket >= aggs_write.len() {
                    let len = aggs_write.len();
                    aggs_write.extend(zeroed_vec(len));
                }
                drop(aggs_write);
                aggs = self.aggs.read().unwrap();
            }

            aggs[*ticket].fetch_add(val.clone(), Ordering::Relaxed);
        }
    }

    fn into_vec(self) -> Vec<Self::Agg> {
        self.aggs
            .into_inner()
            .unwrap()
            .into_iter()
            .map(|v| v.into_inner())
            .collect_vec()
    }
}
pub struct AvgUpdater<V> {
    aggs: RwLock<Vec<AtomicU64>>,
    pd: PhantomData<V>,
}

impl<V: NumCast + Clone + Send + Sync + Default> Updater<V> for AvgUpdater<V> {
    type Agg = f32;

    fn with_capacity_and_threads(capacity: usize, _threads: usize) -> Self {
        Self {
            aggs: RwLock::new((0..capacity).map(|_| Default::default()).collect_vec()),
            pd: Default::default(),
        }
    }

    fn with_capacity_and_threads_zeroed(capacity: usize, _threads: usize) -> Self {
        Self {
            aggs: RwLock::new(zeroed_vec(capacity)),
            pd: Default::default(),
        }
    }

    fn update_vec(&self, tickets: &[usize], values: &[V])
    {
        let mut aggs = self.aggs.read().unwrap();

        for (ticket, val) in tickets.iter().zip(values.iter()) {
            if *ticket >= aggs.len() {
                drop(aggs);
                let mut aggs_write = self.aggs.write().unwrap();
                while *ticket >= aggs_write.len() {
                    let len = aggs_write.len();
                    aggs_write.extend(zeroed_vec(len));
                }
                drop(aggs_write);
                aggs = self.aggs.read().unwrap();
            }

            unsafe {
                let val = <f32 as NumCast>::from(val.clone()).unwrap();
                loop {
                    let load = aggs[*ticket].load(Ordering::Relaxed);
                    let (mut avg, x) = std::mem::transmute::<u64, (f32, u32)>(load);
                    if x == 0 {
                        avg = val;
                    } else {
                        avg = (x as f32) / (x as f32 + 1f32) * avg + val / (x as f32 + 1f32);
                    }

                    if aggs[*ticket].compare_exchange(
                        load,
                        std::mem::transmute::<(f32, u32), u64>((avg, x + 1)),
                        Ordering::Relaxed,
                        Ordering::Relaxed
                    ).is_ok() {
                        break;
                    }
                }
            }
        }
    }

    fn into_vec(self) -> Vec<Self::Agg> {
        self.aggs
            .into_inner()
            .unwrap()
            .into_iter()
            .map(|v| unsafe {
                std::mem::transmute::<u64, (f32, u32)>(v.into_inner()).0
            })
            .collect_vec()
    }
}
