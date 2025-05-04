use std::marker::PhantomData;
use std::sync::RwLock;
use std::sync::atomic::AtomicUsize;

use atomic_traits::{Atomic, NumOps};
use itertools::Itertools;

use crate::updater::Updater;

pub struct CountUpdater<V> {
    aggs: RwLock<Vec<AtomicUsize>>,
    pd: PhantomData<V>,
}

impl<V: Send + Sync + Default> Updater<V> for CountUpdater<V> {
    type Agg = usize;

    fn with_capacity_and_threads(capacity: usize, _threads: usize) -> Self {
        let aggs = (0..capacity)
            .map(|_| AtomicUsize::new(0))
            .collect_vec();

        Self {
            aggs: RwLock::new(aggs),
            pd: Default::default(),
        }
    }

    fn update_vec(&self, tickets: &[usize], _values: &[V]) {
        let mut aggs = self.aggs.read().unwrap();
        for ticket in tickets {
            if *ticket >= aggs.len() {
                std::mem::drop(aggs);
                let mut aggs_write = self.aggs.write().unwrap();
                while *ticket >= aggs_write.len() {
                    let len = aggs_write.len();
                    aggs_write.extend((0..len).map(|_| AtomicUsize::new(0)));
                }
                std::mem::drop(aggs_write);
                aggs = self.aggs.read().unwrap();
            }

            aggs[*ticket].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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

impl<V: Atomic + Send + Sync + NumOps> Updater<<V as Atomic>::Type> for MaxUpdater<V>
where <V as Atomic>::Type: Default + Clone + Ord
{
    type Agg = <V as Atomic>::Type;

    fn with_capacity_and_threads(capacity: usize, _threads: usize) -> Self {
        let aggs = (0..capacity)
            .map(|_| <V as Atomic>::new(<V as Atomic>::Type::default()))
            .collect_vec();

        Self {
            aggs: RwLock::new(aggs),
        }
    }

    fn update_vec(&self, tickets: &[usize], values: &[<V as Atomic>::Type]) {
        let mut aggs = self.aggs.read().unwrap();

        for (ticket, val) in tickets.iter().zip(values.iter()) {
            if *ticket >= aggs.len() {
                std::mem::drop(aggs);
                let mut aggs_write = self.aggs.write().unwrap();
                while *ticket >= aggs_write.len() {
                    let len = aggs_write.len();
                    aggs_write.extend((0..len).map(|_| V::new(<V as Atomic>::Type::default())));
                }
                std::mem::drop(aggs_write);
                aggs = self.aggs.read().unwrap();
            }

            // Atomic fetch_max is somehow slower.
            // self.aggs[*ticket].fetch_max(val.clone(), std::sync::atomic::Ordering::Relaxed);
            let mut curr = aggs[*ticket].load(std::sync::atomic::Ordering::Relaxed);
            while *val > curr {
                match aggs[*ticket].compare_exchange(
                    curr,
                    val.clone(),
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed
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
