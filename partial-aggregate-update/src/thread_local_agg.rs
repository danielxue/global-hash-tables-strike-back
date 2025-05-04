use std::cell::RefCell;
use std::marker::PhantomData;

use rayon::prelude::{IndexedParallelIterator, ParallelIterator, ParallelSlice, ParallelSliceMut};
use thread_local::ThreadLocal;

use crate::Updater;

pub struct CountUpdater<V> {
    aggs: ThreadLocal<RefCell<Vec<usize>>>,
    capacity: usize,
    threads: usize,
    pd: PhantomData<V>,
}

impl<V: Send + Sync + Default> Updater<V> for CountUpdater<V> {
    type Agg = usize;

    fn with_capacity_and_threads(capacity: usize, threads: usize) -> Self {
        Self {
            aggs: ThreadLocal::new(),
            capacity,
            threads,
            pd: Default::default(),
        }
    }

    fn update_vec(&self, tickets: &[usize], _values: &[V]) {
        let mut aggs = self.aggs.get_or(|| RefCell::new(vec![0; self.capacity])).borrow_mut();
        for ticket in tickets {
            while *ticket >= aggs.len() {
                let len = aggs.len();
                aggs.extend((0..len).map(|_| 0));
            }
            aggs[*ticket] += 1;
        }
    }

    fn into_vec(self) -> Vec<Self::Agg> {
        // Did not fix for resizing, so makes bad assumption that if one thread resizes, all do. This is fine for our experiment workloads though.
        let mut iter = self.aggs.into_iter();
        let mut acc = iter.next().unwrap().into_inner();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.threads)
            .build()
            .unwrap();
        pool.install(|| {
            iter.for_each(|ele|
                ele.into_inner()
                    .par_chunks(self.capacity.div_ceil(self.threads))
                    .zip(acc.par_chunks_mut(self.capacity.div_ceil(self.threads)))
                    .for_each(|(a, b)| {
                        a.iter()
                            .zip(b.iter_mut())
                            .for_each(|(a, b)| {
                                *b += a;
                            })
                    })
            );
        });
        acc
    }
}
