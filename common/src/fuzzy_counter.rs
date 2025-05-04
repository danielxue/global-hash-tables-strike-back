use std::cell::{RefCell, RefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use itertools::Itertools;
use thread_local::ThreadLocal;

#[derive(Default)]
pub struct FuzzyCounter {
    ub: AtomicUsize,
    thread_counters: ThreadLocal<RefCell<(usize, usize)>>, // (cur value, max non-inclusive).
    step_size: usize,
}

impl FuzzyCounter {
    pub const DEFAULT_STEP_SIZE: usize = 256;

    pub fn new(v: usize) -> Self {
        Self {
            ub: AtomicUsize::new(v),
            thread_counters: Default::default(),
            step_size: Self::DEFAULT_STEP_SIZE,
        }
    }

    pub fn new_with_step_size(v: usize, step_size: usize) -> Self {
        Self {
            ub: AtomicUsize::new(v),
            thread_counters: Default::default(),
            step_size,
        }
    }

    pub fn get_thread_counter(&self) -> RefMut<(usize, usize)> {
        self.thread_counters.get_or(|| {
            let lb = self.ub.fetch_add(self.step_size, Ordering::Relaxed);
            RefCell::new((lb, lb + self.step_size))
        }).borrow_mut()
    }

    pub fn fetch_increment(&self, thread_counter: &mut RefMut<(usize, usize)>) -> usize {
        let count = thread_counter.0;
        thread_counter.0 += 1;
        if thread_counter.0 == thread_counter.1 {
            thread_counter.0 = self.ub.fetch_add(self.step_size, Ordering::Relaxed);
            thread_counter.1 = thread_counter.0 + self.step_size;
        }
        count
    }

    pub fn fetch(&self, thread_counter: &mut RefMut<(usize, usize)>) -> usize {
        thread_counter.0
    }

    pub fn increment(&self, thread_counter: &mut RefMut<(usize, usize)>) {
        thread_counter.0 += 1;
        if thread_counter.0 == thread_counter.1 {
            thread_counter.0 = self.ub.fetch_add(self.step_size, Ordering::Relaxed);
            thread_counter.1 = thread_counter.0 + self.step_size;
        }
    }

    pub fn len(&self) -> usize {
        self.ub.load(Ordering::Relaxed)
    }

    pub fn step_size(&self) -> usize {
        self.step_size
    }

    pub fn into_finalizer(self) -> FuzzyCounterFinalizer {
        let ranges = self.thread_counters
            .into_iter()
            .map(|cell| cell.into_inner())
            .sorted()
            .collect_vec();
        FuzzyCounterFinalizer { ranges }
    }
}

pub struct FuzzyCounterFinalizer {
    ranges: Vec<(usize, usize)>,
}

impl FuzzyCounterFinalizer {
    pub fn reorder_slice<K>(&self, slice: &mut [K]) -> usize {
        if self.ranges.is_empty() {
            return 0;
        }

        let mut write = self.ranges[0].0;
        let mut read = self.ranges[0].1;
        for idx in 1..self.ranges.len() {
            while read < self.ranges[idx].0 {
                read += 1;
                write += 1;
                slice.swap(write , read);
            }
            read = self.ranges[idx].1;
        }
        write
    }
}