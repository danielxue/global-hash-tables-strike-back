use std::cell::{RefCell, RefMut};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use itertools::Itertools;
use thread_local::ThreadLocal;

pub struct LocalCounter<'a> {
    copy: (usize, usize),
    reference: RefMut<'a, (usize, usize)>,
}

impl<'a> LocalCounter<'a> {
    fn new(reference: RefMut<'a, (usize, usize)>) -> Self {
        Self { copy: reference.clone(), reference }
    }
}

// Only update underlying counter on drop in order to prevent false sharing thread local variables.
impl Drop for LocalCounter<'_> {
    fn drop(&mut self) {
        *self.reference = self.copy;
    }
}

impl Deref for LocalCounter<'_> {
    type Target = (usize, usize);

    fn deref(&self) -> &Self::Target {
        &self.copy
    }
}

impl DerefMut for LocalCounter<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.copy
    }
}

pub struct FuzzyCounter {
    ub: AtomicUsize,
    thread_counters: ThreadLocal<RefCell<(usize, usize)>>, // (cur value, max non-inclusive).
    step_size: usize,
}

impl Default for FuzzyCounter {
    fn default() -> Self {
        Self {
            ub: AtomicUsize::new(0),
            thread_counters: Default::default(),
            step_size: Self::DEFAULT_STEP_SIZE,
        }
    }
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

    pub fn with_step_size(v: usize, step_size: usize) -> Self {
        Self {
            ub: AtomicUsize::new(v),
            thread_counters: Default::default(),
            step_size,
        }
    }

    pub fn get_thread_counter(&self) -> LocalCounter<'_> {
        let reference = self.thread_counters.get_or(|| {
            let lb = self.ub.fetch_add(self.step_size, Ordering::Relaxed);
            RefCell::new((lb, lb + self.step_size))
        }).borrow_mut();
        LocalCounter::new(reference)
    }

    pub fn fetch_increment(&self, thread_counter: &mut LocalCounter) -> usize {
        let count = thread_counter.0;
        thread_counter.0 += 1;
        if thread_counter.0 == thread_counter.1 {
            thread_counter.0 = self.ub.fetch_add(self.step_size, Ordering::Relaxed);
            thread_counter.1 = thread_counter.0 + self.step_size;
        }
        count
    }

    pub fn fetch(&self, thread_counter: &LocalCounter) -> usize {
        thread_counter.0
    }

    pub fn increment(&self, thread_counter: &mut LocalCounter) {
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
                slice.swap(write, read);
                read += 1;
                write += 1;
            }
            read = self.ranges[idx].1;
        }
        write
    }
}
