use std::cell::{RefCell, RefMut};
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash, Hasher};
use std::marker::PhantomData;
use std::ops::{Index, IndexMut};
use std::sync::RwLock;

use fnv::FnvBuildHasher;
use itertools::Itertools;
use thread_local::ThreadLocal;

use crate::partitioned_aggregator::{PartitionedAggregator, PartitionedAggregatorFinalizer};

#[derive(Debug, Default, Clone)]
struct PageEntry<K: Clone + Default> {
    key: K,
    hash: u64,
    value: usize,
}

impl<K: Clone + Default> PageEntry<K> {
    fn new(key: K, hash: u64, value: usize) -> Self {
        Self { key, hash, value }
    }
}

#[derive(Clone)]
struct Page<K: Clone + Default> {
    entries: Vec<PageEntry<K>>,
}

impl<K: Clone + Default> Page<K> {
    const PAGE_SIZE: usize = 256;

    fn push(&mut self, entry: PageEntry<K>) {
        self.entries.push(entry);
    }

    fn pop(&mut self) -> Option<PageEntry<K>> {
        self.entries.pop()
    }
}

impl<K: Clone + Default> Index<usize> for Page<K> {
    type Output = PageEntry<K>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.entries[index]
    }
}

impl<K: Clone + Default> IndexMut<usize> for Page<K> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.entries[index]
    }
}

impl<K: Clone + Default> Default for Page<K> {
    fn default() -> Self {
        Self { entries: Vec::with_capacity(Self::PAGE_SIZE) }
    }
}

#[derive(Default, Clone)]
struct PaginatedHashMap<K: Clone + Default> {
    pages: Vec<Page<K>>,
    table: Vec<u32>, // [PAGE; OFFSET; HASH]
    size: usize,
    capacity: usize
}

impl<K: Clone + Default> Iterator for PaginatedHashMap<K> {
    type Item = (K, usize);

    fn next(&mut self) -> Option<Self::Item> {
        if self.size == 0 {
            return None;
        }
        self.size -= 1;
        let pages_len = self.pages.len();
        match self.pages[pages_len - 1].pop() {
            Some(entry) => {
                Some((entry.key, entry.value))
            },
            None => {
                self.pages.pop();
                let entry = self.pages[pages_len - 2].pop().unwrap();
                Some((entry.key, entry.value))
            }
        }
    }
}

impl<K: Eq + Clone + Default> PaginatedHashMap<K> {
    const HASH_BITS: u32 = 8;
    const OFFSET_BITS: u32 = Page::<K>::PAGE_SIZE.trailing_zeros();

    const OFFSET_OFFSET: u32 = Self::HASH_BITS;
    const PAGE_OFFSET: u32 = Self::HASH_BITS + Self::OFFSET_BITS;

    const HASH_MASK: u32 = (1 << Self::HASH_BITS) - 1;
    const OFFSET_MASK: u32 = ((1 << Self::OFFSET_BITS) - 1) << Self::OFFSET_OFFSET;
    const OFFSET_MASK_UNSHIFTED: u32 = (1 << Self::OFFSET_BITS) - 1;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            pages: Vec::with_capacity(capacity.div_ceil(Page::<K>::PAGE_SIZE)),
            table: vec![!0; capacity * 2],
            size: 0,
            capacity,
        }
    }

    fn insert_or_increment(&mut self, key: K, hash: u64, count: usize) -> Result<(), (K, u64)> {
        let idx = hash as usize % self.capacity;
        let mut cur_idx: usize  = idx;
        loop {
            if self.table[cur_idx] == !0 {
                if self.size == self.capacity {
                    return Err((key, hash))
                }
                if (self.size as u32 & Self::OFFSET_MASK_UNSHIFTED) == 0 {
                    self.pages.push(Page::default())
                }
                let page = self.size >> Self::OFFSET_BITS;
                self.pages[page].push(PageEntry::new(key, hash, count));
                self.table[cur_idx] = (hash as u32 & Self::HASH_MASK) | ((self.size as u32) << Self::HASH_BITS);
                self.size += 1;
                break;
            }

            let table_entry = self.table[cur_idx];
            if hash as u32 & Self::HASH_MASK == table_entry & Self::HASH_MASK {
                let page = (table_entry >>  Self::PAGE_OFFSET) as usize;
                let offset = ((table_entry & Self::OFFSET_MASK) >> Self::OFFSET_OFFSET) as usize;
                if self.pages[page][offset].key == key {
                    self.pages[page][offset].value += count;
                    break;
                }
            }

            cur_idx += 1;
            if cur_idx >= self.table.len() {
                cur_idx = 0;
            }

            if cur_idx == idx {
                return Err((key, hash))
            }
        }

        Ok(())
    }

    fn drain(&mut self) -> (Vec<Page<K>>, usize) {
        let mut tmp = Vec::<Page<K>>::with_capacity(self.capacity.div_ceil(Page::<K>::PAGE_SIZE));
        std::mem::swap(&mut self.pages, &mut tmp);
        let size = self.size;
        self.size = 0;
        self.table.iter_mut().for_each(|v| *v = !0);
        (tmp, size)
    }
}

pub struct CountAgg<K: Send + Sync + Clone + Default, V, S = FnvBuildHasher> {
    aggs: ThreadLocal<RefCell<PaginatedHashMap<K>>>,
    partitions: ThreadLocal<RefCell<Vec<Vec<PageEntry<K>>>>>,
    capacity: usize,
    threads: usize,
    bh: S,
    pd: PhantomData<V>,
}

impl<K: Send + Sync + Eq + Clone + Default, V, S> CountAgg<K, V, S> {
    const LOCAL_HASHTABLE_SIZE: usize = 10_000;

    fn flush_agg<'a>(&self, mut agg: RefMut<'a, PaginatedHashMap<K>>) -> RefMut<'a, PaginatedHashMap<K>> {
        let mut partitions = self.partitions.get_or(
            || RefCell::new(vec![Vec::new(); self.threads])
        ).borrow_mut();

        let radix_mask = self.threads as u64 - 1; // Assumes threads is power of 2.
        let (mut entries, _) = agg.drain();
        while let Some(mut page) = entries.pop() {
            while let Some(entry) = page.pop() {
                partitions[(entry.hash & radix_mask) as usize].push(entry);
            }
        }
        agg
    }
}

impl<
    K: Send + Sync + Eq + Hash + Debug + Copy + Default + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher + Send + Sync + Default,
> PartitionedAggregator<K, V> for CountAgg<K, V, S> {
    type Agg = usize;

    fn with_capacity_and_threads(capacity: usize, threads: usize) -> Self {
        Self {
            aggs: Default::default(),
            partitions: Default::default(),
            capacity,
            threads,
            bh: Default::default(),
            pd: Default::default(),
        }
    }

    fn aggregate_vec(&self, keys: &[K], _values: &[V])
    {
        let mut agg = self.aggs.get_or(
            || RefCell::new(PaginatedHashMap::<K>::with_capacity(Self::LOCAL_HASHTABLE_SIZE))
        ).borrow_mut();

        let hashes = keys
            .iter()
            .enumerate()
            .map(|(_, k)| {
                let mut hasher = self.bh.build_hasher();
                k.hash(&mut hasher);
                hasher.finish()
            })
            .collect_vec();

        for (key, hash) in keys.iter().zip(hashes.into_iter()) {
            match agg.insert_or_increment(key.clone(), hash, 1) {
                Ok(_) => {},
                Err((key, hash)) => {
                    agg = self.flush_agg(agg);
                    agg.insert_or_increment(key, hash, 1).unwrap();
                }
            };
        }
    }

    fn finalize_thread(&self) {
        let agg = self.aggs.get_or(
            || RefCell::new(PaginatedHashMap::<K>::with_capacity(Self::LOCAL_HASHTABLE_SIZE))
        ).borrow_mut();

        self.flush_agg(agg);
    }

    fn into_finalizer(self) -> Box<dyn PartitionedAggregatorFinalizer<K, V, Agg=Self::Agg>> {
        let mut reshuffled_partitions: Vec<Vec<Vec<PageEntry<K>>>> = vec![Default::default(); self.threads];
        self.partitions.into_iter().for_each(|mut thread_partitions| {
            thread_partitions
                .get_mut()
                .into_iter()
                .enumerate()
                .for_each(|(idx, partition)| {
                    reshuffled_partitions[idx].push(std::mem::take(partition));
                })
        });

        let partition_capacity = self.capacity.div_ceil(self.threads) * 6 / 5;  // Allocate slightly extra capacity b/c variance with hashing radix.
        Box::new(CountAggFinalizer::<K, V>::with_capacity_and_partitions(partition_capacity, reshuffled_partitions))
    }
}

struct CountAggFinalizer<K: Clone + Default, V> {
    partition_chunks: RwLock<Vec<Vec<Vec<PageEntry<K>>>>>,
    partition_aggs: RwLock<Vec<PaginatedHashMap<K>>>,
    capacity: usize,
    pd: PhantomData<V>,
}

impl<K: Clone + Default, V> CountAggFinalizer<K, V> {
    fn with_capacity_and_partitions(capacity: usize, partition_chunks: Vec<Vec<Vec<PageEntry<K>>>>) -> Self {
        Self {
            partition_chunks: RwLock::new(partition_chunks),
            partition_aggs: Default::default(),
            capacity,
            pd: Default::default(),
        }
    }
}

impl<
    K: Send + Sync + Eq + Debug + Clone + Default,
    V: Send + Sync
> PartitionedAggregatorFinalizer<K, V> for CountAggFinalizer<K, V> {
    type Agg = usize;

    fn finalize_thread(&self) {
        let partition = self.partition_chunks.write().unwrap().pop().unwrap();
        let mut partition_agg = PaginatedHashMap::<K>::with_capacity(self.capacity);
        for partition_chunk in partition.into_iter() {
            for entry in partition_chunk.into_iter() {
                partition_agg.insert_or_increment(entry.key, entry.hash, entry.value).unwrap();
            }
        }
        self.partition_aggs.write().unwrap().push(partition_agg);
    }

    fn into_vec(self: Box<CountAggFinalizer<K, V>>) -> (Vec<K>, Vec<usize>) {
        let mut partition_keys = Vec::<K>::new();
        let mut partition_aggs = Vec::<usize>::new();
        self.partition_aggs
            .into_inner()
            .unwrap()
            .into_iter()
            .flat_map(|aggs| aggs.into_iter())
            .for_each(|(key, agg)| {
                partition_keys.push(key);
                partition_aggs.push(agg);
            });
        (partition_keys, partition_aggs)
    }
}