use std::cell::{RefCell, RefMut};
use std::hash::{BuildHasher, Hash, Hasher};
use std::marker::PhantomData;
use std::sync::RwLock;

use fnv::FnvBuildHasher;
use thread_local::ThreadLocal;

use crate::partitioned_aggregator::{PartitionedAggregator, PartitionedAggregatorFinalizer};

struct LinearProbingMap<K> where K: Send {
    table: Vec<(K, u64, usize)>,
    len: usize,
}

impl<K: Send + Eq + Hash + Default + Clone> LinearProbingMap<K> {
    const LF: f64 = 0.5;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            table: vec![Default::default(); (capacity as f64 / Self::LF).ceil() as usize],
            len: 0,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn lookup_or_insert_hash(&mut self, key: &K, hash: u64) -> usize {
        let mut hash_mod = hash as usize % self.table.len();
        let mut table_pos = hash_mod;
        loop {
            if self.table[table_pos].2 != 0 && &self.table[table_pos].0 == key {
                return self.table[table_pos].2 - 1;
            }

            if self.table[table_pos].2 == 0 {
                let len = self.len;
                self.len += 1;
                self.table[table_pos] = (key.clone(), hash, len + 1);
                return len;
            }

            table_pos += 1;
            if table_pos >= self.table.len() {
                table_pos = 0;
            }

            if table_pos == hash_mod {
                self.resize();

                // retry insert.
                hash_mod = hash as usize % self.table.len();
                table_pos = hash_mod;
                continue;
            }
        }
    }

    fn resize(&mut self) {
        let mut tmp = vec![Default::default(); self.table.len() * 2];
        std::mem::swap(&mut tmp, &mut self.table);

        'outer: for (key, hash, ticket) in tmp {
            if ticket == 0 {
                continue;
            }

            let hash_mod = hash as usize % self.table.len();
            let mut table_pos = hash_mod;
            loop {
                if self.table[table_pos].2 == 0 {
                    self.table[table_pos] = (key, hash, self.len);
                    self.len += 1;
                    continue 'outer;
                }

                table_pos += 1;
                if table_pos >= self.table.len() {
                    table_pos = 0;
                }
            }
        }
    }

    fn drain(&mut self) -> Vec<(K, u64, usize)> {
        let mut res = Vec::with_capacity(self.len);
        for idx in 0..self.table.len() {
            if self.table[idx].2 != 0 {
                self.table[idx].2 -= 1;
                res.push(std::mem::take(&mut self.table[idx]));
            }
        }
        self.len = 0;
        res
    }
}

pub struct CountAgg<K: Send + Sync + Clone + Default, V, S = FnvBuildHasher> {
    tickets: ThreadLocal<RefCell<LinearProbingMap<K>>>,
    aggs: ThreadLocal<RefCell<Vec<usize>>>,
    tmp_hashes: ThreadLocal<RefCell<Vec<u64>>>,
    spilled_partitions: ThreadLocal<RefCell<Vec<Vec<(K, u64, usize)>>>>,
    capacity: usize,
    threads: usize,
    bh: S,
    pd: PhantomData<V>,
}

impl<
    K: Hash + Send + Sync + Eq + Clone + Default,
    V,
    S: BuildHasher + Default + Send,
> CountAgg<K, V, S> {
    const LOCAL_HASHTABLE_SIZE: usize = 10_000;

    fn flush_agg<'a>(&self, mut tickets: RefMut<'a, LinearProbingMap<K>>, aggs: RefMut<'a, Vec<usize>>) -> (RefMut<'a, LinearProbingMap<K>>, RefMut<'a, Vec<usize>>) {
        let mut partitions = self.spilled_partitions.get_or(
            || RefCell::new(vec![Vec::new(); self.threads])
        ).borrow_mut();

        let radix_mask = (self.threads - 1) as u64; // Assumes threads is power of 2.
        for (k, hash, ticket) in tickets.drain() {
            let partition_idx = hash & radix_mask;
            partitions[partition_idx as usize].push((k, hash, aggs[ticket]));
        }

        (tickets, aggs)
    }
}

impl<
    K: Send + Sync + Eq + Hash + Copy + Default + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher + Send + Sync + Default + 'static,
> PartitionedAggregator<K, V> for CountAgg<K, V, S> {
    type Agg = usize;

    fn with_capacity_and_threads(capacity: usize, threads: usize) -> Self {
        Self {
            tickets: Default::default(),
            tmp_hashes: Default::default(),
            aggs: Default::default(),
            spilled_partitions: Default::default(),
            capacity,
            threads,
            bh: Default::default(),
            pd: Default::default(),
        }
    }

    fn aggregate_vec(&self, keys: &[K], _values: &[V]) {
        let mut tmp_hashes = self.tmp_hashes.get_or(
            || RefCell::new(vec![0; keys.len()])
        ).borrow_mut();

        let mut tickets = self.tickets.get_or(
            || RefCell::new(LinearProbingMap::<K>::with_capacity(Self::LOCAL_HASHTABLE_SIZE))
        ).borrow_mut();

        let mut aggs = self.aggs.get_or(
            || RefCell::new(vec![0; Self::LOCAL_HASHTABLE_SIZE])
        ).borrow_mut();

        keys.iter()
            .zip(tmp_hashes.iter_mut())
            .for_each(|(k, o)| {
                let mut hasher = self.bh.build_hasher();
                k.hash(&mut hasher);
                *o = hasher.finish();
            });

        for (key, &hash) in keys.iter().zip(tmp_hashes.iter()) {
            let len = tickets.len();
            let ticket = tickets.lookup_or_insert_hash(key, hash);
            if len == ticket {
                aggs[ticket] = 1;
            } else {
                aggs[ticket] += 1;
            }

            if tickets.len() >= Self::LOCAL_HASHTABLE_SIZE {
                let out = self.flush_agg(tickets, aggs);
                tickets = out.0;
                aggs = out.1;
            }
        }
    }

    fn finalize_thread(&self) {
        let tickets = self.tickets.get_or(
            || RefCell::new(LinearProbingMap::<K>::with_capacity(Self::LOCAL_HASHTABLE_SIZE))
        ).borrow_mut();

        let aggs = self.aggs.get_or(
            || RefCell::new(vec![0; Self::LOCAL_HASHTABLE_SIZE])
        ).borrow_mut();

        self.flush_agg(tickets, aggs);
    }

    fn into_finalizer(self) -> Box<dyn PartitionedAggregatorFinalizer<K, V, Agg=Self::Agg>> {
        let mut reshuffled_partitions: Vec<Vec<Vec<(K, u64, usize)>>> = vec![Default::default(); self.threads];

        self.spilled_partitions.into_iter().for_each(|mut thread_partitions| {
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
    partition_chunks: RwLock<Vec<Vec<Vec<(K, u64, usize)>>>>,
    partition_keys: RwLock<Vec<Vec<K>>>,
    partition_aggs: RwLock<Vec<Vec<usize>>>,
    capacity: usize,
    pd: PhantomData<V>,
}

impl<K: Clone + Default, V> CountAggFinalizer<K, V> {
    fn with_capacity_and_partitions(capacity: usize, partition_chunks: Vec<Vec<Vec<(K, u64, usize)>>>) -> Self {
        Self {
            partition_chunks: RwLock::new(partition_chunks),
            partition_keys: Default::default(),
            partition_aggs: Default::default(),
            capacity,
            pd: Default::default(),
        }
    }
}


impl<
    K: Send + Sync + Hash + Eq + Clone + Default,
    V: Send + Sync,
> PartitionedAggregatorFinalizer<K, V> for CountAggFinalizer<K, V> {
    type Agg = usize;

    fn finalize_thread(&self) {
        let partition = self.partition_chunks.write().unwrap().pop().unwrap();
        let mut partition_tickets = LinearProbingMap::<K>::with_capacity(self.capacity);
        let mut partition_keys = Vec::<K>::with_capacity(self.capacity);
        let mut partition_aggs = Vec::<usize>::with_capacity(self.capacity);
        for partition_chunk in partition.iter() {
            for (key, hash, value) in partition_chunk.into_iter() {
                let len = partition_tickets.len();
                let ticket = partition_tickets.lookup_or_insert_hash(key, *hash);
                if len != partition_tickets.len() {
                    partition_keys.push(key.clone());
                    partition_aggs.push(*value);
                } else {
                    partition_aggs[ticket] +=  *value;
                }
            }
        }
        self.partition_keys.write().unwrap().push(partition_keys);
        self.partition_aggs.write().unwrap().push(partition_aggs);
    }

    fn into_vec(self: Box<Self>) -> (Vec<K>, Vec<usize>) {
        let partition_keys_inner = self.partition_keys.into_inner().unwrap();
        let partition_aggs_inner = self.partition_aggs.into_inner().unwrap();
        let len = partition_keys_inner.iter().map(|v| v.len()).sum::<usize>();
        let mut partition_keys: Vec<K> = Vec::with_capacity(len);
        let mut partition_aggs: Vec<usize> = Vec::with_capacity(len);
        partition_keys_inner
            .into_iter()
            .for_each(|keys| partition_keys.extend(keys));
        partition_aggs_inner
            .into_iter()
            .for_each(|aggs| partition_aggs.extend(aggs));
        (partition_keys, partition_aggs)
    }
}
