use std::cell::{RefCell, RefMut};
use std::hash::{BuildHasher, Hash, Hasher};
use std::marker::PhantomData;
use std::sync::atomic::AtomicUsize;
use std::sync::RwLock;

use bytemuck::{zeroed_vec, Zeroable};
use fnv::FnvBuildHasher;
use hashbrown::HashTable;
use thread_local::ThreadLocal;

use crate::{PartitionedAggregator, PartitionedAggregatorFinalizer};

pub struct BaseAgg<K: Send, V: Send, Agg: Send, S = FnvBuildHasher>
{
    tickets: ThreadLocal<RefCell<HashTable<(K, u64, usize)>>>,
    aggs: ThreadLocal<RefCell<Vec<Agg>>>,
    partitions: ThreadLocal<RefCell<Vec<Vec<(K, u64, Agg)>>>>,
    threads: usize,
    zeroed: bool,
    init: fn(&V) -> Agg,
    update: fn(&V, &mut Agg),
    reduce: fn(&Agg, &mut Agg),
    bh: S,
    pd: PhantomData<V>,
}

impl<
    K: Send + Sync + Eq + Clone + Default,
    V: Send,
    Agg: Send + Clone,
    S: Default + BuildHasher,
> BaseAgg<K, V, Agg, S> {
    const LOCAL_HASHTABLE_SIZE: usize = 10_000;

    pub(crate) fn new(_capacity: usize, threads: usize, zeroed: bool, init: fn(&V) -> Agg, update: fn(&V, &mut Agg), reduce: fn(&Agg, &mut Agg)) -> Self {
        Self {
            tickets: Default::default(),
            aggs: Default::default(),
            partitions: Default::default(),
            threads,
            zeroed,
            init,
            update,
            reduce,
            bh: Default::default(),
            pd: Default::default(),
        }
    }

    fn flush_agg<'a>(
        &self,
        mut tickets: RefMut<'a, HashTable<(K, u64, usize)>>,
        aggs: RefMut<'a, Vec<Agg>>
    ) -> (RefMut<'a, HashTable<(K, u64, usize)>>, RefMut<'a, Vec<Agg>>) {
        let mut partitions = self.partitions.get_or(
            || RefCell::new(vec![Vec::new(); self.threads])
        ).borrow_mut();

        for (k, hash, ticket) in tickets.drain() {
            partitions[hash as usize % self.threads].push((k, hash, aggs[ticket].clone()));
        }

        (tickets, aggs)
    }
}

impl<
    K: Zeroable + Send + Sync + Eq + Hash + Clone + Default + 'static,
    V: Zeroable + Send + Sync + 'static,
    Agg: Zeroable + Send + Sync + Clone + Default + 'static,
    S: BuildHasher + Send + Sync + Default + 'static,
> PartitionedAggregator<K, V> for BaseAgg<K, V, Agg, S> {
    type Agg = Agg;

    fn with_capacity_and_threads(_capacity: usize, _threads: usize) -> Self {
        panic!("Never call base implementation of with_capacity_and_threads in BaseAgg");
    }

    fn aggregate_vec(&self, keys: &[K], values: &[V]) {
        let mut tickets = self.tickets.get_or(
            || RefCell::new(HashTable::<(K, u64, usize)>::with_capacity(Self::LOCAL_HASHTABLE_SIZE))
        ).borrow_mut();
        let mut aggs = self.aggs.get_or(
            || RefCell::new(vec![Agg::default(); Self::LOCAL_HASHTABLE_SIZE])
        ).borrow_mut();

        for (key, value) in keys.iter().zip(values.iter()) {
            let mut hasher = self.bh.build_hasher();
            key.hash(&mut hasher);
            let hash = hasher.finish();

            let len = tickets.len();
            tickets.entry(hash, |(k, _h, _t)| k == key, |(_k, h, _t)| *h)
                .and_modify(|(_k, _h, t)| (self.update)(value, &mut aggs[*t]))
                .or_insert_with(|| {
                    aggs[len] = (self.init)(value);
                    (key.clone(), hash, len)
                });

            if (*tickets).len() >= Self::LOCAL_HASHTABLE_SIZE {
                let out = self.flush_agg(tickets, aggs);
                tickets = out.0;
                aggs = out.1;
            }
        }
    }

    fn finalize_thread(&self) {
        let tickets = self.tickets.get_or(
            || RefCell::new(HashTable::<(K, u64, usize)>::with_capacity(Self::LOCAL_HASHTABLE_SIZE))
        ).borrow_mut();
        let aggs = self.aggs.get_or(
            || RefCell::new(vec![Agg::default(); Self::LOCAL_HASHTABLE_SIZE])
        ).borrow_mut();

        self.flush_agg(tickets, aggs);
    }

    fn into_finalizer(self) -> Box<dyn PartitionedAggregatorFinalizer<K, V, Agg=Self::Agg>> {
        let mut reshuffled_partitions: Vec<Vec<Vec<(K, u64, Agg)>>> = vec![Default::default(); self.threads];

        self.partitions.into_iter().for_each(|mut thread_partitions| {
            thread_partitions
                .get_mut()
                .into_iter()
                .enumerate()
                .for_each(|(idx, partition)| {
                    reshuffled_partitions[idx].push(std::mem::take(partition));
                })
        });

        // Pass 0 capacity. Implementation now does not pre-allocate, as single-thread vector
        // and hash map does not really benefit from pre-sizing anyway, and it can cause some
        // issues in partitioning in particular because partition size can be less predictable.
        Box::new(BaseAggFinalizer::<K, V, Agg, S>::with_capacity_and_partitions(0, self.zeroed, reshuffled_partitions, self.reduce))
    } }

struct BaseAggFinalizer<K: Clone + Default, V, Agg, S = FnvBuildHasher> {
    partition_chunks: RwLock<Vec<Vec<Vec<(K, u64, Agg)>>>>,
    partition_hts: RwLock<Vec<HashTable<(K, Agg)>>>,
    zeroed: bool,
    reduce: fn(&Agg, &mut Agg),
    bh: S,
    pd: PhantomData<V>,
}

impl<K: Clone + Default, V, Agg, S: Default> BaseAggFinalizer<K, V, Agg, S> {
    fn with_capacity_and_partitions(
        _capacity: usize,
        zeroed: bool,
        partition_chunks: Vec<Vec<Vec<(K, u64, Agg)>>>,
        reduce: fn(&Agg, &mut Agg),
    ) -> Self {
        Self {
            partition_chunks: RwLock::new(partition_chunks),
            partition_hts: Default::default(),
            zeroed,
            reduce,
            bh: Default::default(),
            pd: Default::default(),
        }
    }
}


impl<
    K: Zeroable + Send + Sync + Hash + Eq + Clone + Default,
    V: Zeroable + Send + Sync,
    Agg: Zeroable + Send + Sync + Default + Clone,
    S: BuildHasher + Send + Sync + Default,
> PartitionedAggregatorFinalizer<K, V> for BaseAggFinalizer<K, V, Agg, S> {
    type Agg = Agg;

    fn finalize_thread(&self) {
        let partition = self.partition_chunks.write().unwrap().pop().unwrap();
        let mut partition_ht = HashTable::<(K, Agg)>::new();

        for partition_chunk in partition.iter() {
            for (key, hash, value) in partition_chunk.into_iter() {
                partition_ht
                    .entry(*hash, |val| &val.0 == key, |val| self.bh.hash_one(val.0.clone()))
                    .and_modify(|(_k, agg)| {
                        (self.reduce)(value, agg);
                    }).or_insert_with(|| {
                    (key.clone(), value.clone())
                });
            }
        }
        self.partition_hts.write().unwrap().push(partition_ht);
    }

    fn into_vec(self: Box<Self>) -> (Vec<K>, Vec<Self::Agg>) {
        let partitions = self.partition_hts.read().unwrap().len();
        let len = self.partition_hts.read()
            .unwrap()
            .iter()
            .map(|ht| ht.len())
            .sum::<usize>();

        let keys: Vec<K> = if self.zeroed { zeroed_vec(len) } else { (0..len).map(|_| K::default()).collect() };
        let aggs: Vec<Agg> = if self.zeroed { zeroed_vec(len) } else { (0..len).map(|_| Agg::default()).collect() };

        let head = AtomicUsize::new(0);
        std::thread::scope(|s| {
            for _ in 0..partitions {
                s.spawn(|| {
                    let ht = self.partition_hts.write().unwrap().pop().unwrap();
                    let mut head = head.fetch_add(ht.len(), std::sync::atomic::Ordering::Relaxed);
                    // Safe because non-overlapping copy.
                    ht.into_iter()
                        .for_each(|(k, v)| unsafe {
                            (keys.as_ptr() as *mut K).add(head).write(k);
                            (aggs.as_ptr() as *mut Agg).add(head).write(v);
                            head += 1;
                        });
                });
            }
        });

        (keys, aggs)
    }
}

