use common::FuzzyCounterFinalizer;

pub trait Ticketer<K>: Sync + Send {
    fn with_capacity_and_threads(capacity: usize, threads: usize) -> Self;

    /// Lookup or insert a new ticket for each key.  Results will be written into
    /// `output`, which must be at least as large as `keys`.
    fn ticket(&self, keys: &[K], output: &mut [usize]);

    fn into_kvs(self) -> Vec<(K, usize)>;
}

pub trait KeyedTicketer<K>: Ticketer<K> {
    fn into_keys(self) -> (Vec<K>, FuzzyCounterFinalizer);
}
