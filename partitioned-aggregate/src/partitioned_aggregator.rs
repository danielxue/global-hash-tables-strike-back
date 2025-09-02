pub trait PartitionedAggregator<K, V>: Sync + Send {
    type Agg;

    fn with_capacity_and_threads(capacity: usize, threads: usize) -> Self;

    fn with_capacity_and_threads_zeroed(capacity: usize, threads: usize) -> Self where Self: Sized {
        Self::with_capacity_and_threads(capacity, threads)
    }

    fn aggregate_vec(&self, tickets: &[K], values: &[V]);

    fn finalize_thread(&self);

    fn into_finalizer(self) -> Box<dyn PartitionedAggregatorFinalizer<K, V, Agg=Self::Agg>>;
}

pub trait PartitionedAggregatorFinalizer<K, V>: Sync + Send {
    type Agg;

    fn finalize_thread(&self);

    fn into_vec(self: Box<Self>) -> (Vec<K>, Vec<Self::Agg>);
}
