pub trait Updater<V>: Sync + Send {
    type Agg;

    fn with_capacity_and_threads(capacity: usize, threads: usize) -> Self;

    fn with_capacity_and_threads_zeroed(capacity: usize, threads: usize) -> Self where Self: Sized {
        Self::with_capacity_and_threads(capacity, threads)
    }

    fn update_vec(&self, tickets: &[usize], values: &[V]);

    fn into_vec(self) -> Vec<Self::Agg>;
}
