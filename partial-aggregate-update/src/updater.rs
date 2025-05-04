pub trait Updater<V>: Sync + Send {
    type Agg;

    fn with_capacity_and_threads(capacity: usize, threads: usize) -> Self;

    fn update_vec(&self, tickets: &[usize], values: &[V]);

    fn into_vec(self) -> Vec<Self::Agg>;
}
