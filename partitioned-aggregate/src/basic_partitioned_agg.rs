use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;
use std::ops::AddAssign;

use bytemuck::{Zeroable};
use fnv::FnvBuildHasher;

use crate::partitioned_aggregator::{PartitionedAggregator, PartitionedAggregatorFinalizer};
use crate::base_agg::BaseAgg;

pub struct CountAgg<K: Send + Sync + Clone + Default, V: Send + Sync, S = FnvBuildHasher> {
    base_agg: BaseAgg<K, V, usize, S>,
}

impl<
    K: Zeroable + Send + Sync + Eq + Hash + Clone + Default + 'static,
    V: Zeroable + Send + Sync + 'static,
    S: BuildHasher + Send + Sync + Clone + Default + 'static,
> PartitionedAggregator<K, V> for CountAgg<K, V, S> {
    type Agg = usize;

    fn with_capacity_and_threads(capacity: usize, threads: usize) -> Self {
        Self {
            base_agg: BaseAgg::<K, V, usize, S>::new(
                capacity,
                threads,
                false,
                |_value: &V| { 1usize },
                |_value: &V, agg: &mut Self::Agg| { *agg += 1usize; },
                |new: &Self::Agg, agg: &mut Self::Agg| { *agg += new; },
            )
        }
    }

    fn with_capacity_and_threads_zeroed(capacity: usize, threads: usize) -> Self {
        Self {
            base_agg: BaseAgg::<K, V, usize, S>::new(
                capacity,
                threads,
                true,
                |_value: &V| { 1usize },
                |_value: &V, agg: &mut Self::Agg| { *agg += 1usize; },
                |new: &Self::Agg, agg: &mut Self::Agg| { *agg += new; },
            )
        }
    }


    fn aggregate_vec(&self, keys: &[K], _values: &[V]) {
        self.base_agg.aggregate_vec(keys, _values);
    }

    fn finalize_thread(&self) {
        self.base_agg.finalize_thread();
    }

    fn into_finalizer(self) -> Box<dyn PartitionedAggregatorFinalizer<K, V, Agg=Self::Agg>> {
        Box::new(CountAggFinalizer::<K, V>::new(self.base_agg.into_finalizer()))
    }

}

struct CountAggFinalizer<K: Clone + Default, V> {
    base_agg_finalizer: Box<dyn PartitionedAggregatorFinalizer<K, V, Agg=usize>>,
    pd: PhantomData<V>,
}

impl<K: Clone + Default, V> CountAggFinalizer<K, V> {
    fn new(base_agg_finalizer: Box<dyn PartitionedAggregatorFinalizer<K, V, Agg=usize>>) -> Self {
        Self {
            base_agg_finalizer,
            pd: Default::default(),
        }
    }
}


impl<
    K: Zeroable + Send + Sync + Hash + Eq + Clone + Default,
    V: Zeroable + Send + Sync,
> PartitionedAggregatorFinalizer<K, V> for CountAggFinalizer<K, V> {
    type Agg = usize;

    fn finalize_thread(&self) {
        self.base_agg_finalizer.finalize_thread();
    }

    fn into_vec(self: Box<Self>) -> (Vec<K>, Vec<Self::Agg>) {
        self.base_agg_finalizer.into_vec()
    }
}

pub struct MaxAgg<K: Send + Sync + Clone + Default, V: Send + Sync, S = FnvBuildHasher> {
    base_agg: BaseAgg<K, V, V, S>,
}

impl<
    K: Zeroable + Send + Sync + Eq + Hash + Clone + Default + 'static,
    V: Zeroable + PartialOrd + Send + Sync + Default + Clone + 'static,
    S: BuildHasher + Send + Sync + Clone + Default + 'static,
> PartitionedAggregator<K, V> for MaxAgg<K, V, S> {
    type Agg = V;

    fn with_capacity_and_threads(capacity: usize, threads: usize) -> Self {
        Self {
            base_agg: BaseAgg::<K, V, V, S>::new(
                capacity,
                threads,
                false,
                |value: &V| { value.clone() },
                |value: &V, agg: &mut Self::Agg| { if value > agg { *agg = value.clone() } },
                |new: &Self::Agg, agg: &mut Self::Agg| { if new > agg { *agg = new.clone() } },
            )
        }
    }

    fn with_capacity_and_threads_zeroed(capacity: usize, threads: usize) -> Self {
        Self {
            base_agg: BaseAgg::<K, V, V, S>::new(
                capacity,
                threads,
                true,
                |value: &V| { value.clone() },
                |value: &V, agg: &mut Self::Agg| { if value > agg { *agg = value.clone() } },
                |new: &Self::Agg, agg: &mut Self::Agg| { if new > agg { *agg = new.clone() } },
            )
        }
    }


    fn aggregate_vec(&self, keys: &[K], values: &[V]) {
        self.base_agg.aggregate_vec(keys, values);
    }

    fn finalize_thread(&self) {
        self.base_agg.finalize_thread();
    }

    fn into_finalizer(self) -> Box<dyn PartitionedAggregatorFinalizer<K, V, Agg=Self::Agg>> {
        Box::new(MaxAggFinalizer::<K, V>::new(self.base_agg.into_finalizer()))
    }

}

struct MaxAggFinalizer<K: Clone + Default, V> {
    base_agg_finalizer: Box<dyn PartitionedAggregatorFinalizer<K, V, Agg=V>>,
    pd: PhantomData<V>,
}

impl<K: Clone + Default, V> MaxAggFinalizer<K, V> {
    fn new(base_agg_finalizer: Box<dyn PartitionedAggregatorFinalizer<K, V, Agg=V>>) -> Self {
        Self {
            base_agg_finalizer,
            pd: Default::default(),
        }
    }
}


impl<
    K: Zeroable + Send + Sync + Hash + Eq + Clone + Default,
    V: Zeroable + Send + Sync,
> PartitionedAggregatorFinalizer<K, V> for MaxAggFinalizer<K, V> {
    type Agg = V;

    fn finalize_thread(&self) {
        self.base_agg_finalizer.finalize_thread();
    }

    fn into_vec(self: Box<Self>) -> (Vec<K>, Vec<Self::Agg>) {
        self.base_agg_finalizer.into_vec()
    }
}

pub struct SumAgg<K: Send + Sync + Clone + Default, V: Send + Sync, S = FnvBuildHasher> {
    base_agg: BaseAgg<K, V, V, S>,
}

impl<
    K: Zeroable + Send + Sync + Eq + Hash + Clone + Default + 'static,
    V: Zeroable + AddAssign + Send + Sync + Default + Clone + 'static,
    S: BuildHasher + Send + Sync + Clone + Default + 'static,
> PartitionedAggregator<K, V> for SumAgg<K, V, S> {
    type Agg = V;

    fn with_capacity_and_threads(capacity: usize, threads: usize) -> Self {
        Self {
            base_agg: BaseAgg::<K, V, V, S>::new(
                capacity,
                threads,
                false,
                |value: &V| { value.clone() },
                |value: &V, agg: &mut Self::Agg| { *agg += value.clone(); },
                |new: &Self::Agg, agg: &mut Self::Agg| { *agg += new.clone(); },
            )
        }
    }

    fn with_capacity_and_threads_zeroed(capacity: usize, threads: usize) -> Self {
        Self {
            base_agg: BaseAgg::<K, V, V, S>::new(
                capacity,
                threads,
                true,
                |value: &V| { value.clone() },
                |value: &V, agg: &mut Self::Agg| { *agg += value.clone(); },
                |new: &Self::Agg, agg: &mut Self::Agg| { *agg += new.clone(); },
            )
        }
    }

    fn aggregate_vec(&self, keys: &[K], values: &[V]) {
        self.base_agg.aggregate_vec(keys, values);
    }

    fn finalize_thread(&self) {
        self.base_agg.finalize_thread();
    }

    fn into_finalizer(self) -> Box<dyn PartitionedAggregatorFinalizer<K, V, Agg=Self::Agg>> {
        Box::new(SumAggFinalizer::<K, V>::new(self.base_agg.into_finalizer()))
    }

}

struct SumAggFinalizer<K: Clone + Default, V> {
    base_agg_finalizer: Box<dyn PartitionedAggregatorFinalizer<K, V, Agg=V>>,
    pd: PhantomData<V>,
}

impl<K: Clone + Default, V> SumAggFinalizer<K, V> {
    fn new(base_agg_finalizer: Box<dyn PartitionedAggregatorFinalizer<K, V, Agg=V>>) -> Self {
        Self {
            base_agg_finalizer,
            pd: Default::default(),
        }
    }
}


impl<
    K: Zeroable + Send + Sync + Hash + Eq + Clone + Default,
    V: Zeroable + Send + Sync,
> PartitionedAggregatorFinalizer<K, V> for SumAggFinalizer<K, V> {
    type Agg = V;

    fn finalize_thread(&self) {
        self.base_agg_finalizer.finalize_thread();
    }

    fn into_vec(self: Box<Self>) -> (Vec<K>, Vec<Self::Agg>) {
        self.base_agg_finalizer.into_vec()
    }
}
