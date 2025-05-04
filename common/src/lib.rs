#![feature(portable_simd)]

mod lock_vec;
mod sync_unsafe_cell;
mod fuzzy_counter;

pub use fuzzy_counter::{FuzzyCounter, FuzzyCounterFinalizer};
pub use lock_vec::LockVec;
pub use sync_unsafe_cell::SyncUnsafeCell;
