#![feature(portable_simd)]

mod fuzzy_counter;
mod lock_vec;
mod sync_unsafe_cell;
mod unsafe_vec;

pub use fuzzy_counter::{FuzzyCounter, FuzzyCounterFinalizer, LocalCounter};
pub use lock_vec::LockVec;
pub use sync_unsafe_cell::SyncUnsafeCell;
pub use unsafe_vec::UnsafeVec;