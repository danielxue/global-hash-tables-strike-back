use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use bytemuck::{zeroed_vec, Zeroable};
use itertools::Itertools;

struct Empty {}
pub struct LockVec<T> {
    locks: Vec<RwLock<Empty>>,
    data: Vec<UnsafeCell<T>>,
}

unsafe impl<T> Sync for LockVec<T> {}

pub struct LockVecReadGuard<'a, T> {
    _guard: RwLockReadGuard<'a, Empty>,
    data: &'a UnsafeCell<T>,
}

impl<'a, T> Deref for LockVecReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.data.get().as_ref::<'a>().unwrap() }
    }
}

pub struct LockVecWriteGuard<'a, T> {
    _guard: RwLockWriteGuard<'a, Empty>,
    data: &'a UnsafeCell<T>,
}

impl<'a, T> Deref for LockVecWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.data.get().as_ref::<'a>().unwrap() }
    }
}

impl<'a, T> DerefMut for LockVecWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.data.get().as_mut::<'a>().unwrap() }
    }
}

impl<T: Zeroable> LockVec<T> {
    pub fn new_zeroed(els: usize) -> LockVec<T> {
        let locks = (0..els).map(|_| RwLock::new(Empty {})).collect_vec();
        let data: Vec<T> = zeroed_vec(els);
        unsafe {
            LockVec {
                locks,
                data: std::mem::transmute(data),
            }
        }
    }
}

impl<T: Default> LockVec<T> {
    pub fn new(els: usize) -> LockVec<T> {
        let locks = (0..els).map(|_| RwLock::new(Empty {})).collect_vec();
        let data = (0..els)
            .map(|_| UnsafeCell::new(T::default()))
            .collect_vec();
        LockVec { locks, data }
    }
}

impl<T> LockVec<T> {
    pub fn read(&self, idx: usize) -> Result<LockVecReadGuard<'_, T>, ()> {
        let guard = self.locks[idx].read().map_err(|_g| ())?;
        let data = &self.data[idx];

        Ok(LockVecReadGuard {
            _guard: guard,
            data,
        })
    }

    pub fn write(&self, idx: usize) -> Result<LockVecWriteGuard<'_, T>, ()> {
        let guard = self.locks[idx].write().map_err(|_g| ())?;
        let data = &self.data[idx];

        Ok(LockVecWriteGuard {
            _guard: guard,
            data,
        })
    }

    pub fn get_two_mut(&mut self, idx1: usize, idx2: usize) -> (&mut T, &mut T) {
        assert!(idx1 < idx2);
        let mut iter = self.data.iter_mut();
        let r1 = iter.nth(idx1).unwrap().get_mut();
        let r2 = iter.nth((idx2 - idx1) - 1).unwrap().get_mut();
        (r1, r2)
    }

    pub fn iter(&self) -> impl Iterator<Item = Result<LockVecReadGuard<'_, T>, ()>> {
        (0..self.data.len()).map(|idx| self.read(idx))
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn into_inner(self) -> Vec<T> {
        unsafe { std::mem::transmute(self.data) }
    }
}

impl<T: Clone> Clone for LockVec<T> {
    fn clone(&self) -> Self {
        let mut locks = Vec::with_capacity(self.locks.len());
        let mut data = Vec::with_capacity(self.data.len());
        for idx in 0..self.data.len() {
            locks.push(RwLock::new(Empty {}));

            let item = self.read(idx).unwrap();
            data.push(UnsafeCell::new(item.clone()));
        }

        Self { locks, data }
    }
}
