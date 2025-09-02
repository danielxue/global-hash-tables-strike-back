#![allow(dead_code)] // For some weird linting with Zeroable derive.

use std::cell::UnsafeCell;
use bytemuck::Zeroable;

#[derive(Default, Zeroable)]
#[repr(transparent)]
pub struct SyncUnsafeCell<T: ?Sized> {
    value: UnsafeCell<T>,
}

impl<T> SyncUnsafeCell<T> {
    pub fn new(value: T) -> Self {
        Self { value: UnsafeCell::new(value) }
    }

    pub fn get(&self) -> *mut T { self.value.get() }

    pub fn into_inner(self) -> T { self.value.into_inner() }
}

unsafe impl<T: ?Sized> Send for SyncUnsafeCell<T> {}

unsafe impl<T: ?Sized> Sync for SyncUnsafeCell<T> {}
