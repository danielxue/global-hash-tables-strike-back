use std::cell::UnsafeCell;

#[derive(Default)]
#[repr(transparent)]
pub struct SyncUnsafeCell<T: ?Sized> {
    pub value: UnsafeCell<T>,
}

impl<T> SyncUnsafeCell<T> {
    pub fn new(value: T) -> Self {
        Self { value: UnsafeCell::new(value) }
    }

    pub fn get(&self) -> *mut T { self.value.get() }
}

unsafe impl<T: ?Sized> Send for SyncUnsafeCell<T> {}

unsafe impl<T: ?Sized> Sync for SyncUnsafeCell<T> {}
