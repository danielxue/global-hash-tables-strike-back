use std::mem::{MaybeUninit, transmute};
use bytemuck::zeroed_vec;

pub struct UnsafeVec<T> {
    data: Vec<MaybeUninit<T>>,
}

impl<T> UnsafeVec<T> {
    pub fn new(len: usize) -> UnsafeVec<T> {
        UnsafeVec { data: zeroed_vec(len) }
    }

    // Calling function MUST ensure only one thread is modifying the cell at idx.
    pub fn write(&self, idx: usize, el: T) {
        if idx > self.data.len() {
            panic!("Index out of bounds");
        }
        unsafe { (self.data.as_ptr() as *mut MaybeUninit<T>).add(idx).as_mut().unwrap().write(el) };
    }

    pub fn extend(&mut self, len: usize) {
        self.data.extend(zeroed_vec(len));
    }

    pub fn truncate(&mut self, len: usize) {
        self.data.truncate(len);
    }

    pub fn as_mut_slice(&mut self) -> &mut [MaybeUninit<T>] {
        &mut self.data[..]
    }

    // Calling function MUST ensure all elements are initialized.
    pub fn into_inner(self) -> Vec<T> {
        unsafe { transmute(self.data) }
    }
}

unsafe impl<T> Send for UnsafeVec<T> {}

unsafe impl<T> Sync for UnsafeVec<T> {}
