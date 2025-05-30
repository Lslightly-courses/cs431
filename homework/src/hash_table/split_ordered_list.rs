//! Split-ordered linked list.

use core::mem::{self, MaybeUninit};
use core::sync::atomic::AtomicUsize;
use core::sync::atomic::Ordering::*;

use crossbeam_epoch::{Guard, Owned};
use cs431::lockfree::list::{Cursor, List, Node};

use super::growable_array::GrowableArray;
use crate::ConcurrentMap;

/// Lock-free map from `usize` in range \[0, 2^63-1\] to `V`.
///
/// NOTE: We don't care about hashing in this homework for simplicity.
#[derive(Debug)]
pub struct SplitOrderedList<V> {
    /// Lock-free list sorted by recursive-split order.
    ///
    /// Use `MaybeUninit::uninit()` when creating sentinel nodes.
    list: List<usize, MaybeUninit<V>>,
    /// Array of pointers to the buckets.
    buckets: GrowableArray<Node<usize, MaybeUninit<V>>>,
    /// Number of buckets.
    size: AtomicUsize,
    /// Number of items.
    count: AtomicUsize,
}

impl<V> Default for SplitOrderedList<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> SplitOrderedList<V> {
    /// `size` is doubled when `count > size * LOAD_FACTOR`.
    const LOAD_FACTOR: usize = 2;

    /// Creates a new split ordered list.
    pub fn new() -> Self {
        Self {
            list: List::new(),
            buckets: GrowableArray::new(),
            size: AtomicUsize::new(2),
            count: AtomicUsize::new(0),
        }
    }

    /// Creates a cursor and moves it to the bucket for the given index.  If the bucket doesn't
    /// exist, recursively initializes the buckets.
    fn lookup_bucket<'s>(
        &'s self,
        index: usize,
        guard: &'s Guard,
    ) -> Cursor<'s, usize, MaybeUninit<V>> {
        todo!()
    }

    /// Moves the bucket cursor returned from `lookup_bucket` to the position of the given key.
    /// Returns `(size, found, cursor)`
    fn find<'s>(
        &'s self,
        key: &usize,
        guard: &'s Guard,
    ) -> (usize, bool, Cursor<'s, usize, MaybeUninit<V>>) {
        todo!()
    }

    fn assert_valid_key(key: usize) {
        assert!(key.leading_zeros() != 0);
    }
}

impl<V> ConcurrentMap<usize, V> for SplitOrderedList<V> {
    fn lookup<'a>(&'a self, key: &usize, guard: &'a Guard) -> Option<&'a V> {
        Self::assert_valid_key(*key);

        todo!()
    }

    fn insert(&self, key: usize, value: V, guard: &Guard) -> Result<(), V> {
        Self::assert_valid_key(key);

        todo!()
    }

    fn delete<'a>(&'a self, key: &usize, guard: &'a Guard) -> Result<&'a V, ()> {
        Self::assert_valid_key(*key);

        todo!()
    }
}
