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
    ) -> (Cursor<'s, usize, MaybeUninit<V>>, bool) {
        let index_ptr = self.buckets.get(index, guard);
        let bucket = index_ptr.load(SeqCst, guard);
        if bucket.is_null() {
            let new_v = MaybeUninit::uninit();
            self.list.harris_insert(index, new_v, guard);
        }
        let mut cursor = self.list.head(guard);
        match cursor.find_harris(&index, guard) {
            Ok(true) => (cursor, true),
            Ok(false) => (self.list.head(guard), false),
            Err(_) => {
                // If the cursor is not valid, we need to reinitialize it.
                (self.list.head(guard), false)
            }
        }
    }

    /// Moves the bucket cursor returned from `lookup_bucket` to the position of the given key.
    /// Returns `(size, found, cursor)`
    fn find<'s>(
        &'s self,
        key: &usize,
        guard: &'s Guard,
    ) -> (usize, bool, Cursor<'s, usize, MaybeUninit<V>>) {
        let (cursor, found) = self.lookup_bucket(*key, guard);
        (self.size.load(SeqCst), found, cursor)
    }

    fn assert_valid_key(key: usize) {
        assert!(key.leading_zeros() != 0);
    }
}

impl<V> ConcurrentMap<usize, V> for SplitOrderedList<V> {
    fn lookup<'a>(&'a self, key: &usize, guard: &'a Guard) -> Option<&'a V> {
        Self::assert_valid_key(*key);

        match self.find(key, guard) {
            (_, true, cursor) => Some(unsafe { cursor.lookup().assume_init_ref() }),
            (_, false, _) => None,
        }
    }

    fn insert(&self, key: usize, value: V, guard: &Guard) -> Result<(), V> {
        Self::assert_valid_key(key);

        let (size, found, mut cursor) = self.find(&key, guard);
        if found {
            let old_value = unsafe { cursor.lookup().assume_init_read() };
            Err(old_value)
        } else {
            let new_value = MaybeUninit::new(value);
            if size * Self::LOAD_FACTOR <= self.count.load(SeqCst) {
                // Resize the buckets if necessary.
                self.lookup_bucket(size * 2, guard);
            }
            // Insert the new value into the list.
            self.list.harris_insert(key, new_value, guard);
            self.count.fetch_add(1, SeqCst);
            Ok(())
        }
    }

    fn delete<'a>(&'a self, key: &usize, guard: &'a Guard) -> Result<&'a V, ()> {
        Self::assert_valid_key(*key);

        let (size, found, mut cursor) = self.find(key, guard);
        if !found {
            return Err(());
        }
        // Remove the value from the list.
        if cursor.delete(guard).is_err() {
            // If the cursor is not valid, we need to reinitialize it.
            return Err(());
        }
        self.count.fetch_sub(1, SeqCst);
        Ok(unsafe { cursor.lookup().assume_init_ref() })
    }
}
