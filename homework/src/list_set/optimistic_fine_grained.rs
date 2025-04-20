use std::cmp::Ordering::*;
use std::fmt::write;
use std::mem::{self, ManuallyDrop, replace, take};
use std::sync::atomic::Ordering::*;

use crossbeam_epoch::{Atomic, Guard, Owned, Shared, pin};
use cs431::lock::seqlock::{ReadGuard, SeqLock};

use crate::ConcurrentSet;

#[derive(Debug)]
struct Node<T> {
    data: T,
    next: SeqLock<Atomic<Node<T>>>,
}

/// Concurrent sorted singly linked list using fine-grained optimistic locking.
#[derive(Debug)]
pub struct OptimisticFineGrainedListSet<T> {
    head: SeqLock<Atomic<Node<T>>>,
}

unsafe impl<T: Send> Send for OptimisticFineGrainedListSet<T> {}
unsafe impl<T: Sync> Sync for OptimisticFineGrainedListSet<T> {}

#[derive(Debug)]
struct Cursor<'g, T> {
    // Reference to the `next` field of previous node which points to the current node.
    prev: ReadGuard<'g, Atomic<Node<T>>>,
    curr: Shared<'g, Node<T>>,
}

impl<T> Node<T> {
    fn new(data: T, next: Shared<'_, Self>) -> Owned<Self> {
        Owned::new(Self {
            data,
            next: SeqLock::new(next.into()),
        })
    }
}

impl<'g, T: Ord> Cursor<'g, T> {
    /// Moves the cursor to the position of key in the sorted list.
    /// Returns whether the value was found.
    ///
    /// Return `Err(())` if the cursor cannot move.
    fn find(&mut self, key: &T, guard: &'g Guard) -> Result<bool, ()> {
        while self.prev.validate() {
            if let Some(curr_node) = unsafe { self.curr.as_ref() } {
                if curr_node.data == *key {
                    return Ok(true);
                }
                let prev = replace(&mut self.prev, unsafe { curr_node.next.read_lock() });
                prev.finish();
                self.curr = self.prev.load(SeqCst, guard);
            } else {
                return Ok(false);
            }
        }
        Err(())
    }
}

impl<T> OptimisticFineGrainedListSet<T> {
    /// Creates a new list.
    pub fn new() -> Self {
        Self {
            head: SeqLock::new(Atomic::null()),
        }
    }

    fn head<'g>(&'g self, guard: &'g Guard) -> Cursor<'g, T> {
        let prev = unsafe { self.head.read_lock() };
        let curr = prev.load(Acquire, guard);
        Cursor { prev, curr }
    }
}

impl<T: Ord> OptimisticFineGrainedListSet<T> {
    fn find<'g>(&'g self, key: &T, guard: &'g Guard) -> Result<(bool, Cursor<'g, T>), ()> {
        let mut cur = self.head(guard);
        if let Ok(found) = cur.find(key, guard) {
            Ok((found, cur))
        } else {
            cur.prev.finish();
            Err(())
        }
    }
}

impl<T: Ord> ConcurrentSet<T> for OptimisticFineGrainedListSet<T> {
    fn contains(&self, key: &T) -> bool {
        let guard = pin();
        match self.find(key, &guard) {
            Ok((found, cursor)) => {
                cursor.prev.finish();
                found
            }
            Err(_) => false,
        }
    }

    fn insert(&self, key: T) -> bool {
        let guard = pin();
        let mut cur = self.head(&guard);

        'outer: loop {
            if !cur.prev.validate() {
                continue;
            }
            if let Some(curr_node) = unsafe { cur.curr.as_ref() } {
                match curr_node.data.cmp(&key) {
                    Less => {
                        cur.prev.finish();
                        cur.prev = unsafe { curr_node.next.read_lock() };
                        cur.curr = cur.prev.load(SeqCst, &guard);
                    }
                    Equal => {
                        cur.prev.finish(); // FUCK finish
                        return false;
                    }
                    Greater => break 'outer,
                }
            } else {
                break 'outer;
            }
        }

        // Insert before the current node
        let new_node = Node::new(key, cur.curr);
        let write_guard = cur.prev.upgrade().unwrap();
        write_guard.store(new_node, SeqCst);
        true
    }

    fn remove(&self, key: &T) -> bool {
        /*
           write lock the previous node
        */
        let guard = pin();

        // deal with the first node
        'outer: loop {
            let mut cursor = self.head(&guard);
            loop {
                if !cursor.prev.validate() {
                    cursor.prev.finish();
                    continue 'outer;
                }
                let cursor_ref = unsafe { cursor.curr.as_ref() };
                if cursor_ref.is_none() {
                    cursor.prev.finish();
                    return false;
                }
                let curr_node = cursor_ref.unwrap();
                match curr_node.data.cmp(key) {
                    Less => {
                        cursor.prev.finish();
                        cursor.prev = unsafe { curr_node.next.read_lock() };
                        cursor.curr = cursor.prev.load(SeqCst, &guard);
                    }
                    Equal => {
                        if !cursor.prev.validate() {
                            cursor.prev.finish();
                            continue 'outer; // retry because the previous node is invalid. It's destroyed by write lock.
                        }
                        let write_guard = cursor.prev.upgrade().unwrap();
                        let write_guard_next = curr_node.next.write_lock(); // !!! to invalidate iterator.
                        write_guard.store(write_guard_next.load(SeqCst, &guard), SeqCst);
                        unsafe {
                            guard.defer_destroy(cursor.curr);
                        }
                        return true;
                    }
                    Greater => {
                        cursor.prev.finish();
                        return false;
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Iter<'g, T> {
    // Can be dropped without validation, because the only way to use cursor.curr is next().
    cursor: ManuallyDrop<Cursor<'g, T>>,
    guard: &'g Guard,
}

impl<T> OptimisticFineGrainedListSet<T> {
    /// An iterator visiting all elements. `next()` returns `Some(Err(()))` when validation fails.
    /// In that case, the user must restart the iteration.
    pub fn iter<'g>(&'g self, guard: &'g Guard) -> Iter<'g, T> {
        Iter {
            cursor: ManuallyDrop::new(self.head(guard)),
            guard,
        }
    }
}

impl<'g, T> Iterator for Iter<'g, T> {
    type Item = Result<&'g T, ()>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.cursor.prev.validate() {
            return Some(Err(()));
        }
        let cursor_ref = unsafe { self.cursor.curr.as_ref() };
        cursor_ref?;
        let curr_node = cursor_ref.unwrap();
        let cur = unsafe { ManuallyDrop::take(&mut self.cursor) };
        let next_prev_guard = unsafe { curr_node.next.read_lock() };
        if !next_prev_guard.validate() {
            next_prev_guard.finish();
            cur.prev.finish();
            return Some(Err(()));
        }
        let next_node = next_prev_guard.load(SeqCst, self.guard);
        self.cursor = ManuallyDrop::new(Cursor {
            prev: next_prev_guard,
            curr: next_node,
        });
        cur.prev.finish();
        Some(Ok(&curr_node.data))
    }
}

impl<T> Drop for OptimisticFineGrainedListSet<T> {
    fn drop(&mut self) {
        let guard = pin();
        let read_guard = unsafe { self.head.read_lock() };
        let mut cur_node = read_guard.load(SeqCst, &guard);
        read_guard.finish();
        while !cur_node.is_null() {
            let node = unsafe { cur_node.into_owned() };
            let read_guard = unsafe { node.next.read_lock() };
            cur_node = read_guard.load(SeqCst, &guard);
            read_guard.finish();
            drop(node);
        }
    }
}

impl<T> Default for OptimisticFineGrainedListSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
