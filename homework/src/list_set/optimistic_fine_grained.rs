use std::cmp::Ordering::*;
use std::mem::{self, ManuallyDrop};
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
                self.prev = unsafe { curr_node.next.read_lock() };
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
        cur.find(key, guard).map(|found| {
            (true, cur)
        })
    }
}

impl<T: Ord> ConcurrentSet<T> for OptimisticFineGrainedListSet<T> {
    fn contains(&self, key: &T) -> bool {
        let guard = pin();
        match self.find(key, &guard) {
            Ok((found, _)) => found,
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
                        cur.prev = unsafe { curr_node.next.read_lock() };
                        cur.curr = cur.prev.load(SeqCst, &guard);
                    },
                    Equal => return false,
                    Greater => break 'outer,
                }
            } else {
                break 'outer;
            }
        }

        // Insert before the current node
        let new_node = Node::new(key, cur.curr);
        cur.prev.store(new_node, SeqCst);
        return true;
    }

    fn remove(&self, key: &T) -> bool {
        let guard = pin();
        loop {
            if let Ok((found, mut cursor)) = self.find(key, &guard) {
                if !found {
                    return false;
                }
                if let Some(curr_node) = unsafe { cursor.curr.as_ref() } {
                    if cursor.prev.validate() {
                        let next_guard = unsafe { curr_node.next.read_lock() };
                        while let Err(()) = cursor.prev.clone().upgrade() {}
                        // todo upgrade to write lock
                        todo!();
                    }
                }
            } else {
                // find failed, try again
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
        todo!()
    }
}

impl<T> Drop for OptimisticFineGrainedListSet<T> {
    fn drop(&mut self) {
        todo!()
    }
}

impl<T> Default for OptimisticFineGrainedListSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
