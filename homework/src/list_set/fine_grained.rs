use std::cmp::Ordering::*;
use std::sync::{Mutex, MutexGuard};
use std::{mem, ptr};

use crate::ConcurrentSet;

#[derive(Debug)]
struct Node<T> {
    data: T,
    next: Mutex<*mut Node<T>>,
}

/// Concurrent sorted singly linked list using fine-grained lock-coupling.
#[derive(Debug)]
pub struct FineGrainedListSet<T> {
    head: Mutex<*mut Node<T>>,
}

unsafe impl<T: Send> Send for FineGrainedListSet<T> {}
unsafe impl<T: Send> Sync for FineGrainedListSet<T> {}

/// Reference to the `next` field of previous node which points to the current node.
///
/// For example, given the following linked list:
///
/// ```text
/// head -> 1 -> 2 -> 3 -> null
/// ```
///
/// If `cursor` is currently at node 2, then `cursor.0` should be the `MutexGuard` obtained from the
/// `next` of node 1. In particular, `cursor.0.as_ref().unwrap()` creates a shared reference to node
/// 2.
struct Cursor<'l, T>(MutexGuard<'l, *mut Node<T>>);

impl<T> Node<T> {
    fn new(data: T, next: *mut Self) -> *mut Self {
        Box::into_raw(Box::new(Self {
            data,
            next: Mutex::new(next),
        }))
    }
}

impl<T: Ord> Cursor<'_, T> {
    /// Moves the cursor to the position of key in the sorted list.
    /// Returns whether the value was found.
    fn find(&mut self, key: &T) -> bool {
        if let Some(node) = unsafe { self.0.as_ref() } {
            if node.data == *key {
                return true;
            }
            let next_guard = node.next.lock().unwrap();
            self.0 = next_guard;
            return self.find(key);
        }
        false
    }
}

impl<T> FineGrainedListSet<T> {
    /// Creates a new list.
    pub fn new() -> Self {
        Self {
            head: Mutex::new(ptr::null_mut()),
        }
    }
}

impl<T: Ord> FineGrainedListSet<T> {
    fn find(&self, key: &T) -> (bool, Cursor<'_, T>) {
        let mut cur = Cursor(self.head.lock().unwrap());
        (cur.find(key), cur)
    }
}

impl<T: Ord> ConcurrentSet<T> for FineGrainedListSet<T> {
    fn contains(&self, key: &T) -> bool {
        self.find(key).0
    }

    fn insert(&self, key: T) -> bool {
        let mut cursor_guard = self.head.lock().unwrap();
        while let Some(node) = unsafe { cursor_guard.as_ref() } {
            if node.data == key {
                return false; // already in the list
            }
            if node.data > key {
                // Insert before the current node
                let new_node = Node::new(key, node as *const _ as *mut _);
                *cursor_guard = new_node;
                return true;
            }
            cursor_guard = node.next.lock().unwrap();
        }
        // insert to the end of the list
        let new_node = Node::new(key, ptr::null_mut());
        *cursor_guard = new_node;
        true
    }

    fn remove(&self, key: &T) -> bool {
        let (found, mut cursor) = self.find(key);
        if !found {
            return false;
        }
        let node = unsafe { Box::from_raw(*cursor.0) }; // release cur node
        let next_node = *node.next.lock().unwrap();
        *cursor.0 = next_node;
        true
    }
}

#[derive(Debug)]
pub struct Iter<'l, T> {
    cursor: MutexGuard<'l, *mut Node<T>>,
}

impl<T> FineGrainedListSet<T> {
    /// An iterator visiting all elements.
    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            cursor: self.head.lock().unwrap(),
        }
    }
}

impl<'l, T> Iterator for Iter<'l, T> {
    type Item = &'l T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(node) = unsafe { self.cursor.as_ref() } {
            let next_guard = node.next.lock().unwrap();
            self.cursor = next_guard;
            return Some(&node.data);
        }
        None
    }
}

impl<T> Drop for FineGrainedListSet<T> {
    fn drop(&mut self) {
        // drop all node because all nodes are leaked by Box::into_raw
        let mut cur_node = *self.head.lock().unwrap();
        while !cur_node.is_null() {
            let node = unsafe { Box::from_raw(cur_node) };
            cur_node = *node.next.lock().unwrap();
            // drop the node
            mem::drop(node);
        }
    }
}

impl<T> Default for FineGrainedListSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
