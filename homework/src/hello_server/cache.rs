//! Thread-safe key/value cache.

use std::char::REPLACEMENT_CHARACTER;
use std::collections::hash_map::{Entry, HashMap};
use std::hash::Hash;
use std::sync::{Arc, Mutex, RwLock};

/// Cache that remembers the result for each key.
#[derive(Debug)]
pub struct Cache<K, V> {
    // todo! This is an example cache type. Build your own cache type that satisfies the
    // specification for `get_or_insert_with`.
    /// `None` mean no value yet.
    /// Getting or updating `HashMap` value should always use read lock of hashmap.
    /// Only inserting value into HashMap should use write lock of hashmap.
    inner: RwLock<HashMap<K, Arc<RwLock<Option<V>>>>>,
}

impl<K, V> Default for Cache<K, V> {
    fn default() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }
}

impl<K: Eq + Hash + Clone, V: Clone> Cache<K, V> {
    /// Retrieve the value or insert a new one created by `f`.
    ///
    /// An invocation to this function should not block another invocation with a different key. For
    /// example, if a thread calls `get_or_insert_with(key1, f1)` and another thread calls
    /// `get_or_insert_with(key2, f2)` (`key1≠key2`, `key1,key2∉cache`) concurrently, `f1` and `f2`
    /// should run concurrently.
    ///
    /// On the other hand, since `f` may consume a lot of resource (= money), it's undesirable to
    /// duplicate the work. That is, `f` should be run only once for each key. Specifically, even
    /// for concurrent invocations of `get_or_insert_with(key, f)`, `f` is called only once per key.
    ///
    /// Hint: the [`Entry`] API may be useful in implementing this function.
    ///
    /// [`Entry`]: https://doc.rust-lang.org/stable/std/collections/hash_map/struct.HashMap.html#method.entry
    pub fn get_or_insert_with<F: FnOnce(K) -> V>(&self, key: K, f: F) -> V {
        // read if there is an entry
        let value_status = {
            let r_cache = self.inner.read().unwrap();
            r_cache.get(&key).cloned() // inevitable clone
            // release cache read lock here
        };
        if let Some(value_lock) = value_status {
            let r_value = value_lock.read().unwrap();
            return r_value.as_ref().unwrap().clone();
        }

        {
            // create a value lock if there is not an entry with None content
            let value_lock = Arc::new(RwLock::new(None));
            let mut value = value_lock.write().unwrap();
            {
                // insert None value_lock
                let mut w_cache = self.inner.write().unwrap();
                match w_cache.entry(key.clone()) {
                    Entry::Occupied(entry) => {
                        // some other threads have already insert the value
                        let value_lock = entry.get();
                        return value_lock.read().unwrap().as_ref().unwrap().clone();
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(value_lock.clone());
                    }
                }
                // release w_cache write lock here
            }
            let new_value = f(key);
            *value = Some(new_value.clone());
            return new_value;
        }
    }
}
