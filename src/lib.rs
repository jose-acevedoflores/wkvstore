//! Simple multi-threaded Key Value store with optional expiration on keys/value pairs
//! ### Assumptions
//! * Heavy read load (prioritize reads over mutations i.e. writes/deletes)
//! * Internal usage (clients generating keys are known/internal to the overall system)
//!     * Hash keys will not be intentionally malicious (possible to ignore collision attacks i.e. could use a less robust but faster hasher if needed)
//! * Keys are valid UTF-8 strings (future improvement: make generic)
//! * Expiration implies auto removal from map
//!
//!
//! ## Design Specs
//!
//! Current [`KVStore`] implementation is based on the default rust [`HashMap`] using
//! [hashbrown](https://github.com/rust-lang/hashbrown#hashbrown).
//!
//! To account for concurrent access the default implementation for [`KVStore`]
//! wraps the HashMap in an [`Arc`] for shared access and in a [`RwLock`] for shared mutability.
//!
//! The RwLock has special implications:
//! * Every read will need to go through a read lock. This will result in some time trying to
//! acquire the lock.\
//! Note that one possibility to mitigate read access is to leverage the
//! [evmap](https://docs.rs/evmap/10.0.2/evmap/) crate.
//!
//!
//! **Key/value pair expiration**
//!
//! The mechanism used to expire keys relies on the
//! [multi-threaded](https://docs.rs/tokio/1.6.0/tokio/runtime/index.html#multi-thread-scheduler)
//! `tokio_rs` async [Runtime](https://docs.rs/tokio/1.6.0/tokio/runtime/index.html).\
//! This approach was chosen to leverage the async capabilities of rust and reduce
//! the number of resources required to enforce expiration on a large number of entries.
//!
//! #### Memory Usage
//!
//! The default HashMap adds an overhead of 1 byte for each entry.
//!
//! Memory usage will have two components:
//! * **Data map:** should be roughly the size of the `key/value` pair plus the size of an [`Arc`]
//! (the one encapsulating the value) multiplied by the number of entries.
//! * **Expiration map:** supporting map that holds the expiration task for a given key.\
//!   This decision of holding the expiration data on a map was based on:
//!     * Assuming there is a good portion of keys that have no expiration.
//!       This reduces the overall size since the alternative would be to store an extra field with each
//!       value in the `Data map` even if it has no expiration.\
//!       Note that this means that if many keys also include expiration it could hurt performance
//!       since the rehashing of both maps could coincide. For that scenario storing the
//!       expiration alongside the values would be better.
//!
//! For the clients memory usage, they are thin wrappers around the backing [`KVStore`] so
//! their size is negligible
//!
//! #### Performance
//!
//! Benchmarks can be run by executing:
//! ```shell
//! $ cargo run -p max_load
//! ```
//! The `max_load` test writes 10 million entries to the store and then.
//!
//! **NOTE:** I believe this test doesn't trigger a reshash since at most it changes the map size
//! by 400k entries. This means we are not seeing the effect on the reads when one write triggers
//! a size increase on the map.
//!
//! Sample run from an Intel MacbookPro with specs:
//!  * 4 core i7 processor
//!  * 8GB of RAM
//!
//! | 95th Percentile| 99th Percentile |
//! |----------------|-----------------|
//! | 0.139935ms     |  0.257684ms     |
//!
//! See included spreadsheet at
//! [`benchmarks/r4/perf_run.xlsx`](https://drive.google.com/file/d/1Tc6zwWmA57Dy9jjSyp_W7MGLDZ4UqR65/view?usp=sharing).

#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::task::JoinHandle;

mod scheduler;

/// Struct that holds the inner workings of a KVStore
struct BackingStruct<V: Send + Sync + 'static> {
    /// Data Map. Notice that V is wrapped in an Arc to account for retrievals since
    /// the V could be deleted by another thread.
    map: HashMap<String, Arc<V>>,
    /// Expiration Map:
    ///  NOTE: See `Memory Usage` section of crate documentation.
    expiry_map: HashMap<String, (Duration, JoinHandle<()>)>,
    /// Scheduler that executes the removal of entries from the `Data map`.
    expire_scheduler: scheduler::ScheduledExecutor,
}

impl<V: Send + Sync + 'static> BackingStruct<V> {
    fn new() -> Self {
        BackingStruct {
            map: HashMap::new(),
            expiry_map: HashMap::new(),
            expire_scheduler: scheduler::ScheduledExecutor::new(),
        }
    }
}

/// Backing store for `n` number of clients.
#[derive(Clone)]
pub struct KVStore<V: Send + Sync + 'static> {
    backing_struct: Arc<RwLock<BackingStruct<V>>>,
}

impl<V: Send + Sync + 'static> KVStore<V> {
    /// Create a new store.
    pub fn new() -> Self {
        KVStore {
            backing_struct: Arc::new(RwLock::new(BackingStruct::new())),
        }
    }

    /// Shallow copy an existing store
    fn copy(orig: &KVStore<V>) -> Self {
        KVStore {
            backing_struct: Arc::clone(&orig.backing_struct),
        }
    }

    /// Returns a [`KVClient`] that connects to this store
    pub fn get_client(&self) -> KVClient<V> {
        KVClient::new(self)
    }
}

impl<V: Send + Sync + 'static> StoreAccessor<V> for KVStore<V> {
    fn write(&self, key: &str, val: V, exp: Option<Duration>) {
        let mut write_g = self.backing_struct.write().unwrap();

        write_g.map.insert(key.to_string(), Arc::new(val));

        if exp.is_some() {
            let k = key.to_string();
            let timeout = exp.unwrap();

            //Use a weak ptr because ff the store is dropped we don't want to have the expire scheduler holding a ref
            let weak_handle = Arc::downgrade(&self.backing_struct);
            let join_h = write_g.expire_scheduler.submit_task(
                move || {
                    if let Some(r) = weak_handle.upgrade() {
                        let mut l = r.write().unwrap();
                        // remove expired key
                        (*l).map.remove(&k);
                        // Remove expired task
                        (*l).expiry_map.remove(&k);
                    }
                },
                timeout.clone(),
            );
            let expiry_task = write_g
                .expiry_map
                .insert(key.to_string(), (timeout, join_h));
            if let Some((_, task)) = expiry_task {
                task.abort();
            }
        }
    }

    fn delete(&self, key: &str) -> Option<Arc<V>> {
        let mut write_g = self.backing_struct.write().unwrap();

        let expiry_task = write_g.expiry_map.remove(key);
        if let Some((_, task)) = expiry_task {
            task.abort();
        };
        write_g.map.remove(key)
    }

    fn read(&self, key: &str) -> Option<Arc<V>> {
        self.backing_struct
            .read()
            .unwrap()
            .map
            .get(key)
            .map(|e| Arc::clone(e))
    }

    fn read_cb(&self, key: &str, callback: impl FnOnce(Option<&V>)) {
        let read_g = self.backing_struct.read().unwrap();

        let val = read_g.map.get(key).map(|a| a.as_ref());
        callback(val);
    }

    fn size(&self) -> usize {
        let read_g = self.backing_struct.read().unwrap();
        read_g.map.len()
    }
}

trait StoreAccessor<V: Send + Sync + 'static> {
    fn write(&self, key: &str, val: V, exp: Option<std::time::Duration>);

    fn delete(&self, key: &str) -> Option<Arc<V>>;

    fn read(&self, key: &str) -> Option<Arc<V>>;

    fn read_cb(&self, key: &str, callback: impl FnOnce(Option<&V>));

    fn size(&self) -> usize;
}

/// Client that interfaces with a given `StoreAccessor<V>`
#[derive(Clone)]
pub struct KVClient<V: Send + Sync + 'static> {
    // Ref to backing struct.
    // Having the whole struct frees us from tying a specific backing struct to the client.
    // Eventually make this a generic that takes a store implementing StoreAccessor
    store: KVStore<V>,
}

impl<V: Send + Sync + 'static> KVClient<V> {
    ///
    /// # Arguments
    /// * `store` the object this client will connect to.
    pub fn new(store: &KVStore<V>) -> Self {
        KVClient {
            store: KVStore::copy(store),
        }
    }

    /// Insert a key/value pair to the store for this client.
    pub fn insert(&self, key: &str, val: V) {
        self.store.write(key, val, None);
    }

    /// Insert a key/value pair to the store for this client.\
    /// Note that, if a key is inserted with a new expiration value, it will be updated and
    /// the previous expiration will be ignored.
    /// # Arguments
    /// * `dur` expiration time for this key/value pair. Starts counting from moment of insertion
    pub fn insert_with_expiration(&self, key: &str, val: V, dur: std::time::Duration) {
        self.store.write(key, val, Some(dur));
    }

    /// Retrieve the value corresponding to the given `key` from the store for this client.\
    /// Returns an [`Arc`] since the value could be removed by another thread.
    pub fn retrieve(&self, key: &str) -> Option<Arc<V>> {
        self.store.read(key)
    }

    /// Removes the key from the store for this client
    pub fn delete(&self, key: &str) -> Option<Arc<V>> {
        self.store.delete(key)
    }

    ///
    /// NOTE: if the `callback` takes a long time it can hurt write performance.
    ///
    /// # Arguments
    /// * `callback` will get the value at the given `key`. Note that while this `callback`
    ///   is being executed no inserts can happen.
    pub fn retrieve_with_cb(&self, key: &str, callback: impl FnOnce(Option<&V>)) {
        self.store.read_cb(key, callback);
    }

    /// Returns the size of the store for this client. Note that this is more of a snapshot
    /// since the store can be updated by another thread.
    pub fn size(&self) -> usize {
        self.store.size()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::ops::Add;
    use std::thread;

    #[test]
    fn basic_checks() {
        let store = KVStore::<Vec<u8>>::new();

        let client = store.get_client();
        let test_key = "simple_key";

        client.insert(test_key, vec![34, 89]);

        let res = client.retrieve(test_key).expect("To be present");

        assert_eq!(*res, vec![34, 89]);
        client.retrieve_with_cb(test_key, |val| {
            assert_eq!(*val.unwrap(), vec![34, 89]);
        });

        let deleted = client.delete(test_key).expect("Test key to be present");
        assert_eq!(*deleted, vec![34, 89]);

        let deleted = client.delete("non-existent-key");
        assert!(deleted.is_none());
    }

    #[test]
    fn basic_expiration_check() {
        let store = KVStore::<Vec<u8>>::new();

        let client = store.get_client();
        let test_key = "simple_key";

        let two_seconds_duration = Duration::from_secs(2);

        client.insert_with_expiration(test_key, vec![34, 89], two_seconds_duration.clone());

        let res = client.retrieve(test_key).expect("To be present");

        assert_eq!(*res, vec![34, 89]);

        thread::sleep(Duration::from_millis(250).add(two_seconds_duration));
        assert_eq!(client.size(), 0);

        // test key should not be present because it should've expired
        let deleted = client.delete(test_key);
        assert!(deleted.is_none());
    }

    #[test]
    fn basic_thread_checks() {
        let store = KVStore::<Vec<u8>>::new();
        let client1 = store.get_client();
        let client2 = client1.clone();
        let client3 = client1.clone();

        let test_key1 = "simple_key_from_thread_1";
        let test_key2 = "simple_key_from_thread_2";

        let _handle1 = thread::spawn(move || {
            let client = client1.clone();
            thread::sleep(Duration::from_millis(750));
            client.insert(test_key1, vec![34, 89]);
        });

        let _handle2 = thread::spawn(move || {
            thread::sleep(Duration::from_millis(500));
            client2.insert(test_key2, vec![11, 21]);
        });

        let handle3 = thread::spawn(move || loop {
            let key1 = client3.retrieve(test_key1);
            let key2 = client3.retrieve(test_key2);
            if key1.is_some() && key2.is_some() {
                break;
            }
        });

        handle3.join().unwrap();
        let client = store.get_client();

        let deleted = client.delete(test_key1).expect("To have test_key 1");
        assert_eq!(*deleted, vec![34, 89]);
        let deleted = client.delete(test_key2).expect("To have test_key 2");
        assert_eq!(*deleted, vec![11, 21]);
    }
}
