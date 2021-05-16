//!
//!

// #![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

mod scheduler;

pub struct ExpiryValue<V: Send + Sync + 'static> {
    pub val: V,
    pub expiration: Option<Duration>,
}

struct BackingStruct<V: Send + Sync + 'static> {
    map: HashMap<String, Arc<ExpiryValue<V>>>,
    expire_scheduler: scheduler::ScheduledExecutor,
}

impl<V: Send + Sync + 'static> BackingStruct<V> {
    fn new() -> Self {
        BackingStruct {
            map: HashMap::new(),
            expire_scheduler: scheduler::ScheduledExecutor::new(),
        }
    }
}

///
///
#[derive(Clone)]
pub struct KVStore<V: Send + Sync + 'static> {
    backing_struct: Arc<RwLock<BackingStruct<V>>>,
}

impl<V: Send + Sync + 'static> KVStore<V> {
    pub fn new() -> Self {
        KVStore {
            backing_struct: Arc::new(RwLock::new(BackingStruct::new())),
        }
    }

    fn copy(orig: &KVStore<V>) -> Self {
        KVStore {
            backing_struct: Arc::clone(&orig.backing_struct),
        }
    }

    pub fn get_client(&self) -> KVClient<V> {
        KVClient::new(self)
    }
}

impl<V: Send + Sync + 'static> StoreAccessor<V> for KVStore<V> {
    fn write(&self, key: &str, val: V, exp: Option<Duration>) {
        let mut write_g = self.backing_struct.write().unwrap();

        write_g.map.insert(
            key.to_string(),
            Arc::new(ExpiryValue {
                val,
                expiration: exp.clone(),
            }),
        );

        //TODO handle reinsertion to avoid registering multiple ?
        if exp.is_some() {
            let k = key.to_string();
            let w = Arc::downgrade(&self.backing_struct);
            write_g.expire_scheduler.submit_task(move || {
                if let Some(r) =  w.upgrade(){
                    let mut l = r.write().unwrap();
                    (*l).map.remove(&k);
                }
            },  exp.unwrap());
        }
    }

    fn delete(&self, key: &str) -> Option<Arc<ExpiryValue<V>>> {
        let mut write_g = self.backing_struct.write().unwrap();

        write_g.map.remove(key)
    }

    fn read(&self, key: &str) -> Option<Arc<ExpiryValue<V>>> {
        self.backing_struct
            .read()
            .unwrap()
            .map
            .get(key)
            .map(|e| Arc::clone(e))
    }

    ///NOTE: if this fn takes a long time it can hurt writes.
    fn read_cb(&self, key: &str, callback: impl FnOnce(Option<&V>)) {
        let read_g = self.backing_struct.read().unwrap();

        let val = read_g.map.get(key).map(|a| &a.as_ref().val);
        callback(val);
    }
}

trait StoreAccessor<V: Send + Sync + 'static> {
    fn write(&self, key: &str, val: V, exp: Option<std::time::Duration>);

    fn delete(&self, key: &str) -> Option<Arc<ExpiryValue<V>>>;

    fn read(&self, key: &str) -> Option<Arc<ExpiryValue<V>>>;

    fn read_cb(&self, key: &str, callback: impl FnOnce(Option<&V>));
}

#[derive(Clone)]
pub struct KVClient<V: Send + Sync + 'static> {
    // Ref to backing struct.
    // Having the whole struct frees us from tying the backing struct to the client
    store: KVStore<V>,
}

impl<V: Send + Sync + 'static> KVClient<V> {
    pub fn new(store: &KVStore<V>) -> Self {
        KVClient {
            store: KVStore::copy(store),
        }
    }

    pub fn insert(&self, key: &str, val: V) {
        self.store.write(key, val, None);
    }

    pub fn insert_with_expiration(&self, key: &str, val: V, dur: std::time::Duration) {
        self.store.write(key, val, Some(dur));
    }

    pub fn retrieve(&self, key: &str) -> Option<Arc<ExpiryValue<V>>> {
        self.store.read(key)
    }

    pub fn delete(&self, key: &str) -> Option<Arc<ExpiryValue<V>>> {
        self.store.delete(key)
    }

    pub fn retrieve_with_cb(&self, key: &str, callback: impl FnOnce(Option<&V>)) {
        self.store.read_cb(key, callback);
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

        assert_eq!(*res.val, vec![34, 89]);
        client.retrieve_with_cb(test_key, |val| {
            assert_eq!(*val.unwrap(), vec![34, 89]);
        });

        let deleted = client.delete(test_key).expect("Test key to be present");
        assert_eq!(*deleted.val, vec![34, 89]);

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

        assert_eq!(*res.val, vec![34, 89]);

        thread::sleep(Duration::from_millis(250).add(two_seconds_duration));

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
        assert_eq!(*deleted.val, vec![34, 89]);
        let deleted = client.delete(test_key2).expect("To have test_key 2");
        assert_eq!(*deleted.val, vec![11, 21]);
    }
}
