//! KV instance.

use std::{collections::HashMap, sync::RwLock};

#[derive(Default)]
pub struct KV {
    inner: RwLock<KVInner>,
}

impl KV {
    pub fn get(&self, key: char) -> Option<u64> {
        self.inner.read().unwrap().get(key)
    }

    pub fn put(&self, key: char, value: u64) {
        self.inner.write().unwrap().put(key, value)
    }

    pub fn del(&self, key: char) {
        self.inner.write().unwrap().del(key)
    }
}

#[derive(Debug, Default)]
struct KVInner {
    data: HashMap<char, u64>,
}

impl KVInner {
    fn get(&self, key: char) -> Option<u64> {
        self.data.get(&key).copied()
    }

    fn put(&mut self, key: char, value: u64) {
        self.data.insert(key, value);
    }

    fn del(&mut self, key: char) {
        self.data.remove(&key);
    }
}
