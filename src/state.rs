//! KV instance.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::{fs, task, time};

#[derive(Debug, Clone)]
pub struct KVHandle {
    inner: Arc<RwLock<KVInner>>,
}

impl KVHandle {
    pub async fn new(mount_path: PathBuf) -> Self {
        let kv = Self::restore(&mount_path).await.unwrap_or_default();
        let this = Self {
            inner: Arc::new(RwLock::new(kv)),
        };
        let that = this.clone();
        task::spawn(async move {
            let path = mount_path;
            loop {
                time::sleep(Duration::from_millis(100)).await;
                that.persist(&path).await;
            }
        });
        this
    }

    pub fn get(&self, key: char) -> Option<u64> {
        self.inner.read().unwrap().get(key)
    }

    pub fn put(&self, key: char, value: u64) {
        self.inner.write().unwrap().put(key, value)
    }

    pub fn del(&self, key: char) {
        self.inner.write().unwrap().del(key)
    }

    async fn restore(path: &Path) -> Option<KVInner> {
        let data = fs::read(path).await.ok()?;
        bincode::deserialize::<KVInner>(&data[..]).ok()
    }

    async fn persist(&self, path: &Path) {
        // XXX too much unwrap
        let data = {
            let kv = self.inner.read().unwrap();
            bincode::serialize(&*kv).unwrap()
        };
        fs::write(path, &data[..]).await.unwrap();
    }
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct KVInner {
    data: HashMap<char, u64>,
}

impl KVInner {
    fn get(&self, key: char) -> Option<u64> {
        self.data.get(&key).copied()
    }

    fn put(&mut self, key: char, value: u64) {
        // TODO add WAL
        self.data.insert(key, value);
    }

    fn del(&mut self, key: char) {
        // TODO add WAL
        self.data.remove(&key);
    }
}
