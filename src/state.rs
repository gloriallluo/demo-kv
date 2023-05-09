//! KV instance.

use crate::api::{
    kv_server::Kv, DelArg, DelReply, GetArg, GetReply, IncArg, IncReply, PutArg, PutReply,
};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::{fs, task, time};
use tonic::{Request, Response, Status};

#[derive(Debug, Clone)]
pub struct KVHandle {
    inner: Arc<RwLock<KVInner>>,
}

#[tonic::async_trait]
impl Kv for KVHandle {
    async fn get(&self, req: Request<GetArg>) -> Result<Response<GetReply>, Status> {
        let GetArg { key } = req.into_inner();
        let reply = if let Some(value) = self.inner.read().unwrap().get(&key) {
            GetReply {
                has_value: true,
                value,
            }
        } else {
            GetReply {
                has_value: false,
                value: 0,
            }
        };
        Ok(Response::new(reply))
    }

    async fn put(&self, req: Request<PutArg>) -> Result<Response<PutReply>, Status> {
        let PutArg { key, value } = req.into_inner();
        self.inner.write().unwrap().put(key, value);
        let reply = PutReply {};
        Ok(Response::new(reply))
    }

    async fn inc(&self, req: Request<IncArg>) -> Result<Response<IncReply>, Status> {
        let IncArg { key, value } = req.into_inner();
        let mut inner = self.inner.write().unwrap();
        let reply = if let Some(orig) = inner.get(&key) {
            inner.put(key, orig + value);
            IncReply { has_value: true }
        } else {
            IncReply { has_value: false }
        };
        Ok(Response::new(reply))
    }

    async fn del(&self, req: Request<DelArg>) -> Result<Response<DelReply>, Status> {
        let DelArg { key } = req.into_inner();
        self.inner.write().unwrap().del(&key);
        let reply = DelReply {};
        Ok(Response::new(reply))
    }
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
    data: HashMap<String, i64>,
}

impl KVInner {
    fn get(&self, key: &String) -> Option<i64> {
        self.data.get(key).copied()
    }

    fn put(&mut self, key: String, value: i64) {
        // TODO add WAL
        self.data.insert(key, value);
    }

    fn del(&mut self, key: &String) {
        // TODO add WAL
        self.data.remove(key);
    }
}
