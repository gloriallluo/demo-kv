//! KV instance.

use crate::{
    api::{kv_server::Kv, DelArg, DelReply, GetArg, GetReply, IncArg, IncReply, PutArg, PutReply},
    log::{LogCommand, LogOp, Loggable, Logger},
};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc, RwLock},
    time::Duration,
};
use tokio::{sync::{mpsc, Notify}, task, time};
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
        self.inner.write().unwrap().del(key);
        let reply = DelReply {};
        Ok(Response::new(reply))
    }
}

impl KVHandle {
    pub async fn new(mount_path: PathBuf, log_path: PathBuf) -> Self {
        // XXX piece of shit
        let (mut kv, logger) = if let Some(res) = Self::restore(&mount_path).await {
            res
        } else {
            KVInner::new(&log_path).await
        };
        logger.restore(&mut kv).await;
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

    async fn restore(_path: &Path) -> Option<(KVInner, Arc<Logger>)> {
        // TODO snapshot
        // let data = fs::read(path).await.ok()?;
        // bincode::deserialize::<KVInner>(&data[..]).ok()
        None
    }

    async fn persist(&self, _path: &Path) {
        // TODO snapshot
        // let data = {
        //     let kv = self.inner.read().unwrap();
        //     bincode::serialize(&*kv).unwrap()
        // };
        // fs::write(path, &data[..]).await.unwrap();
    }
}

#[derive(Debug)]
struct KVInner {
    data: HashMap<String, Versions>,
    log_tx: mpsc::UnboundedSender<LogCommand>,
}

impl KVInner {
    async fn new(log_path: &Path) -> (Self, Arc<Logger>) {
        let (logger, log_tx) = Logger::new(&log_path).await;
        let me = Self {
            data: HashMap::default(),
            log_tx,
        };
        (me, logger)
    }

    fn get(&self, key: &String) -> Option<i64> {
        self.data.get(key)?.get_latest()
    }

    fn put(&mut self, key: String, value: i64) {
        let op = LogOp { key: key.clone(), value: Some(value) };
        let done = Arc::new(Notify::new());
        self.log_tx.send(LogCommand { op, done: Arc::clone(&done) }).unwrap();
        self.data.entry(key).or_default().update(0, Some(value));
    }

    fn del(&mut self, key: String) {
        let op = LogOp { key: key.clone(), value: None };
        let done = Arc::new(Notify::new());
        self.log_tx.send(LogCommand { op, done: Arc::clone(&done) }).unwrap();
        self.data.entry(key).or_default().update(0, None);
    }
}

impl Loggable for KVInner {
    fn reply(&mut self, op: LogOp) {
        let LogOp { key, value } = op;
        if let Some(value) = value {
            self.put(key, value);
        } else {
            self.del(key);
        }
    }
}

/// Multiple versions of a key.
#[derive(Debug, Default)]
#[allow(dead_code)]
struct Versions {
    /// A lock.
    updating: AtomicBool,
    /// Sorted by version number.
    versions: Vec<(usize, Option<i64>)>,
}

impl Versions {
    fn get_latest(&self) -> Option<i64> {
        // XXX to be deprecated
        todo!()
    }

    #[allow(dead_code)]
    fn get_version(&self, _version: usize) -> Option<i64> {
        todo!()
    }

    fn update(&self, _version: usize, _value: Option<i64>) {
        todo!()
    }
}
