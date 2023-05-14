//! KV instance.

use crate::{
    api::{
        kv_server::Kv, CommitArg, CommitReply, DelArg, DelReply, GetArg, GetReply, IncArg,
        IncReply, PutArg, PutReply, ReadArg, ReadReply,
    },
    log::{LogCommand, LogOp, Loggable, Logger},
    ts::TS_MANAGER,
};
use concurrent_map::ConcurrentMap;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, Notify},
    task, time,
};
// use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

/// KV Handle. It's a reference to `KVInner`.
#[derive(Debug, Clone)]
pub struct KVHandle {
    inner: Arc<KVInner>,
    _logger: Arc<Logger>,
}

#[tonic::async_trait]
impl Kv for KVHandle {
    async fn get(&self, req: Request<GetArg>) -> Result<Response<GetReply>, Status> {
        let GetArg { key } = req.into_inner();
        let reply = if let Some(value) = self.inner.get(key, usize::MAX) {
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
        let ts = TS_MANAGER.new_ts();
        self.inner.update(key, Some(value), ts).await;
        let reply = PutReply {};
        Ok(Response::new(reply))
    }

    async fn inc(&self, req: Request<IncArg>) -> Result<Response<IncReply>, Status> {
        let IncArg { key, value } = req.into_inner();
        let ts = TS_MANAGER.new_ts();
        let reply = if let Some(orig) = self.inner.get(key.clone(), ts) {
            self.inner.update(key, Some(orig + value), ts).await;
            IncReply { has_value: true }
        } else {
            IncReply { has_value: false }
        };
        Ok(Response::new(reply))
    }

    async fn del(&self, req: Request<DelArg>) -> Result<Response<DelReply>, Status> {
        let DelArg { key } = req.into_inner();
        let ts = TS_MANAGER.new_ts();
        self.inner.update(key, None, ts).await;
        let reply = DelReply {};
        Ok(Response::new(reply))
    }

    async fn read_txn(&self, req: Request<ReadArg>) -> Result<Response<ReadReply>, tonic::Status> {
        log::debug!("read_txn arg {req:?}");
        let ReadArg { ts, key } = req.into_inner();
        let ts = if ts == -1 {
            TS_MANAGER.new_ts()
        } else {
            ts as _
        };
        let reply = if let Some(value) = self.inner.get(key, ts) {
            ReadReply {
                ts: ts as _,
                has_value: true,
                value,
            }
        } else {
            ReadReply {
                ts: ts as _,
                has_value: false,
                value: 0,
            }
        };
        log::debug!("read_txn reply {reply:?}");
        Ok(Response::new(reply))
    }

    async fn commit_txn(
        &self,
        req: Request<CommitArg>,
    ) -> Result<Response<CommitReply>, tonic::Status> {
        let CommitArg { ts, write_set } = req.into_inner();

        let txn_id = rand::random::<i64>(); // XXX collision
        let start_ts = if ts == -1 {
            TS_MANAGER.new_ts() // blind write
        } else {
            ts as _
        };
        let write_set: HashMap<String, Option<i64>> =
            bincode::deserialize(write_set.as_bytes()).expect("failed to deserialize");
        log::debug!("commit_txn begin_ts={start_ts} write_set={write_set:?}");
        let mut commit = true;

        // Lock and validate
        for (key, _) in write_set.iter() {
            self.inner.lock(key, txn_id);
            if !self.inner.validate(key, start_ts) {
                log::warn!("key {key} validation failed");
                commit = false;
                break;
            }
        }

        // Commit
        if commit {
            let commit_ts = TS_MANAGER.new_ts();
            for (key, value) in write_set.iter() {
                self.inner.update(key.to_owned(), *value, commit_ts).await;
            }
        }

        // Release locks
        for (key, _) in write_set.iter() {
            self.inner.unlock(key, txn_id);
        }
        log::debug!("commit_txn reply {commit}");
        Ok(Response::new(CommitReply { commit }))
    }
}

impl KVHandle {
    /// Get a new instance of `KVHandle`.
    pub async fn new(mount_path: PathBuf, log_path: PathBuf) -> Self {
        // Get a Logger first.
        let (logger, tx) = Logger::new(&log_path).await;
        // Try to restore from snapshot.
        let kv = if let Some(kv) = Self::restore(&mount_path, &tx).await {
            kv
        } else {
            KVInner::new(tx)
        };

        // Replay log.
        logger.restore(&kv);
        let this = Self {
            inner: Arc::new(kv),
            _logger: logger,
        };

        // Spawn a task that does persisting.
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

    /// Restore KV state from snapshot file. None if no snapshot available.
    async fn restore(_path: &Path, _tx: &mpsc::UnboundedSender<LogCommand>) -> Option<KVInner> {
        // TODO(snapshot) restore
        None
    }

    /// Dump snapshot.
    async fn persist(&self, _path: &Path) {
        // TODO(snapshot) persist
    }
}

type KeyVersion = (String, usize);

/// Real KV instance.
#[derive(Debug)]
struct KVInner {
    data: ConcurrentMap<KeyVersion, Option<i64>>, // TODO(gc)
    log_tx: mpsc::UnboundedSender<LogCommand>,
}

impl KVInner {
    const LOCK: usize = usize::MAX;

    fn new(log_tx: mpsc::UnboundedSender<LogCommand>) -> Self {
        Self {
            data: ConcurrentMap::default(),
            log_tx,
        }
    }

    fn get(&self, key: String, ts: usize) -> Option<i64> {
        log::trace!("get key={key} ts={ts}");
        self.data.get_lt(&(key, ts))?.1
    }

    fn lock(&self, key: &String, txn_id: i64) {
        // XXX wait or not
        while self
            .data
            .cas((key.to_owned(), Self::LOCK), None, Some(Some(txn_id)))
            .is_err()
        {}
    }

    fn unlock(&self, key: &String, txn_id: i64) {
        self.data
            .cas((key.to_owned(), Self::LOCK), Some(&Some(txn_id)), None)
            .ok();
    }

    fn validate(&self, key: &String, start_ts: usize) -> bool {
        if let Some(((ref k, ts), _)) = self.data.get_lt(&(key.to_owned(), Self::LOCK)) {
            log::debug!("validate key={k} ts={ts}");
            return k != key || ts < start_ts;
        }
        true
    }

    async fn update(&self, key: String, value: Option<i64>, ts: usize) {
        let op = LogOp {
            key: key.clone(),
            value,
            ts,
        };
        log::trace!("update key={key} value={value:?} ts={ts}");
        let done = Arc::new(Notify::new());
        self.log_tx
            .send(LogCommand {
                op,
                done: Arc::clone(&done),
            })
            .expect("channel closed");
        done.notified().await;
        self.data.insert((key, ts), value);
    }
}

impl Loggable for KVInner {
    fn replay(&self, op: LogOp) {
        let LogOp { key, value, ts } = op;
        self.data.insert((key, ts), value);
    }
}
