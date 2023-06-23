//! KV instance.

use crate::{
    api::{
        kv_server::Kv, CommitArg, CommitReply, DelArg, DelReply, GetArg, GetReply, IncArg,
        IncReply, PutArg, PutReply, ReadArg, ReadReply,
    },
    log::{LogOp, Loggable, Logger},
    ts::TS_MANAGER,
};
use concurrent_map::ConcurrentMap;
use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};
use tokio::{task, time};
// use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

/// KV Handle. It's a reference to `KVInner`.
#[derive(Debug, Clone)]
pub struct KVHandle {
    inner: Arc<KVInner>,
}

#[tonic::async_trait]
impl Kv for KVHandle {
    async fn get(&self, req: Request<GetArg>) -> Result<Response<GetReply>, Status> {
        let GetArg { key } = req.into_inner();
        log::debug!("Get - {key}");
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
        log::debug!("Get reply {reply:?}");
        Ok(Response::new(reply))
    }

    async fn put(&self, req: Request<PutArg>) -> Result<Response<PutReply>, Status> {
        let PutArg { key, value } = req.into_inner();
        log::debug!("Put - {key}={value}");
        let ts = TS_MANAGER.new_ts();
        self.inner.update(key, Some(value), ts);
        let reply = PutReply {};
        Ok(Response::new(reply))
    }

    async fn inc(&self, req: Request<IncArg>) -> Result<Response<IncReply>, Status> {
        let IncArg { key, key1, value } = req.into_inner();
        log::debug!("Inc - {key}+={value}");
        let ts = TS_MANAGER.new_ts();
        let reply = if let Some(orig) = self.inner.get(key1, ts) {
            self.inner.update(key, Some(orig + value), ts);
            IncReply { has_value: true }
        } else {
            IncReply { has_value: false }
        };
        Ok(Response::new(reply))
    }

    async fn del(&self, req: Request<DelArg>) -> Result<Response<DelReply>, Status> {
        let DelArg { key } = req.into_inner();
        log::debug!("Del - {key}");
        let ts = TS_MANAGER.new_ts();
        self.inner.update(key, None, ts);
        let reply = DelReply {};
        Ok(Response::new(reply))
    }

    async fn read_txn(&self, req: Request<ReadArg>) -> Result<Response<ReadReply>, tonic::Status> {
        let ReadArg { ts, key } = req.into_inner();
        log::debug!("TxnRead - {key}@{ts}");
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

        let txn_id = rand::random::<i64>();
        let start_ts = if ts == -1 {
            TS_MANAGER.new_ts() // blind write
        } else {
            ts as _
        };
        let write_set: HashMap<String, Option<i64>> =
            bincode::deserialize(write_set.as_bytes()).expect("failed to deserialize");
        let mut write_set = write_set.into_iter().collect::<Vec<_>>();
        write_set.sort();
        log::debug!("CommitTxn begin_ts={start_ts} write_set={write_set:?}");
        let mut commit = true;

        // Lock and validate
        for (key, _) in write_set.iter() {
            log::trace!("before lock {key}");
            if !self.inner.lock(key, txn_id) {
                commit = false;
                break;
            }
            log::trace!("acquire lock {key}");
            if !self.inner.validate(key, start_ts) {
                // log::warn!("key {key} validation failed");
                commit = false;
                break;
            }
        }

        if !commit {
            for (key, _) in write_set.iter() {
                self.inner.unlock(key, txn_id);
                log::trace!("release lock {key}");
            }
            return Ok(Response::new(CommitReply { commit }));
        }

        let commit_ts = TS_MANAGER.new_ts();
        for (key, value) in write_set.iter() {
            self.inner.update(key.to_owned(), *value, commit_ts);
        }

        // Release locks
        for (key, _) in write_set.iter() {
            self.inner.unlock(key, txn_id);
            log::trace!("release lock {key}");
        }
        log::debug!("commit_txn reply {commit}");
        Ok(Response::new(CommitReply { commit }))
    }
}

impl KVHandle {
    /// Get a new instance of `KVHandle`.
    pub async fn new(mount_path: &Path, log_path: &Path) -> Self {
        // Get a Logger first.
        let logger = Logger::new(log_path).await;
        // Try to restore from snapshot.
        let kv = if let Some(kv) = Self::restore(mount_path, &logger).await {
            kv
        } else {
            KVInner::new(Arc::clone(&logger))
        };

        // Replay log.
        logger.restore(&kv);
        let this = Self {
            inner: Arc::new(kv),
        };

        // Spawn a task that does persisting.
        let that = this.clone();
        let path = mount_path.to_path_buf();
        task::spawn(async move {
            loop {
                time::sleep(Duration::from_millis(100)).await;
                that.persist(&path).await;
            }
        });
        this
    }

    /// Restore KV state from snapshot file. None if no snapshot available.
    async fn restore(_path: &Path, _logger: &Arc<Logger>) -> Option<KVInner> {
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
    logger: Arc<Logger>,
}

impl KVInner {
    const LOCK: usize = usize::MAX;

    fn new(logger: Arc<Logger>) -> Self {
        Self {
            data: ConcurrentMap::default(),
            logger,
        }
    }

    fn get(&self, key: String, ts: usize) -> Option<i64> {
        log::trace!("get key={key} ts={ts}");
        let kt = &(key, ts);
        let (k, v) = self.data.get_lt(kt)?;
        if k.0 == kt.0 {
            v
        } else {
            None
        }
    }

    fn lock(&self, key: &String, txn_id: i64) -> bool {
        self.data
            .cas((key.to_owned(), Self::LOCK), None, Some(Some(txn_id)))
            .is_ok()
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

    fn update(&self, key: String, value: Option<i64>, ts: usize) {
        let op = LogOp {
            key: key.clone(),
            value,
            ts,
        };
        log::trace!("update key={key} value={value:?} ts={ts}");
        self.logger.log(&op);
        self.data.insert((key, ts), value);
    }
}

impl Loggable for KVInner {
    fn replay(&self, op: LogOp) {
        let LogOp { key, value, ts } = op;
        self.data.insert((key, ts), value);
    }
}
