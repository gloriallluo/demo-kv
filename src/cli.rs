//! Client interface

use std::error::Error as StdError;
use std::{collections::HashMap, fmt, str::FromStr};

use crate::api::{kv_client::KvClient, CommitArg, DelArg, GetArg, IncArg, PutArg, ReadArg};
use tonic::transport::Channel;
use tonic::Request;

#[derive(Debug, Clone)]
pub enum PutStmt {
    Val(i64),
    Incr(String, i64),
}

#[derive(Debug, Clone)]
enum Command {
    Begin,
    Commit,
    Abort,
    Get { key: String },
    Put { key: String, stmt: PutStmt },
    Del { key: String },
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum KvOutput {
    Ok,
    Value(Option<i64>),
    Commit,
    Abort,
}

#[derive(Debug)]
pub struct ParseCommandError;

impl fmt::Display for ParseCommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("command parse error")
    }
}

impl StdError for ParseCommandError {}

impl FromStr for Command {
    type Err = ParseCommandError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let cmds = s
            .split(char::is_whitespace)
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        log::trace!("input: {cmds:?}");
        if cmds.is_empty() {
            return Err(ParseCommandError);
        }
        match cmds[0] {
            "BEGIN" => Ok(Command::Begin),
            "COMMIT" => Ok(Command::Commit),
            "ABORT" => Ok(Command::Abort),
            "GET" => {
                if cmds.len() != 2 {
                    log::error!("Invalid arg length, should be 2");
                    return Err(ParseCommandError);
                }
                let key = cmds[1].to_string();
                Ok(Command::Get { key })
            }
            "PUT" => {
                if cmds.len() != 3 {
                    log::error!("Invalid arg length, should be 3");
                    return Err(ParseCommandError);
                }
                let key = cmds[1].to_string();
                if let Ok(val) = cmds[2].parse::<i64>() {
                    log::trace!("PutStmt::Val");
                    Ok(Command::Put {
                        key,
                        stmt: PutStmt::Val(val),
                    })
                } else {
                    // may be a incr statement
                    log::trace!("PutStmt::Incr");
                    let mut neg = false;
                    let (key1, inc) = cmds[2]
                        .strip_prefix('(')
                        .and_then(|s| s.strip_suffix(')'))
                        .and_then(|s| {
                            s.split_once(|c| {
                                if c == '-' {
                                    neg = true;
                                }
                                c == '+' || c == '-'
                            })
                        })
                        .ok_or(ParseCommandError)?;
                    let mut inc = inc.parse::<i64>().map_err(|_| ParseCommandError)?;
                    if neg {
                        inc = inc.wrapping_neg();
                    }
                    Ok(Command::Put {
                        key,
                        stmt: PutStmt::Incr(key1.to_string(), inc),
                    })
                }
            }
            "DEL" => {
                if cmds.len() != 2 {
                    log::error!("Invalid arg length, should be 2");
                    return Err(ParseCommandError);
                }
                let key = cmds[1].to_string();
                Ok(Command::Del { key })
            }
            cmd => {
                log::error!("Unknown command {cmd}");
                Err(ParseCommandError)
            }
        }
    }
}

/// Ongoing transaction context used in client side.
#[derive(Debug, Default)]
struct TxnContext {
    start_ts: Option<usize>,
    read_set: HashMap<String, Option<i64>>,
    write_set: HashMap<String, Option<i64>>,
}

#[derive(Debug)]
pub struct Client {
    kv_client: KvClient<Channel>,
    txn_ctx: Option<TxnContext>,
}

impl Client {
    pub async fn new(addr: String) -> Result<Self, Box<dyn StdError>> {
        let kv_client = KvClient::connect(addr).await?;
        Ok(Self {
            kv_client,
            txn_ctx: None,
        })
    }

    async fn handle_get(&mut self, key: String) -> Result<KvOutput, Box<dyn StdError>> {
        let out = if let Some(TxnContext {
            start_ts, read_set, ..
        }) = &mut self.txn_ctx
        {
            // Check read set first to achieve Repeatable Read.
            match read_set.get(&key) {
                Some(&v) => KvOutput::Value(v),
                None => {
                    let arg = Request::new(ReadArg {
                        ts: start_ts.unwrap_or(usize::MAX) as i64,
                        key: key.clone(),
                    });
                    let reply = self.kv_client.read_txn(arg).await?.into_inner();
                    // Update start_ts acquired from server.
                    if start_ts.is_none() {
                        *start_ts = Some(reply.ts as _);
                    }
                    // Print the value, update read set.
                    if reply.has_value {
                        read_set.insert(key, Some(reply.value));
                        KvOutput::Value(Some(reply.value))
                    } else {
                        read_set.insert(key, None);
                        KvOutput::Value(None)
                    }
                }
            }
        } else {
            let arg = Request::new(GetArg { key: key.clone() });
            let reply = self.kv_client.get(arg).await?.into_inner();
            if reply.has_value {
                KvOutput::Value(Some(reply.value))
            } else {
                KvOutput::Value(None)
            }
        };
        Ok(out)
    }

    async fn handle_put(
        &mut self,
        key: String,
        stmt: PutStmt,
    ) -> Result<KvOutput, Box<dyn StdError>> {
        if let Some(TxnContext {
            start_ts,
            read_set,
            write_set,
        }) = &mut self.txn_ctx
        {
            match stmt {
                PutStmt::Val(value) => {
                    // Read my own writes.
                    read_set.insert(key.clone(), Some(value));
                    // Writes are buffered.
                    write_set.insert(key, Some(value));
                }
                PutStmt::Incr(key1, value) => {
                    // Check read set first to achieve Repeatable Read.
                    let orig = match read_set.get(&key1) {
                        Some(Some(v)) => *v,
                        Some(None) => panic!("{key} does not exist"),
                        None => {
                            let arg = Request::new(ReadArg {
                                ts: start_ts.unwrap_or(usize::MAX) as i64,
                                key: key1.clone(),
                            });
                            let reply = self.kv_client.read_txn(arg).await?.into_inner();
                            // Print the value, update read set.
                            if reply.has_value {
                                read_set.insert(key1.clone(), Some(reply.value));
                            } else {
                                panic!("{key1} does not exist")
                            }
                            // Update start_ts acquired from server.
                            if start_ts.is_none() {
                                *start_ts = Some(reply.ts as _);
                            }
                            reply.value
                        }
                    };
                    // Read my own writes.
                    read_set.insert(key.clone(), Some(orig + value));
                    // Writes are buffered.
                    write_set.insert(key, Some(orig + value));
                }
            }
        } else {
            match stmt {
                PutStmt::Val(value) => {
                    let arg = Request::new(PutArg { key, value });
                    self.kv_client.put(arg).await?.into_inner();
                }
                PutStmt::Incr(key1, value) => {
                    let arg = Request::new(IncArg { key, key1, value });
                    self.kv_client.inc(arg).await?.into_inner();
                }
            }
        }
        Ok(KvOutput::Ok)
    }

    async fn handle_del(&mut self, key: String) -> Result<KvOutput, Box<dyn StdError>> {
        if let Some(TxnContext {
            read_set,
            write_set,
            ..
        }) = &mut self.txn_ctx
        {
            read_set.insert(key.clone(), None);
            write_set.insert(key, None);
        } else {
            let arg = Request::new(DelArg { key });
            let _res = self.kv_client.del(arg).await?.into_inner();
        }
        Ok(KvOutput::Ok)
    }

    async fn handle_commit(&mut self) -> Result<KvOutput, Box<dyn StdError>> {
        if let Some(TxnContext {
            start_ts,
            write_set,
            ..
        }) = &mut self.txn_ctx
        {
            let write_set = bincode::serialize(&write_set).expect("failed to serialize");
            let arg = Request::new(CommitArg {
                ts: start_ts.unwrap_or(usize::MAX) as _,
                write_set: unsafe { String::from_utf8_unchecked(write_set) },
            });
            self.txn_ctx = None;

            let reply = self.kv_client.commit_txn(arg).await?.into_inner();
            if reply.commit {
                Ok(KvOutput::Commit)
            } else {
                Ok(KvOutput::Abort)
            }
        } else {
            panic!("no active transaction");
        }
    }

    pub async fn handle(&mut self, cmd: &str) -> Result<KvOutput, Box<dyn StdError>> {
        let cmd: Command = cmd.parse()?;
        log::trace!("command: {cmd:?}");
        let output = match cmd {
            Command::Begin => {
                self.txn_ctx = Some(TxnContext::default());
                KvOutput::Ok
            }
            Command::Get { key } => self.handle_get(key).await?,
            Command::Put { key, stmt } => self.handle_put(key, stmt).await?,
            Command::Del { key } => self.handle_del(key).await?,
            Command::Commit => self.handle_commit().await?,
            Command::Abort => {
                self.txn_ctx = None;
                KvOutput::Ok
            }
        };
        Ok(output)
    }
}
