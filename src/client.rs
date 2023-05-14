//! kv-cli

use demo_kv::api::CommitArg;
use demo_kv::api::{kv_client::KvClient, DelArg, GetArg, IncArg, PutArg, ReadArg};
use std::error::Error as StdError;
use std::sync::Arc;
use std::{collections::HashMap, fmt, io, str::FromStr};
use tokio::sync::mpsc;
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

#[derive(Debug, Clone, Copy)]
enum PutStmt {
    Val(i64),
    Incr(i64),
}

#[derive(Debug, Clone)]
enum Command {
    Start,
    Commit,
    Abort,
    Get { key: String },
    Put { key: String, stmt: PutStmt },
    Del { key: String },
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
            "START" => Ok(Command::Start),
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
                    let (key2, inc) = cmds[2]
                        .strip_prefix('(')
                        .and_then(|s| s.strip_suffix(')'))
                        .and_then(|s| s.split_once(|c| c == '+' || c == '-'))
                        .ok_or(ParseCommandError)?;
                    if key2 != key.as_str() {
                        log::error!("Only modification on the same key is supported");
                        return Err(ParseCommandError);
                    }
                    let inc = inc.parse::<i64>().map_err(|_| ParseCommandError)?;
                    Ok(Command::Put {
                        key,
                        stmt: PutStmt::Incr(inc),
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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn StdError>> {
    env_logger::init();

    let mut client = KvClient::connect("http://127.0.0.1:33333").await?;

    let mut buffer = String::new();
    let mut txn_ctx: Option<TxnContext> = None;
    while io::stdin().read_line(&mut buffer).is_ok() {
        if buffer.is_empty() || buffer.as_str() == "exit\n" || buffer.as_str() == "exit\t\n" {
            break;
        }
        if let Ok(command) = buffer.as_str().parse() {
            log::debug!("command: {command:?}");
            match command {
                Command::Start => txn_ctx = Some(TxnContext::default()),
                Command::Get { key } => {
                    if let Some(TxnContext {
                        start_ts, read_set, ..
                    }) = &mut txn_ctx
                    {
                        // Check read set first to achieve Repeatable Read.
                        match read_set.get(&key) {
                            Some(Some(v)) => println!("{key}: {v}"),
                            Some(None) => eprintln!("{key}: None"),
                            None => {
                                let arg = Request::new(ReadArg {
                                    ts: start_ts.unwrap_or(usize::MAX) as i64,
                                    key: key.clone(),
                                });
                                let reply = client.read_txn(arg).await?.into_inner();
                                // Print the value, update read set.
                                if reply.has_value {
                                    println!("{key}: {}", reply.value);
                                    read_set.insert(key, Some(reply.value));
                                } else {
                                    eprintln!("{key}: None");
                                    read_set.insert(key, None);
                                }
                                // Update start_ts acquired from server.
                                if start_ts.is_none() {
                                    *start_ts = Some(reply.ts as _);
                                }
                            }
                        }
                    } else {
                        let arg = Request::new(GetArg { key: key.clone() });
                        let reply = client.get(arg).await?.into_inner();
                        if reply.has_value {
                            println!("{key}: {}", reply.value);
                        } else {
                            eprintln!("{key}: None");
                        }
                    }
                }
                Command::Put { key, stmt } => {
                    if let Some(TxnContext {
                        start_ts,
                        read_set,
                        write_set,
                    }) = &mut txn_ctx
                    {
                        match stmt {
                            PutStmt::Val(value) => {
                                // Read my own writes.
                                read_set.insert(key.clone(), Some(value));
                                // Writes are buffered.
                                write_set.insert(key, Some(value));
                            }
                            PutStmt::Incr(value) => {
                                // Check read set first to achieve Repeatable Read.
                                let orig = match read_set.get(&key) {
                                    Some(Some(v)) => *v,
                                    Some(None) => panic!("{key} does not exist"),
                                    None => {
                                        let arg = Request::new(ReadArg {
                                            ts: start_ts.unwrap_or(usize::MAX) as i64,
                                            key: key.clone(),
                                        });
                                        let reply = client.read_txn(arg).await?.into_inner();
                                        // Print the value, update read set.
                                        if reply.has_value {
                                            read_set.insert(key.clone(), Some(reply.value));
                                        } else {
                                            panic!("{key} does not exist")
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
                                client.put(arg).await?.into_inner();
                            }
                            PutStmt::Incr(value) => {
                                let arg = Request::new(IncArg { key, value });
                                client.inc(arg).await?.into_inner();
                            }
                        }
                    }
                }
                Command::Del { key } => {
                    if let Some(TxnContext {
                        read_set,
                        write_set,
                        ..
                    }) = &mut txn_ctx
                    {
                        read_set.insert(key.clone(), None);
                        write_set.insert(key, None);
                    } else {
                        let arg = Request::new(DelArg { key });
                        let _res = client.del(arg).await?.into_inner();
                    }
                }
                Command::Commit => {
                    if let Some(TxnContext {
                        start_ts,
                        write_set,
                        ..
                    }) = txn_ctx
                    {
                        let (tx, rx) = mpsc::channel(8);
                        let write_set = Arc::new(write_set);
                        let t = task::spawn(async move {
                            for (k, v) in write_set.iter() {
                                let arg = CommitArg {
                                    ts: start_ts.unwrap_or(usize::MAX) as _,
                                    key: k.clone(),
                                    delete: v.is_none(),
                                    value: v.unwrap_or(0),
                                };
                                tx.send(arg).await.expect("channel closed");
                            }
                        });
                        t.await.expect("task aborted");
                        let reply = client
                            .commit_txn(ReceiverStream::new(rx))
                            .await?
                            .into_inner();
                        if reply.commit {
                            println!("Transaction commit");
                        } else {
                            eprintln!("Transaction abort");
                        }
                        txn_ctx = None;
                    }
                }
                Command::Abort => txn_ctx = None,
            };
        }
        buffer.clear();
    }
    Ok(())
}
