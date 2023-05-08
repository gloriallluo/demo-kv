//! kv-cli

use std::error::Error as StdError;
use std::fmt;
use std::{io, str::FromStr};
use tonic::Request;

mod api {
    tonic::include_proto!("demokv.api");
}

use api::{kv_client::KvClient, DelArg, GetArg, IncArg, PutArg};

#[derive(Debug, Clone, Copy)]
enum PutStmt {
    Val(i64),
    Incr(i64),
}

#[derive(Debug)]
struct ParseCommandError;

impl fmt::Display for ParseCommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("command parse error")
    }
}

impl StdError for ParseCommandError {}

#[derive(Debug, Clone, Copy)]
enum Command {
    Get { key: char },
    Put { key: char, stmt: PutStmt },
    Del { key: char },
}

impl FromStr for Command {
    type Err = ParseCommandError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let cmds = s
            .split(char::is_whitespace)
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        log::trace!("input: {cmds:?}");
        match cmds[0] {
            "GET" => {
                if cmds.len() != 2 {
                    log::error!("Invalid arg length, should be 2");
                    return Err(ParseCommandError);
                }
                if cmds[1].len() == 1 {
                    let key = cmds[1].chars().next().ok_or(ParseCommandError)?;
                    Ok(Command::Get { key })
                } else {
                    log::error!("Invalid key length");
                    Err(ParseCommandError)
                }
            }
            "PUT" => {
                if cmds.len() != 3 {
                    log::error!("Invalid arg length, should be 3");
                    return Err(ParseCommandError);
                }
                if cmds[1].len() != 1 {
                    log::error!("Invalid key length");
                    return Err(ParseCommandError);
                }
                let key = cmds[1].chars().next().ok_or(ParseCommandError)?;
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
                        .and_then(|s| s.split_once('+'))
                        .ok_or(ParseCommandError)?;
                    if key2.len() != 1 {
                        log::error!("Invalid key length");
                        return Err(ParseCommandError);
                    }
                    let key2 = key2.chars().next().ok_or(ParseCommandError)?;
                    if key2 != key {
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
                if cmds[1].len() == 1 {
                    let key = cmds[1].chars().next().ok_or(ParseCommandError)?;
                    Ok(Command::Del { key })
                } else {
                    log::error!("Invalid key length");
                    Err(ParseCommandError)
                }
            }
            cmd => {
                log::error!("Unknown command {cmd}");
                Err(ParseCommandError)
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    env_logger::init();

    let mut client = KvClient::connect("127.0.0.1:33333").await?;

    let mut buffer = String::new();
    while io::stdin().read_line(&mut buffer).is_ok() {
        if buffer.as_str() == "exit\n" {
            break;
        }
        let command = buffer.as_str().parse()?;
        log::debug!("command: {command:?}");
        match command {
            Command::Get { key } => {
                let arg = Request::new(GetArg { key: key as _ });
                let reply = client.get(arg).await?.into_inner();

                if reply.has_value {
                    println!("{}", reply.value);
                } else {
                    log::error!("None");
                }
            }
            Command::Put { key, stmt } => match stmt {
                PutStmt::Val(value) => {
                    let arg = Request::new(PutArg {
                        key: key as _,
                        value,
                    });
                    client.put(arg).await?.into_inner();
                }
                PutStmt::Incr(value) => {
                    let arg = Request::new(IncArg {
                        key: key as _,
                        value,
                    });
                    client.inc(arg).await?.into_inner();
                }
            },

            Command::Del { key } => {
                let arg = Request::new(DelArg { key: key as _ });
                let _res = client.del(arg).await?.into_inner();
            }
        };
        buffer.clear();
    }
    Ok(())
}
