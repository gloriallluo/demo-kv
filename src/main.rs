//! kv-cli

use demo_kv::KVHandle;
use std::{io, path::PathBuf, str::FromStr};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short, long, parse(from_os_str), default_value = "./data/dkv.db")]
    mount_path: PathBuf,
}

#[derive(Debug, Clone, Copy)]
enum PutStmt {
    Val(u64),
    Incr(u64),
}

#[derive(Debug)]
struct ParseCommandError;

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
        log::trace!("Got commands: {cmds:?}");
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
                if let Ok(v) = cmds[2].parse::<u64>() {
                    log::trace!("PutStmt::Val");
                    let stmt = PutStmt::Val(v);
                    Ok(Command::Put { key, stmt })
                } else {
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
                    let i = inc.parse::<u64>().map_err(|_| ParseCommandError)?;
                    let stmt = PutStmt::Incr(i);
                    Ok(Command::Put { key, stmt })
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
async fn main() {
    env_logger::init();
    let opt = Opt::from_args();
    let mut buffer = String::new();
    let kv = KVHandle::new(opt.mount_path).await;
    while io::stdin().read_line(&mut buffer).is_ok() {
        if buffer.as_str() == "exit\n" {
            break;
        }
        let command = buffer.as_str().parse().unwrap();
        match command {
            Command::Get { key } => {
                if let Some(res) = kv.get(key) {
                    println!("{res}");
                } else {
                    println!("None");
                }
            }
            Command::Put { key, stmt } => match stmt {
                PutStmt::Val(val) => kv.put(key, val),
                PutStmt::Incr(inc) => {
                    let orig = kv.get(key).unwrap();
                    kv.put(key, orig + inc);
                }
            },
            Command::Del { key } => kv.del(key),
        };
        buffer.clear();
    }
}
