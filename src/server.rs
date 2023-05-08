//! Server cli tool

use demo_kv::KVHandle;
use std::error::Error as StdError;
use std::path::PathBuf;
use structopt::StructOpt;
use tonic::{transport::Server, Request, Response, Status};

mod api {
    tonic::include_proto!("demokv.api");
}

use api::{
    kv_server::{Kv, KvServer},
    DelArg, DelReply, GetArg, GetReply, IncArg, IncReply, PutArg, PutReply,
};

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short, long, parse(from_os_str), default_value = "./data/dkv.db")]
    snapshot_path: PathBuf,
    #[allow(dead_code)]
    #[structopt(short, long, parse(from_os_str), default_value = "./data/dkv.log")]
    log_path: PathBuf,
}

#[derive(Debug)]
struct DemoKv {
    kv: KVHandle,
}

#[tonic::async_trait]
impl Kv for DemoKv {
    async fn get(&self, _req: Request<GetArg>) -> Result<Response<GetReply>, Status> {
        todo!()
    }
    async fn put(&self, _req: Request<PutArg>) -> Result<Response<PutReply>, Status> {
        todo!()
    }
    async fn inc(&self, _req: Request<IncArg>) -> Result<Response<IncReply>, Status> {
        todo!()
    }
    async fn del(&self, _req: Request<DelArg>) -> Result<Response<DelReply>, Status> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    env_logger::init();
    let addr = "127.0.0.1:33333".parse()?;
    let opt = Opt::from_args();
    let sevr = DemoKv {
        kv: KVHandle::new(opt.snapshot_path).await,
    };
    Server::builder()
        .add_service(KvServer::new(sevr))
        .serve(addr)
        .await?;
    Ok(())
}
