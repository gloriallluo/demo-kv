//! demo-kv

#![deny(clippy::all)]

pub mod cli;
mod kv;
mod log;
mod ts;

pub use kv::KVHandle;

/// tonic APIs
pub mod api {
    tonic::include_proto!("demokv.api");
}

pub mod peers {
    tonic::include_proto!("demokv.peers");
}
