//! demo-kv

#![deny(clippy::all)]

mod kv;
mod log;
mod ts;

pub use kv::KVHandle;

/// tonic APIs
pub mod api {
    tonic::include_proto!("demokv.api");
}
