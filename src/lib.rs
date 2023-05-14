//! demo-kv

#![deny(clippy::all, clippy::unwrap_used)]

mod kv;
mod log;
mod ts;

pub use kv::KVHandle;

/// tonic APIs
#[allow(clippy::unwrap_used)]
pub mod api {
    tonic::include_proto!("demokv.api");
}
