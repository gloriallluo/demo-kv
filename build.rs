//! Build script
use tonic_build::compile_protos;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    compile_protos("proto/rpc.proto")?;
    compile_protos("proto/peers.proto")?;
    Ok(())
}
