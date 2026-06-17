use std::error::Error;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn Error>> {
    let proto_path = PathBuf::from("../session.proto");
    if !proto_path.exists() {
        panic!(
            "session.proto not found at {}",
            proto_path.to_string_lossy()
        );
    }

    println!("cargo:rerun-if-changed={}", proto_path.to_string_lossy());

    let mut config = prost_build::Config::new();
    config.out_dir(std::env::var_os("OUT_DIR").expect("OUT_DIR must be set by cargo"));

    config.compile_protos(&[proto_path.as_path()], &[proto_path.parent().unwrap()])?;
    Ok(())
}
