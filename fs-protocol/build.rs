use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed=proto/fs.proto");

    let mut config = prost_build::Config::new();
    config.out_dir(std::env::var_os("OUT_DIR").expect("OUT_DIR must be set by cargo"));
    config.compile_protos(&["proto/fs.proto"], &["proto"])?;
    Ok(())
}
