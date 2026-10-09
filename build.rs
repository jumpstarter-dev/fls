use std::{env, fs, path::PathBuf};

fn main() {
    let src = "remote/flswh-aarch64-qnx7";
    let out = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("flswh-aarch64-qnx7");
    if std::path::Path::new(src).exists() {
        fs::copy(src, out).expect("copy write head");
    } else {
        fs::write(out, []).expect("create empty write-head placeholder");
    }
    println!("cargo:rerun-if-changed={src}");
}
