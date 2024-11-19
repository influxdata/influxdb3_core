//! Compiles Protocol Buffers into native Rust types.
//! <https://github.com/grpc/grpc/blob/master/doc/binary-logging.md>

use std::env;
use std::io::Result;
use std::path::PathBuf;

fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto");
    let proto_files = vec![
        root.join("grpc/binlog/v1/binarylog.proto"),
    ];
    // distributions which obtain `google/protobuf/{duration,timestamp}.proto` from rpm package:
    // `protobuf-devel` (eg: centos, fedora, etc), require a `--proto_path=/usr/include` protoc arg
    let protobuf_devel_include_path = PathBuf::from("/usr/include");
    let includes = vec![
        root,
        protobuf_devel_include_path,
    ];
    tonic_build::configure().compile(&proto_files, &includes)
}
