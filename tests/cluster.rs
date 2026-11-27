use std::path::Path;

#[test]
fn local_cluster_write_read_and_redirect() {
    quorum::bench::smoke(Path::new(env!("CARGO_BIN_EXE_quorum"))).unwrap();
}
