fn main() {
    std::env::set_var("PROTOC", protobuf_src::protoc());
    std::env::set_var("PROTOC_INCLUDE", protobuf_src::include());
    tonic_build::configure()
        .compile(&["api/engine.proto"], &["api/"])
        .unwrap();
}
