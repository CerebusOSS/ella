fn main() {
    #[cfg(not(target_os = "windows"))]
    std::env::set_var("PROTOC", protobuf_src::protoc());
    #[cfg(not(target_os = "windows"))]
    std::env::set_var("PROTOC_INCLUDE", protobuf_src::include());

    tonic_build::configure()
        .compile(&["api/engine.proto"], &["api/"])
        .unwrap();
}
