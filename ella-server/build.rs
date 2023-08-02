fn main() {
    #[cfg(all(feature = "protobuf", not(target_os = "windows")))]
    std::env::set_var("PROTOC", protobuf_src::protoc());
    #[cfg(all(feature = "protobuf", not(target_os = "windows")))]
    std::env::set_var("PROTOC_INCLUDE", protobuf_src::include());

    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["api/engine.proto"], &["api/"])
        .unwrap();
}
