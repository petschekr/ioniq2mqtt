fn main() {
    capnpc::CompilerCommand::new()
        .import_path("cereal")
        .src_prefix("cereal")
        .file("cereal/log.capnp")
        .file("cereal/car.capnp")
        .file("cereal/deprecated.capnp")
        .file("cereal/custom.capnp")
        .run().expect("schema compiler command");
}
