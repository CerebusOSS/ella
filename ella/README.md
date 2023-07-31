# ella

[![Crates.io](https://img.shields.io/crates/v/ella?style=for-the-badge)](https://crates.io/crates/ella/)
[![docs.rs](https://img.shields.io/docsrs/ella?style=for-the-badge)](https://docs.rs/ella/)
![Crates.io](https://img.shields.io/crates/l/ella?style=for-the-badge)

<!-- cargo-rdme start -->

## Getting Started

`ella` requires a few additional setup steps after adding it as a dependency.

`ella` depends on the currently unstable UUIDv7 specification.
You will need to add `--cfg uuid_unstable` to your `RUSTFLAGS`.
The easiest way to do this is to update (or create) your `.cargo/config.toml` to include

```toml
[build]
rustflags = ["--cfg", "uuid_unstable"]
```

For your crate to build on platforms like `doc.rs` you'll also need to add the following to your `Cargo.toml` file:

```toml
[package.metadata.docs.rs]
rustc-args = ["--cfg", "uuid_unstable"]
rustdoc-args = ["--cfg", "uuid_unstable"]
```

## Usage

You can access ella by either starting a new instance or connecting to an existing instance.

Start a new instance by opening or creating a datastore:

```rust
let el = ella::open("file:///path/to/db")
    .or_create(ella::Config::default())
    .and_serve("localhost:50052")?
    .await?;

```

Connect to an existing instance using `ella::connect`:

```rust
let el = ella::connect("http://localhost:50052").await?;
```
