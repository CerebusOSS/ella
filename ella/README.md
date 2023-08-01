# ella

[![Crates.io](https://img.shields.io/crates/v/ella?style=for-the-badge)](https://crates.io/crates/ella/)
[![docs.rs](https://img.shields.io/docsrs/ella?style=for-the-badge)](https://docs.rs/ella/)
![Crates.io](https://img.shields.io/crates/l/ella?style=for-the-badge)

<!-- cargo-rdme start -->

## Getting Started

Add `ella` to your `Cargo.toml` file:

```toml
ella = "0.1.3"
```

You may also want to install the [ella CLI tools](https://crates.io/crates/ella-cli/).

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

<!-- cargo-rdme end -->
