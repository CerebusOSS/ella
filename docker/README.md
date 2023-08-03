# ella Docker Image

The `ella`` Docker image runs a standalone ella server.
You can use the [CLI](../ella-cli/README.md) or the [Rust](../ella/README.md) or [Python](../pyella/README.md) bindings to connect to the server.

## Getting Started

```shell
docker run --rm \
    -p 50052:50052 \
    -v /path/to/datastore:/var/lib/ella \
    ghcr.io/cerebusoss/ella:latest
```
