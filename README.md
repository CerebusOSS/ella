# ella: Embedded Low-Latency Datastore

[![Crates.io](https://img.shields.io/crates/v/ella?style=for-the-badge)](https://crates.io/crates/ella/)
[![docs.rs](https://img.shields.io/docsrs/ella?style=for-the-badge)](https://docs.rs/ella/)
[![GitHub Workflow Status (with event)](https://img.shields.io/github/actions/workflow/status/CerebusOSS/ella/rust.yaml?style=for-the-badge)](https://github.com/CerebusOSS/ella/actions/workflows/rust.yaml)
![Crates.io](https://img.shields.io/crates/l/ella?style=for-the-badge)


**ella is in extremely early development. There is very little documentation and many parts of it do not work. Expect that any part of the API may change in the future.**

ella is a streaming time-series datastore designed for:

- Low (<1 ms) end-to-end latency.
- First-class multidimensional tensor support.
- Hybrid embedded/client-server deployment.

ella is **not**:

- An ACID database.
- A replacement for Delta Lake, Snowflake, or any other cloud data service.

## Usage

### Rust

*[See the `ella` crate](ella/)*

### Python

*[See the `pyella` package](pyella/)*

### CLI

*[See the `ella-cli` crate](ella-cli/)*

### Docker

*[See the Docker guide](docker/README.md)*


## Use Cases

The goal of ella is to simplify the storage and analysis of data in systems that require low-latency data access.

A typical workflow for such a system might be something like:
1. Data is ingested from sensors.
2. The data is sent to streaming consumers using a low-latency API (e.g. gRPC, LSL).
3. The data is written to persistent storage such as a SQL database or an HDF5 file.
4. Batch consumers read data from persistent storage using a separate API.

```mermaid
---
title: Typical Workflow
---
flowchart LR
    pub1[Data Source]
    sub1[Streaming Consumer]
    sub2[Batch Consumer]
    api1[[Streaming API]]
    api2[[Batch API]]
    store[(Storage)]

    pub1 --> api1
    api1 --> sub1
    pub1 --> api2
    api2 --> sub2
    api2 <-.-> store
```

In contrast, ella provides a unified API for both streaming and batch processing:

```mermaid
---
title: Unified Workflow
---
flowchart LR
    pub1[Data Source]
    sub1[Streaming Consumer]
    sub2[Batch Consumer]

    subgraph ella[ella]
        direction TB
        table1[[Table]]
        store[(Storage)]
        api[[API]]
    end

    pub1 ---> api
    table1 <--> api
    table1 <-.-> store
    api ---> sub2
    api ---> sub1
```

For example, consider the following queries:

```sql
-- Get all rows from the table
SELECT time,x,y FROM sensor

-- Only return new rows published after this query is executed
SELECT time,x,y FROM sensor WHERE time > now()

-- Return existing rows, but ignore any additional rows published after the query is executed
SELECT time,x,y FROM sensor WHERE time < now()
```

## Concepts
### Tables

Data in ella is grouped into *tables*. Each table is either a *topic* or a *view*.

- **Topics:** collect rows of data written by publishers. Topics are stored to disk by default, but *temporary* topics are not.
- **Views:** return the result of specific queries. By default a view is re-computed each time it's scanned, but views can also be *materialized* to disk. Views are read-only.

### Organization

ella follows the *Catalog → Schema → Table* organizational model.

The default catalog is `"ella"` and the default schema is `"public"`.

### Columns and Indices

Ella is a time-series datastore, and all topics have a timestamp as their first column (named `"time"` by default but can be renamed) and primary index.

Views are not required to have a time column.
