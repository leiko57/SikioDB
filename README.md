# SikioDB

![NPM Version](https://img.shields.io/npm/v/sikiodb?style=flat-square&color=blue) ![License](https://img.shields.io/github/license/seiko1337/SikioDB?style=flat-square) ![Rust](https://img.shields.io/badge/built_with-Rust-orange?style=flat-square) ![WASM](https://img.shields.io/badge/target-WASM-purple?style=flat-square) ![Size](https://img.shields.io/bundlephobia/minzip/sikiodb?style=flat-square&label=minzipped)

A blazing-fast, local-first database for the web. Built with Rust, WebAssembly, and OPFS.

## Performance

| Records | IndexedDB | SikioDB | Speedup |
|---------|-----------|---------|---------|
| 10,000 | 220 ms | 81 ms | **2.70x** |
| 100,000 | 2,339 ms | 972 ms | **2.41x** |
| 1,000,000 | 31,759 ms | 10,761 ms | **2.95x** |

*Benchmarks: 100 runs average (10K/100K), 5 runs average (1M)*

## Features

- **OPFS Storage** — Direct file system access, no IndexedDB overhead
- **LZ4 Compression** — Automatic data compression
- **B-Tree Index** — Efficient key-value lookups
- **WAL** — Write-ahead logging for durability
- **ACID Transactions** — Atomic commits with rollback
- **Multi-Tab Sync** — Leader election via Web Locks API
- **Query Builder** — SQL-like filtering, sorting, pagination
- **TTL Support** — Auto-expiring keys
- **Subscriptions** — Real-time change notifications

## Installation

```bash
npm i sikiodb
```

or

```bash
yarn add sikiodb
```


## Building from Source

```bash
cargo install wasm-pack
wasm-pack build --target web --release
```

## License

This project is licensed under AGPLv3.

If you want to use SikioDB in a proprietary (closed-source) commercial project and cannot comply with AGPL requirements, please contact me at **keiko1337@proton.me** for a commercial license.

