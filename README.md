# Replay

Replay is a Rust CLI tool that downloads an Eclipse snapshot, replays the next _N_ blocks **off‑chain**, and measures performance and state‑hash integrity.  It is geared towards regression‑testing new runtime optimisations **and towards fast catch‑up & verification workflows for full nodes**.

The project provides three CLI sub‑commands:

| Command | Purpose |
|---------|---------|
| `setup` | Record stage: download (or use a provided) snapshot, run an **Agave (sequential)** replay – blocks are processed in order while the entries inside each block execute in parallel – for *N* blocks, then persist the resulting accounts‑hash back into `config.json` so that future replays can validate it. |
| `replay` | Replay stage: run the actual replay with either the **Agave (sequential)** scheduler or the **concurrent DAG** scheduler, validating the final accounts‑hash against the value recorded by `setup`. |
| `download-single` | Downloads only the latest full snapshot for a network ‑ useful if you want to inspect the snapshot manually. |

---

## Table of Contents

- [Replay](#replay)
  - [Table of Contents](#table-of-contents)
  - [Quick start](#quick-start)
  - [CLI commands](#cli-commands)
    - [1. `replay`](#1-replay)
    - [2. `setup`](#2-setup)
    - [3. `download-single`](#3-download-single)
  - [`config.json` reference](#configjson-reference)
  - [Local testing](#local-testing)
  - [Docker metrics (optional)](#docker-metrics-optional)
  - [Sample results](#sample-results)
    - [Traffic simulation](#traffic-simulation)
    - [Agave (sequential) replay](#agave-sequential-replay)
    - [DAG concurrent replay](#dag-concurrent-replay)

---

## Quick start

```bash
# Clone the repo and build
$ git clone https://github.com/Eclipse-Laboratories-Inc/replay.git
$ cd replay
$ cargo build --release

# Run a simple Agave (sequential) replay against devnet for 50 blocks
$ cargo run -- replay -n devnet -b 50
```

---

## CLI commands

### 1. `replay`

Replay *N* blocks starting from the snapshot specified in `config.json`, verify PoH if requested, and optionally run with a concurrent DAG scheduler.

```bash
cargo run -- replay \
    -n <network>            # devnet | testnet | mainnet | localnet
    -b <NUM_BLOCKS>         # overrides config.json
    --scheduler-type <T>    # sequential (Agave) | concurrent (DAG) (default sequential)
    --end-hash <HASH>       # override expected final accounts hash
    --verify-poh            # enable PoH verification
    --batch-size <SIZE>     # RPC batch size (default 200)
```

Typical concurrent run against localnet:

```bash
cargo run -- replay -n localnet --scheduler-type concurrent
```

### 2. `setup`

Convenience wrapper that:

1. Downloads (or re‑uses) the latest snapshot for the selected network.
2. Runs an **Agave (sequential)** replay for *N* blocks (blocks in order, entries in parallel).
3. Stores the resulting `end_accounts_hash` and `num_blocks_between` back into `config.json`.

```bash
cargo run -- setup \
    -n <network> \
    -b <NUM_BLOCKS> \
    [--snapshot-file <FILE>]   # reuse local snapshot instead of downloading
```

After running `setup`, subsequent `replay` runs can use the concurrent scheduler and still validate state integrity.

### 3. `download-single`

Download the most recent full snapshot only:

```bash
cargo run -- download-single -n <network>
```

---

## `config.json` reference

`config.json` lives in the project root and looks like this (truncated):

```json
{
  "rpc_batch_size": 200,
  "rpc_max_retries": 20,
  "rpc_retry_delay_ms": 50,
  "enable_metrics": false,
  "networks": {
    "localnet": {
      "rpc_url": "http://127.0.0.1:8899",
      "start_snapshot": "snapshot-1600-….tar.zst",
      "genesis_folder": "localnet",
      "end_accounts_hash": "…",
      "num_blocks_between": 75
    }
  }
}
```

Key fields:

* `rpc_url` – endpoint of the network to pull blocks from.
* `start_snapshot` – filename (stored under `./cache/snapshots-<network>/`).  **Must** match the file you placed there or downloaded via `setup`/`download`.
* `genesis_folder` – path under `./genesis/` containing the network's `genesis.bin`.
* `end_accounts_hash` – recorded by `setup`; used by `replay` for integrity checks.
* `num_blocks_between` – number of blocks to process (if not supplied via `-b`).

---

## Local testing

The following notes describe a reproducible local workflow that combines **solar‑eclipse** (Eclipse validator), **traffic** (the AMM simulation), and **replay**.

> RnR notes
>
> * you need **3 GitHub repos** locally
>   * `solar-eclipse` `1.18.26` (production or `replay` branch)
>     * <https://github.com/Eclipse-Laboratories-Inc/solar-eclipse>
>   * `traffic` (`main` branch)
>     * <https://github.com/eclipse-Laboratories-Inc/traffic>
>   * `replay` (`master` branch)
>     * <https://github.com/Eclipse-Laboratories-Inc/replay>
> * **solar‑eclipse** – start a local validator:
>   ```bash
>   cargo run --bin solana-test-validator --release --features eclipse -- --limit-ledger-size 1000000000
>   # add --reset to wipe the ledger
>   ```
> * **traffic/minimal-amm** – build & deploy the on‑chain programs:
>   ```bash
>   RUSTUP_TOOLCHAIN="nightly-2024-11-19" anchor build && anchor deploy
>   ```
> * **traffic/simulation-runner** – configure and run the trading simulation:
>   ```bash
>   # edit default-config.json as desired
>   cargo run -- setup      # deploy contracts, create accounts, fund everything
>   cargo run -- run        # can be executed repeatedly
>   ```
>   Tips:
>   * Try to start the simulation close to a multiple‑of‑100 block height – the validator snapshots every 100 blocks.
>   * Keep the validator and the simulation‑runner logs side‑by‑side.
> * **replay** – copy artifacts and run:
>   ```bash
>   # Assuming the validator's ledger is in solar-eclipse/test-ledger/
>   cp solar-eclipse/test-ledger/genesis.bin genesis/localnet/genesis.bin
>   cp <SNAPSHOT_FILE> cache/snapshots-localnet/
> 
>   # Agave (sequential) pre‑run (records the hash in config.json)
>   cargo run -- setup -n localnet -b <NUM_BLOCKS> -s <SNAPSHOT_FILE>
>
>   # concurrent DAG replay
>   cargo run -- replay -n localnet --scheduler-type concurrent
>   ```
> * **docker** – optional metrics pipeline
>   * `docker-compose up` starts an InfluxDB instance.
>   * Set `enable_metrics` to `true` in `config.json` to emit runtime metrics.
>   * Point Grafana at the InfluxDB container for live dashboards.

---

## Docker metrics (optional)

`docker-compose.yml` spins up an **InfluxDB** container on port 8086.  When `enable_metrics` in `config.json` is `true`, replay will emit runtime metrics that can be consumed by Grafana or any other Influx client.

```bash
# Start services
$ docker-compose up -d

# Stop services
$ docker-compose down
```

---

## Sample results

### Traffic simulation
```
--- Simulation Results ---
Slots: 1629 to 1675
Total Attempts: 9516
Successful Swaps: 7166
Failed Swaps (Slippage): 2350
Failed Swaps (Other): 0
Overall Failure Rate: 24.70%
Slippage Failure Rate: 24.70%
Duration: 21.42s (46 slots)
TPS: 444.32
------------------------
```

### Agave (sequential) replay
```
------ Replay Results ------
Blocks Processed: 75
Scheduler Type: Sequential
------ Performance ------
Snapshot Load Time: 0.88s
Replay Time: 35.34s
--------------------------
```

### DAG concurrent replay
```
------ Replay Results ------
Blocks Processed: 75
Scheduler Type: Concurrent
------ Performance ------
Snapshot Load Time: 1.28s
Replay Time: 10.37s
--------------------------
```