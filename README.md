# Re-schedule

Re-schedule is a Rust CLI tool that downloads an Solana-compatible snapshot, a number _N_ of blocks and re-executes or simulates the execution of the available non-voting transactions under different scheduling strategies, measuring performance and various other metrics of interes.

The project provides three CLI sub‑commands:

| Command | Purpose |
|---------|---------|
| `reschedule` | run or simulate _N_ transactions under different transaction scheduling strategies such as **Sequential**, **Greedy(Agave)**, **RoundRobin** or **Bloom** |
| `download-transactions` | Downloads only the transactions from the latest _N_ number of blocks |
| `download-blocks` | Downloads only the latest _N_ number of blocks |
| `download-snapshot` | Downloads only the latest full snapshot for a network |
| `download-all` | Downloads the latest full snapshot for a network, and  |


---

## Table of Contents

- [Replay](#replay)
  - [Table of Contents](#table-of-contents)
  - [Quick start](#quick-start)
  - [CLI commands](#cli-commands)
    - [1. `reschedule`](#1-reschedule)
    - [2. `download-transactions`](#2-download-transactions)
    - [3. `download-blocks`](#2-download-blocks)
    - [4. `download-snapshot`](#3-download-snapshot)
    - [5. `download-all`](#3-download-all)
  - [`config.json` reference](#configjson-reference)
  - [Sample results](#sample-results)

---

## Quick start

```bash
# Clone the repo and build
$ git clone https://github.com/raresifrim/re-schedule.git
$ cd re-schedule
$ cargo build --release

# Download all txs from the last 10 blocks on the configured testnet
$ cargo run download-transactions -n testnet -b 10
# Simulate execution of 50 txs using the default Bloom scheduler strategy
$ cargo run reschedule -n testnet -t 50 --simulate
```

---

## CLI commands

### 1. `reschedule`

Re-execute or simulate execution of *NUM_TXS* transactions using a scheduling strategy specified by the scheduler-type parameter.

```bash
cargo run -- reschedule \
    -n <network>            # devnet | testnet | mainnet | localnet
    -t <NUM_TXS>            # number of txs to execute or simulate (overrides config.json)
    -s <T>                  # sequential | roundrobin | greedy | bloom (default) | bloomcounter
    -w <NUM_WORKERS>        # number of worker threads that execute txs (override config.json)
    --batch-size <SIZE>     # how many txs to schedule at once, if possible (overrides config.json)
    --simulate              # simulate tx execution following a normal distribution (optional)
    --compute-bloom-fpr     # compute and report the false positive rate of the BloomScheduler(optional)
```

Typical run of a GreedyScheduler against localnet for 50 txs:

```bash
cargo run -- reschedule -n localnet -s greedy -t 50
```

The command assumes that transactions and snapshot (if not simulating) are downloaded localy under `./cache/snapshots-<network>/`. You **must** run the download commands `download-snapshot`/`download-tranasctions`/`download-all` depending on the execution or simulation mode.

---

### 2. `download-transactions`

Download all transactions from the most recent *B* blocks from the specified network:

```bash
cargo run -- download-transactions -n <network> -b 10
```

The command will download transactions under base58 format, and store them under `./cache/snapshots-<network>/transactions.json`.
If running in simulation mode, where the snapshot is not needed, you can run this command first, and afterwards run the `reschedule` command in simulation mode.

---

### 3. `download-blocks`

Download most recent *B* blocks from the specified network:

```bash
cargo run -- download-blocks -n <network> -b 10
```

The command will download blocks, including transactions under base58 format, and store them under `./cache/snapshots-<network>/blocks.json`.

---

### 4. `download-snapshot`

Download the most recent full snapshot only:

```bash
cargo run -- download-snapshot -n <network>
```

The command will store the snapshot as an archive under `./cache/snapshots-<network>/`. The command also downloads the genesis binary of the network and store it under `./genesis/<network>/genesis.bin`.

---

### 5. `download-all`

Perform a complete download of the snapshot, a **B** number of blocks, and extract all transactions from the downloaded blocks:

```bash
cargo run -- download-all -n <network> -b 10
```

---

## `config.json` reference

`config.json` lives in the project root and looks like this (truncated):

```json
{
  "networks": {
    "testnet": {
      "batch_size": 64,
      "genesis_folder": "testnet",
      "num_blocks": 300,
      "num_txs": 418872,
      "num_workers": 4,
      "rpc_url": "https://api.mainnet-beta.solana.com",
      "start_snapshot": "snapshot-81020073-BTQQKRH28GiiP6te6J6bEm7xMogWg5SBN2zUR5LuKQxb.tar.zst"
    }
  }
}

```

Key fields:

* `rpc_url` – endpoint of the network to pull blocks from.
* `start_snapshot` – filename (stored under `./cache/snapshots-<network>/`).  **Must** match the file you placed there or downloaded via `download-snapshot`/`download-all`.
* `genesis_folder` – path under `./genesis/` containing the network's `genesis.bin`.
* `num_workers` – number of worker threads to execute or simulate transactions.
* `num_txs` – default number of transactions to execute/simulate; the `download` commands will update this value with the total number of transactions found in the downloaded blocks. 
* `num_blocks` – this is the number of blocks downloaded through one of the available `download` commands. 

---

## Sample results

The CLI tool also collects various types if metrics throughout the stages of the reschedulig, including transaction issuing, transaction scheduling and transaction scheduling. 

Example of metrics collected under the following sample command:

```bash
cargo run reschedule -n testnet -s greedy -t 512 -w 4 --batch-size 64 --simulate

{
  "scheduling": {
    "txs_per_worker": {
      "3": {
        "unique": 121,
        "total": 121,
        "retried": 0,
        "work_share": 23.6328125
      },
      "0": {
        "unique": 175,
        "total": 175,
        "retried": 0,
        "work_share": 34.1796875
      },
      "2": {
        "unique": 74,
        "total": 74,
        "retried": 0,
        "work_share": 14.453125
      },
      "1": {
        "unique": 142,
        "total": 142,
        "retried": 0,
        "work_share": 27.734375
      }
    },
    "useful_txs": 512,
    "total_txs": 512
  },
  "issuer": {
    "num_initial_txs": 512,
    "num_txs_executed": 512,
    "num_txs_retried": 0,
    "total_exec_time": 2.617427625,
    "useful_tx_throughput": 195.61190350010156,
    "raw_tx_throughput": 195.61190350010156
  },
  "executors": [
    {
      "execution_time_us": 2616990,
      "work_time_us": 2616672,
      "idle_time_us": 72,
      "retry_time_us": 0,
      "total_time_secs": 2.617203948999999,
      "real_saturation": 99.98784863526417,
      "raw_saturation": 99.98784863526417,
      "useful_workload_saturation": 100.0,
      "txs_received_per_sec": 1699161.0998912524
    },
    {
      "execution_time_us": 2076608,
      "work_time_us": 2076342,
      "idle_time_us": 112,
      "retry_time_us": 0,
      "total_time_secs": 2.0768670029999994,
      "real_saturation": 99.98719064936667,
      "raw_saturation": 99.98719064936667,
      "useful_workload_saturation": 100.0,
      "txs_received_per_sec": 990658.5088496514
    },
    {
      "execution_time_us": 1122723,
      "work_time_us": 1122553,
      "idle_time_us": 98,
      "retry_time_us": 0,
      "total_time_secs": 1.1229010390000003,
      "real_saturation": 99.98485824197064,
      "raw_saturation": 99.98485824197064,
      "useful_workload_saturation": 100.0,
      "txs_received_per_sec": 649841.0523912393
    },
    {
      "execution_time_us": 1717159,
      "work_time_us": 1716833,
      "idle_time_us": 103,
      "retry_time_us": 0,
      "total_time_secs": 1.7173962899999995,
      "real_saturation": 99.9810151535181,
      "raw_saturation": 99.9810151535181,
      "useful_workload_saturation": 100.0,
      "txs_received_per_sec": 907556.7222951433
    }
  ],
  "read_locks": [
    [
      "ComputeBudget111111111111111111111111111111",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 404,
        "num_write_access": 0
      }
    ],
    [
      "11111111111111111111111111111111",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 395,
        "num_write_access": 0
      }
    ],
    [
      "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 261,
        "num_write_access": 0
      }
    ],
    [
      "So11111111111111111111111111111111111111112",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 137,
        "num_write_access": 0
      }
    ],
    [
      "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 100,
        "num_write_access": 0
      }
    ],
    [
      "SysvarRent111111111111111111111111111111111",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 72,
        "num_write_access": 0
      }
    ],
    [
      "SysvarRecentB1ockHashes11111111111111111111",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 52,
        "num_write_access": 0
      }
    ],
    [
      "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 44,
        "num_write_access": 0
      }
    ],
    [
      "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 43,
        "num_write_access": 1
      }
    ],
    [
      "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 40,
        "num_write_access": 0
      }
    ]
  ],
  "write_locks": [
    [
      "oQPnhXAbLbMuKHESaGrbXT17CyvWCpLyERSJA9HCYd7",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 38
      }
    ],
    [
      "7MWYRCu4mBYPTDd4Hyg7D4Dz9fy6dNmzE4vCudgqFuQk",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 34
      }
    ],
    [
      "A8iY1bXHPZkoqj1cXVY4ZZTC2SWimxr5ghSahx8qNZG5",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 34
      }
    ],
    [
      "H8HjessbqxLm94zfBxrPQKkLoqhPtADWXogc4a2w7cyo",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 34
      }
    ],
    [
      "3E1RMb82Hzrin5YrurfSVHixeFw87jbxMeHT3CFAsdY6",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 34
      }
    ],
    [
      "773ZnqkimXtDpzyXan8AdC83wnULMfrpMhTvVXE7K9NA",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 34
      }
    ],
    [
      "7LwhjBDLibe1WPfPETKTHj36qiN9trcM162C6tYSmMqK",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 34
      }
    ],
    [
      "AP9aqPBJAcS4VcWSSTv7KddHwHAu8NMimDnMnrz4Tsmm",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 34
      }
    ],
    [
      "A6ATaE5FBknkDkaVDNJM7kAH1PUEGJkFpdcKTgLSkDLU",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 34
      }
    ],
    [
      "28ePwagFvTANxEPPJ9Yu8YD2MU7FYfRuSxeXciAGSxUb",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 34
      }
    ]
  ]
}
Recomputed Bulk Execution Summaries[
  {
    "execution_time_us": 2616990,
    "work_time_us": 2616672,
    "idle_time_us": 72,
    "retry_time_us": 0,
    "total_time_secs": 2.617203948999999,
    "real_saturation": 99.98784863526417,
    "raw_saturation": 99.98784863526417,
    "useful_workload_saturation": 100.0,
    "txs_received_per_sec": 1699161.0998912524
  },
  {
    "execution_time_us": 2616990,
    "work_time_us": 2076342,
    "idle_time_us": 540494,
    "retry_time_us": 0,
    "total_time_secs": 2.617203948999999,
    "real_saturation": 79.34084578083981,
    "raw_saturation": 79.34084578083981,
    "useful_workload_saturation": 100.0,
    "txs_received_per_sec": 990658.5088496514
  },
  {
    "execution_time_us": 2616990,
    "work_time_us": 1122553,
    "idle_time_us": 1494365,
    "retry_time_us": 0,
    "total_time_secs": 2.617203948999999,
    "real_saturation": 42.89481427135755,
    "raw_saturation": 42.89481427135755,
    "useful_workload_saturation": 100.0,
    "txs_received_per_sec": 649841.0523912393
  },
  {
    "execution_time_us": 2616990,
    "work_time_us": 1716833,
    "idle_time_us": 899934,
    "retry_time_us": 0,
    "total_time_secs": 2.617203948999999,
    "real_saturation": 65.60334582860462,
    "raw_saturation": 65.60334582860462,
    "useful_workload_saturation": 100.0,
    "txs_received_per_sec": 907556.7222951433
  }
]

```

If running the BloomScheduler, you can also report the false positive rate of the current bloom filter configurations (only available for the `bloom` scheduler-type, as `bloomcounter` will have same rate under the same configuration). This metric also reports a `multiple_conflict_rate` percentage, which shows how many accounts (out of all the accounts of all the transactions executed/simulated) are conflicting (common) between transactions.

```bash
cargo run reschedule -n testnet -s bloom -t 512 -w 4 --batch-size 64 --simulate --compute-bloom-fpr

BloomScheduler: {
 "false_positive_rate": 0, 
 "multiple_conflict_rate": 7.348880024547408, 
}

{
  "scheduling": {
    "txs_per_worker": {
      "1": {
        "unique": 190,
        "total": 197,
        "retried": 7,
        "work_share": 37.09981167608286
      },
      "3": {
        "unique": 52,
        "total": 55,
        "retried": 3,
        "work_share": 10.357815442561206
      },
      "0": {
        "unique": 204,
        "total": 211,
        "retried": 7,
        "work_share": 39.73634651600753
      },
      "2": {
        "unique": 66,
        "total": 68,
        "retried": 2,
        "work_share": 12.8060263653484
      }
    },
    "useful_txs": 512,
    "total_txs": 531
  },
  "issuer": {
    "num_initial_txs": 512,
    "num_txs_executed": 531,
    "num_txs_retried": 19,
    "total_exec_time": 2.95917525,
    "useful_tx_throughput": 173.02118216891682,
    "raw_tx_throughput": 179.44189010096648
  },
  "executors": [
    {
      "execution_time_us": 2957256,
      "work_time_us": 2852723,
      "idle_time_us": 1746,
      "retry_time_us": 104061,
      "total_time_secs": 2.9590664609999995,
      "real_saturation": 96.46520287726189,
      "raw_saturation": 99.98403925801486,
      "useful_workload_saturation": 96.48060189719642,
      "txs_received_per_sec": 123244.42406595679
    },
    {
      "execution_time_us": 2832477,
      "work_time_us": 2726530,
      "idle_time_us": 1688,
      "retry_time_us": 105421,
      "total_time_secs": 2.8342453649999992,
      "real_saturation": 96.25956362575936,
      "raw_saturation": 99.98142968151198,
      "useful_workload_saturation": 96.27744265349224,
      "txs_received_per_sec": 118699.01064073412
    },
    {
      "execution_time_us": 1023571,
      "work_time_us": 990033,
      "idle_time_us": 1677,
      "retry_time_us": 33356,
      "total_time_secs": 1.025288957,
      "real_saturation": 96.7234319846889,
      "raw_saturation": 99.98221911328086,
      "useful_workload_saturation": 96.74063332711216,
      "txs_received_per_sec": 40643.43336598762
    },
    {
      "execution_time_us": 733374,
      "work_time_us": 675470,
      "idle_time_us": 1673,
      "retry_time_us": 57633,
      "total_time_secs": 0.735095374,
      "real_saturation": 92.10443784481043,
      "raw_saturation": 99.96304750372934,
      "useful_workload_saturation": 92.13848531516035,
      "txs_received_per_sec": 32982.65771857162
    }
  ],
  "read_locks": [
    [
      "ComputeBudget111111111111111111111111111111",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 404,
        "num_write_access": 0
      }
    ],
    [
      "11111111111111111111111111111111",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 395,
        "num_write_access": 0
      }
    ],
    [
      "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 261,
        "num_write_access": 0
      }
    ],
    [
      "So11111111111111111111111111111111111111112",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 137,
        "num_write_access": 0
      }
    ],
    [
      "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 100,
        "num_write_access": 0
      }
    ],
    [
      "SysvarRent111111111111111111111111111111111",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 72,
        "num_write_access": 0
      }
    ],
    [
      "SysvarRecentB1ockHashes11111111111111111111",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 52,
        "num_write_access": 0
      }
    ],
    [
      "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 44,
        "num_write_access": 0
      }
    ],
    [
      "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 43,
        "num_write_access": 1
      }
    ],
    [
      "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 40,
        "num_write_access": 0
      }
    ]
  ],
  "write_locks": [
    [
      "oQPnhXAbLbMuKHESaGrbXT17CyvWCpLyERSJA9HCYd7",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 38
      }
    ],
    [
      "FpwhKwcucVqGeCbXU5DkwJAtWeFMPz24q9WsPnMWEfMf",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 34
      }
    ],
    [
      "A8iY1bXHPZkoqj1cXVY4ZZTC2SWimxr5ghSahx8qNZG5",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 34
      }
    ],
    [
      "7MWYRCu4mBYPTDd4Hyg7D4Dz9fy6dNmzE4vCudgqFuQk",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 34
      }
    ],
    [
      "773ZnqkimXtDpzyXan8AdC83wnULMfrpMhTvVXE7K9NA",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 34
      }
    ],
    [
      "2ETSEqF2Rs5fzNwBFAfAJfkD5vreRGvEY76V5dmXJYAq",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 34
      }
    ],
    [
      "7LwhjBDLibe1WPfPETKTHj36qiN9trcM162C6tYSmMqK",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 34
      }
    ],
    [
      "28ePwagFvTANxEPPJ9Yu8YD2MU7FYfRuSxeXciAGSxUb",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 34
      }
    ],
    [
      "A6ATaE5FBknkDkaVDNJM7kAH1PUEGJkFpdcKTgLSkDLU",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 34
      }
    ],
    [
      "H8HjessbqxLm94zfBxrPQKkLoqhPtADWXogc4a2w7cyo",
      {
        "lock_type": 0,
        "lock_counter": 0,
        "num_read_access": 0,
        "num_write_access": 34
      }
    ]
  ]
}
Recomputed Bulk Execution Summaries[
  {
    "execution_time_us": 2957256,
    "work_time_us": 2852723,
    "idle_time_us": 1746,
    "retry_time_us": 104061,
    "total_time_secs": 2.9590664609999995,
    "real_saturation": 96.46520287726189,
    "raw_saturation": 99.98403925801486,
    "useful_workload_saturation": 96.48060189719642,
    "txs_received_per_sec": 123244.42406595679
  },
  {
    "execution_time_us": 2957256,
    "work_time_us": 2726530,
    "idle_time_us": 126467,
    "retry_time_us": 105421,
    "total_time_secs": 2.9590664609999995,
    "real_saturation": 92.19797001003633,
    "raw_saturation": 95.76279496939054,
    "useful_workload_saturation": 96.27744265349224,
    "txs_received_per_sec": 118699.01064073412
  },
  {
    "execution_time_us": 2957256,
    "work_time_us": 990033,
    "idle_time_us": 1935362,
    "retry_time_us": 33356,
    "total_time_secs": 2.9590664609999995,
    "real_saturation": 33.47809591053328,
    "raw_saturation": 34.60603343099143,
    "useful_workload_saturation": 96.74063332711216,
    "txs_received_per_sec": 40643.43336598762
  },
  {
    "execution_time_us": 2957256,
    "work_time_us": 675470,
    "idle_time_us": 2225555,
    "retry_time_us": 57633,
    "total_time_secs": 2.9590664609999995,
    "real_saturation": 22.841106755722194,
    "raw_saturation": 24.789974219343875,
    "useful_workload_saturation": 92.13848531516035,
    "txs_received_per_sec": 32982.65771857162
  }
]

```