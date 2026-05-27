# AssetChain cNGN Pipeline

A Python pipeline that indexes all cNGN token activity on Asset Chain and produces a structured dataset for analytics.

Asset Chain is not covered by Dune Analytics or any other public indexing platform. This pipeline queries the Blockscout v2 REST API, fetches the cNGN and USDT transfer streams in parallel, and joins them by transaction hash to classify every event.

## Output

| File | Description |
|------|-------------|
| `assetchain_cngn_master.csv` | One row per cNGN transfer event |
| `assetchain_cngn_swaps.csv` | One row per cNGN/USDT swap transaction |

## How it works

Two token transfer streams are fetched concurrently — cNGN and USDT — then joined in memory by `transaction_hash`. Each cNGN event is classified into one of four types:

| Type | Condition |
|------|-----------|
| `MINT` | Sender is the zero address |
| `BURN` | Receiver is the zero address |
| `SWAP` | Same transaction contains a USDT transfer |
| `TRANSFER` | Everything else |

**Incremental by default.** On each run the pipeline reads the highest block number from the existing master CSV and only fetches transfers above that block, then prepends the new rows. Set `FORCE_FULL_FETCH = True` to re-fetch all history from scratch.

## Setup

Requires Python 3.11+.

```bash
python3 -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Usage

```bash
python3 pipeline.py
```

## Output schema

### `assetchain_cngn_master.csv`

| Column | Type | Description |
|--------|------|-------------|
| `chain` | string | Always `assetchain` |
| `block_time` | string | `YYYY-MM-DD HH:MM:SS` |
| `slot` | integer | Block number |
| `tx_hash` | string | Transaction hash |
| `evt_index` | integer | Log index of this transfer event within the transaction |
| `sender` | string | From address |
| `receiver` | string | To address |
| `amount` | float | cNGN amount (6 decimals) |
| `type` | string | `MINT` / `BURN` / `SWAP` / `TRANSFER` |
| `method` | string | Contract method name (e.g. `multicall`, `transfer`) |
| `usd_value` | float | USDT value from the paired leg *(swaps only)* |
| `trader_address` | string | EOA that initiated the trade *(swaps only)* |
| `pool_address` | string | DEX pool address *(swaps only)* |
| `pool_name` | string | Pool label *(swaps only)* |
| `token_in_address` | string | Token sold by the trader *(swaps only)* |
| `token_out_address` | string | Token bought by the trader *(swaps only)* |
| `dex` | string | Always `AssetChain Swap` *(swaps only)* |
| `liquidity_source` | string | Always `dex_swap` *(swaps only)* |
| `gas_used` | string | Empty unless `FETCH_GAS = True` |
| `gas_price` | string | Empty unless `FETCH_GAS = True` |
| `tx_fee_native` | string | Empty unless `FETCH_GAS = True` |

### `assetchain_cngn_swaps.csv`

One row per swap transaction. Matches the multi-chain Dune swaps schema column order.

| Column | Description |
|--------|-------------|
| `transaction_hash` | Transaction hash |
| `cngn_amount` | Total cNGN moved in this swap |
| `usd_value` | USDT value |
| `trader_address` | EOA that initiated the trade |
| `block_time` | `YYYY-MM-DD HH:MM:SS` |
| `pool_address` | DEX pool address |
| `pool_name` | Pool label |
| `token_in_address` | Token sold |
| `token_out_address` | Token bought |
| `chain` | Always `assetchain` |
| `dex` | Always `AssetChain Swap` |
| `slot` | Block number |

## Configuration

All settings are at the top of `pipeline.py`.

| Variable | Default | Description |
|----------|---------|-------------|
| `FORCE_FULL_FETCH` | `False` | Re-fetch all history from block 0 |
| `FETCH_GAS` | `False` | Populate gas columns (adds one API call per unique tx) |
| `CONCURRENCY` | `6` | Max parallel API requests |
| `TOKEN_STREAMS` | — | cNGN and USDT contract addresses |
