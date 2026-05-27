#!/usr/bin/env python3
"""
AssetChain cNGN Pipeline
========================
Fetches all cNGN transfer events from Blockscout and produces:

  assetchain_cngn_master.csv  — one row per cNGN transfer event
  assetchain_cngn_swaps.csv   — one row per cNGN/USDT swap transaction

Event classification:
  MINT     — sender  is the zero address
  BURN     — receiver is the zero address
  SWAP     — same tx also contains a USDT transfer
  TRANSFER — everything else

Runs incrementally by default: reads the latest block from the existing
master CSV and only fetches transfers above that block.
"""

import asyncio
import aiohttp
import csv
import random
import time
from collections import defaultdict
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────

BLOCKSCOUT = "https://scan.assetchain.org/api/v2"
CHAIN      = "assetchain"
DEX        = "AssetChain Swap"

MASTER_CSV = "assetchain_cngn_master.csv"
SWAPS_CSV  = "assetchain_cngn_swaps.csv"

FORCE_FULL_FETCH = False  # set True to ignore existing data and re-fetch from scratch
FETCH_GAS        = False  # set True to populate gas columns (one extra API call per tx)

TOKEN_STREAMS = {
    "cNGN": "0x7923C0f6FA3d1BA6EAFCAedAaD93e737Fd22FC4F",
    "USDT": "0x26E490d30e73c36800788DC6d6315946C4BbEa24",
}

CNGN_ADDRESS = TOKEN_STREAMS["cNGN"].lower()
USDT_ADDRESS = TOKEN_STREAMS["USDT"].lower()
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

CONTRACT_NAMES: dict[str, str] = {
    "0x7923c0f6fa3d1ba6eafcaedaad93e737fd22fc4f": "cNGN Token",
    "0xe2a45a102b00fad6447d0ad859b43baf8bf6def1": "UniswapV3Pool (cNGN/USDT)",
    "0xa4dbc0e4f9bc2d9600568329506bdfef66edc108": "UniswapV3Pool (cNGN/USDC)",
    "0x26e490d30e73c36800788dc6d6315946c4bbea24": "USDT",
    "0x2b7c1342cc64add10b2a79c8f9767d2667de64b2": "USDC",
    "0x54527b09aeb2be23f99958db8f2f827dab863a28": "UniswapV3Router",
    "0xec2b2209d710d4283b5d1e29441df0dbb9cee5c3": "SwapRouter",
    "0x8804e26b04f52b0183ece80b797d1c1079956e56": "NonfungiblePositionManager",
}

POOL_PAIR_NAMES: dict[str, str] = {
    "0xe2a45a102b00fad6447d0ad859b43baf8bf6def1": "cNGN / USDT",
    "0xa4dbc0e4f9bc2d9600568329506bdfef66edc108": "cNGN / USDC",
}

SWAP_METHODS = frozenset({
    "exactinputsingle", "exactinput", "exactoutputsingle", "exactoutput",
    "multicall", "swap", "swapexacttokensfortokens", "swaptokensforexacttokens",
})

LIQUIDITY_METHODS = frozenset({
    "mint", "increaseliquidity", "decreaseliquidity",
    "collect", "burn", "collectprotocol",
})

CONCURRENCY  = 6
MAX_RETRIES  = 8

# ── Schemas ───────────────────────────────────────────────────────────────────

MASTER_HEADERS = [
    "chain",
    "block_time",
    "slot",
    "tx_hash",
    "evt_index",
    "sender",
    "receiver",
    "amount",
    "type",
    "method",
    "usd_value",
    "trader_address",
    "pool_address",
    "pool_name",
    "token_in_address",
    "token_out_address",
    "dex",
    "liquidity_source",
    "gas_used",
    "gas_price",
    "tx_fee_native",
]

SWAPS_HEADERS = [
    "transaction_hash",
    "cngn_amount",
    "usd_value",
    "trader_address",
    "block_time",
    "pool_address",
    "pool_name",
    "token_in_address",
    "token_out_address",
    "chain",
    "dex",
    "gas_used",
    "gas_price",
    "pool_liquidity_usd",
    "cngn_reserves",
    "other_token_reserves",
    "liquidity_source",
    "slot",
    "tx_fee_native",
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_amount(value: str | None, decimals: str | int) -> float:
    try:
        return int(value or 0) / (10 ** int(decimals or 6))
    except (ValueError, TypeError):
        return 0.0


def read_existing_max_block(csv_path: str) -> int:
    path = Path(csv_path)
    if not path.exists():
        return 0
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                return int(row.get("slot") or 0)
            except (ValueError, TypeError):
                pass
    return 0


def read_existing_rows(csv_path: str) -> list[dict]:
    path = Path(csv_path)
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

# ── HTTP ──────────────────────────────────────────────────────────────────────

async def fetch_page(
    session: aiohttp.ClientSession,
    url: str,
    params: dict,
    sem: asyncio.Semaphore,
) -> dict:
    delay = 1.5
    for attempt in range(MAX_RETRIES):
        try:
            async with sem:
                async with session.get(
                    url, params=params, timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 429:
                        raise aiohttp.ClientResponseError(
                            resp.request_info, resp.history, status=429
                        )
                    resp.raise_for_status()
                    return await resp.json()
        except aiohttp.ClientResponseError as exc:
            if exc.status == 429:
                await asyncio.sleep(delay + random.random())
                delay = min(delay * 2, 30)
                continue
            if attempt == MAX_RETRIES - 1:
                print(f"    [error] {exc}")
                return {}
            await asyncio.sleep(delay + random.random())
            delay = min(delay * 2, 30)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if attempt == MAX_RETRIES - 1:
                print(f"    [error] {exc}")
                return {}
            await asyncio.sleep(delay + random.random())
            delay = min(delay * 2, 30)
    return {}


async def fetch_all_transfers(
    session: aiohttp.ClientSession,
    token_name: str,
    token_address: str,
    sem: asyncio.Semaphore,
    stop_block: int = 0,
) -> list:
    url       = f"{BLOCKSCOUT}/tokens/{token_address}/transfers"
    all_items: list = []
    params:   dict  = {}
    page = 0

    while True:
        data  = await fetch_page(session, url, params, sem)
        items = data.get("items", [])
        if not items:
            break

        all_items.extend(items)
        page += 1

        if page % 50 == 0:
            print(f"  [{token_name}]  page {page:4d}  |  {len(all_items):,} transfers …")

        if stop_block > 0:
            page_min = min(int(i.get("block_number") or 0) for i in items)
            if page_min <= stop_block:
                print(f"  [{token_name}]  ✓ early stop page {page} (block {page_min:,} ≤ {stop_block:,})")
                break

        params = data.get("next_page_params") or {}
        if not params:
            break

    print(f"  [{token_name}]  ✓ done — {len(all_items):,} transfers ({page} pages)")
    return all_items


async def fetch_tx_gas(
    session: aiohttp.ClientSession,
    tx_hash: str,
    sem: asyncio.Semaphore,
) -> dict:
    data = await fetch_page(session, f"{BLOCKSCOUT}/transactions/{tx_hash}", {}, sem)
    return {
        "gas_used":      data.get("gas_used") or "",
        "gas_price":     data.get("gas_price") or "",
        "tx_fee_native": data.get("fee", {}).get("value") or "",
    }

# ── Processing ────────────────────────────────────────────────────────────────

def find_trader(transfers: list) -> str:
    counts: dict[str, int] = {}
    for t in transfers:
        for side in ("from", "to"):
            info = t.get(side) or {}
            if not info.get("is_contract") and info.get("hash"):
                addr = info["hash"].lower()
                counts[addr] = counts.get(addr, 0) + 1
    if not counts:
        return ((transfers[0].get("from") or {}).get("hash") or "").lower()
    return max(counts, key=counts.__getitem__)


def build_master_row(
    transfer: dict,
    tx_type: str,
    swap_info: dict | None,
    gas_info: dict | None,
) -> dict:
    raw_ts   = (transfer.get("timestamp") or "").replace("T", " ")[:19]
    sender   = ((transfer.get("from") or {}).get("hash") or "").lower()
    receiver = ((transfer.get("to")   or {}).get("hash") or "").lower()
    amount   = parse_amount(
        ((transfer.get("total") or {}).get("value") or "0"),
        (transfer.get("total") or {}).get("decimals") or 6,
    )
    gas = gas_info or {}

    row = {
        "chain":             CHAIN,
        "block_time":        raw_ts,
        "slot":              transfer.get("block_number", ""),
        "tx_hash":           transfer.get("transaction_hash", ""),
        "evt_index":         transfer.get("log_index", ""),
        "sender":            sender,
        "receiver":          receiver,
        "amount":            amount,
        "type":              tx_type.upper(),
        "method":            (transfer.get("method") or "").lower(),
        "gas_used":          gas.get("gas_used", ""),
        "gas_price":         gas.get("gas_price", ""),
        "tx_fee_native":     gas.get("tx_fee_native", ""),
        "usd_value":         "",
        "trader_address":    "",
        "pool_address":      "",
        "pool_name":         "",
        "token_in_address":  "",
        "token_out_address": "",
        "dex":               "",
        "liquidity_source":  "",
    }

    if swap_info:
        row.update(swap_info)

    return row


def process_swap_group(tx_hash: str, transfers: list) -> dict | None:
    """
    Build a swap row from all transfers in a single transaction.
    Returns None if the transaction is not a valid cNGN/USDT swap.
    """
    cngn_txs = [t for t in transfers if t["token"]["address"].lower() == CNGN_ADDRESS]
    usdt_txs = [t for t in transfers if t["token"]["address"].lower() == USDT_ADDRESS]

    if not cngn_txs or not usdt_txs:
        return None

    method = (transfers[0].get("method") or "").lower()
    if method in LIQUIDITY_METHODS:
        return None

    trader     = find_trader(transfers)
    raw_ts     = next((t.get("timestamp", "") for t in transfers if t.get("timestamp")), "")
    block_time = raw_ts.replace("T", " ")[:19]
    slot       = int(transfers[0].get("block_number") or 0)

    # Determine token_in / token_out from the trader's perspective
    token_in: dict | None  = None
    token_out: dict | None = None
    for t in transfers:
        from_addr = ((t.get("from") or {}).get("hash") or "").lower()
        to_addr   = ((t.get("to")   or {}).get("hash") or "").lower()
        sym = t["token"].get("symbol", "")
        dec = t["token"].get("decimals", 6)
        amt = parse_amount(((t.get("total") or {}).get("value") or "0"), dec)
        if from_addr == trader and token_in is None:
            token_in  = {"address": t["token"]["address"], "symbol": sym, "amount": amt}
        if to_addr == trader and token_out is None:
            token_out = {"address": t["token"]["address"], "symbol": sym, "amount": amt}

    cngn_amount = sum(
        parse_amount(
            ((t.get("total") or {}).get("value") or "0"),
            t["token"].get("decimals", 6),
        )
        for t in cngn_txs
    )

    st        = usdt_txs[0]
    usd_value = parse_amount(
        ((st.get("total") or {}).get("value") or "0"),
        st["token"].get("decimals", 6),
    )

    pool_address = pool_name = ""
    for t in cngn_txs:
        for side in ("from", "to"):
            info = t.get(side) or {}
            h    = (info.get("hash") or "").lower()
            name = CONTRACT_NAMES.get(h) or info.get("name") or ""
            if "pool" in name.lower() or "pair" in name.lower():
                pool_address = info.get("hash", "")
                pool_name    = POOL_PAIR_NAMES.get(h, name)
                break
        if pool_address:
            break

    swap_row = {
        "transaction_hash":     tx_hash,
        "cngn_amount":          cngn_amount,
        "usd_value":            usd_value,
        "trader_address":       trader,
        "block_time":           block_time,
        "pool_address":         pool_address,
        "pool_name":            pool_name,
        "token_in_address":     (token_in  or {}).get("address", ""),
        "token_out_address":    (token_out or {}).get("address", ""),
        "chain":                CHAIN,
        "dex":                  DEX,
        "gas_used":             "",
        "gas_price":            "",
        "pool_liquidity_usd":   "",
        "cngn_reserves":        "",
        "other_token_reserves": "",
        "liquidity_source":     "dex_swap",
        "slot":                 slot,
        "tx_fee_native":        "",
        # passed back to enrich the corresponding master rows
        "_swap_info": {
            "usd_value":         usd_value,
            "trader_address":    trader,
            "pool_address":      pool_address,
            "pool_name":         pool_name,
            "token_in_address":  (token_in  or {}).get("address", ""),
            "token_out_address": (token_out or {}).get("address", ""),
            "dex":               DEX,
            "liquidity_source":  "dex_swap",
        },
    }

    return swap_row

# ── Main ──────────────────────────────────────────────────────────────────────

async def run() -> None:
    t0  = time.monotonic()
    sem = asyncio.Semaphore(CONCURRENCY)

    print("=" * 62)
    print("  AssetChain cNGN Pipeline")
    print("=" * 62)

    if FORCE_FULL_FETCH:
        last_block = 0
        print("\n  Mode: FULL fetch")
    else:
        last_block = read_existing_max_block(MASTER_CSV)
        if last_block:
            print(f"\n  Mode: INCREMENTAL — resuming from block {last_block:,}")
        else:
            print("\n  Mode: FULL fetch (no existing data)")

    print("\nFetching token streams …\n")

    async with aiohttp.ClientSession() as session:
        cngn_transfers, usdt_transfers = await asyncio.gather(
            fetch_all_transfers(session, "cNGN", TOKEN_STREAMS["cNGN"], sem, stop_block=last_block),
            fetch_all_transfers(session, "USDT", TOKEN_STREAMS["USDT"], sem, stop_block=last_block),
        )

        if not cngn_transfers:
            print("\n  No new cNGN transfers — already up to date.")
            return

        cngn_min       = min(int(t.get("block_number") or 0) for t in cngn_transfers)
        usdt_transfers = [t for t in usdt_transfers if int(t.get("block_number") or 0) >= cngn_min]
        print(f"\n  cNGN range : {cngn_min:,} → {int(cngn_transfers[0].get('block_number') or 0):,}")
        print(f"  USDT in range : {len(usdt_transfers):,}")

        gas_map: dict[str, dict] = {}
        if FETCH_GAS:
            unique_hashes = {t.get("transaction_hash") for t in cngn_transfers if t.get("transaction_hash")}
            print(f"\n  Fetching gas for {len(unique_hashes):,} unique transactions …")
            gas_results = await asyncio.gather(*[fetch_tx_gas(session, h, sem) for h in unique_hashes])
            gas_map = dict(zip(unique_hashes, gas_results))

    fetch_time = time.monotonic() - t0
    print(f"\nFetched {len(cngn_transfers):,} cNGN + {len(usdt_transfers):,} USDT in {fetch_time:.1f}s")

    tx_groups: dict[str, list] = defaultdict(list)
    for t in cngn_transfers + usdt_transfers:
        h = t.get("transaction_hash")
        if h:
            tx_groups[h].append(t)

    # tx hashes that have a USDT leg — swap candidates
    usdt_tx_set = {t.get("transaction_hash") for t in usdt_transfers if t.get("transaction_hash")}

    print("\nBuilding master rows …")

    master_rows: list[dict] = []
    swap_rows:   list[dict] = []
    skipped = 0

    for transfer in cngn_transfers:
        tx_hash  = transfer.get("transaction_hash") or ""
        sender   = ((transfer.get("from") or {}).get("hash") or "").lower()
        receiver = ((transfer.get("to")   or {}).get("hash") or "").lower()
        method   = (transfer.get("method") or "").lower()

        # Liquidity events (add/remove) are not transfers — skip them,
        # but always keep genuine mints and burns (zero address).
        if method in LIQUIDITY_METHODS and sender != ZERO_ADDRESS and receiver != ZERO_ADDRESS:
            skipped += 1
            continue

        if sender == ZERO_ADDRESS:
            tx_type   = "MINT"
            swap_info = None
        elif receiver == ZERO_ADDRESS:
            tx_type   = "BURN"
            swap_info = None
        elif tx_hash in usdt_tx_set:
            tx_type  = "SWAP"
            swap_row = process_swap_group(tx_hash, tx_groups[tx_hash])
            if swap_row:
                swap_info = swap_row.pop("_swap_info", None)
                swap_rows.append(swap_row)
            else:
                swap_info = None
                tx_type   = "TRANSFER"
        else:
            tx_type   = "TRANSFER"
            swap_info = None

        gas_info = gas_map.get(tx_hash) if gas_map else None
        master_rows.append(build_master_row(transfer, tx_type, swap_info, gas_info))

    # A tx with multiple cNGN events produces one master row per event but only one swap row
    seen_swap_hashes: set[str] = set()
    deduped_swaps: list[dict]  = []
    for row in swap_rows:
        h = row["transaction_hash"]
        if h not in seen_swap_hashes:
            seen_swap_hashes.add(h)
            deduped_swaps.append(row)

    master_rows.sort(key=lambda r: int(r.get("slot") or 0), reverse=True)
    deduped_swaps.sort(key=lambda r: int(r.get("slot") or 0), reverse=True)

    print(f"  {len(master_rows):,} master rows  |  {len(deduped_swaps):,} swap rows  |  {skipped} skipped")

    existing_master = read_existing_rows(MASTER_CSV) if not FORCE_FULL_FETCH else []
    existing_swaps  = read_existing_rows(SWAPS_CSV)  if not FORCE_FULL_FETCH else []

    all_master = master_rows + existing_master
    all_swaps  = deduped_swaps + existing_swaps

    print(f"\nWriting {len(all_master):,} rows → {MASTER_CSV}")
    with open(MASTER_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MASTER_HEADERS, extrasaction="ignore", restval="")
        writer.writeheader()
        writer.writerows(all_master)

    print(f"Writing {len(all_swaps):,} rows → {SWAPS_CSV}")
    with open(SWAPS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SWAPS_HEADERS, extrasaction="ignore", restval="")
        writer.writeheader()
        writer.writerows(all_swaps)

    total_time = time.monotonic() - t0
    print()
    print("=" * 62)
    print(f"  Master  : {MASTER_CSV}  ({len(all_master):,} rows)")
    print(f"  Swaps   : {SWAPS_CSV}  ({len(all_swaps):,} rows)")
    print(f"  Runtime : {total_time:.1f}s")
    print("=" * 62)


if __name__ == "__main__":
    asyncio.run(run())
