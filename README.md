
# Asset Chain cNGN Data Pipeline

**Status**: Production Live (v4.0)  
**Data Coverage**: Complete History (Jan 2026)

## 1. The Mission

The objective was to create a **financial-grade index** of all cNGN activity on the Asset Chain. Since Asset Chain is not indexed by public analytics platforms (Dune), we built this custom indexer to feed reliable data directly into SQL-ready formats, enabling precise tracking of **Volume, Buying Pressure, Price, and Liquidity Utilization**.

---

## 2. Architecture & Solution

We built a custom ETL (Extract, Transform, Load) pipeline that converts raw blockchain logs into a clean financial ledger.

### A. The Extraction Engine (`src/index.ts`)
Instead of scraping one transaction at a time, we built a smart **"Delta-Fetcher"**:
1.  **Resume Capability**: The script loads existing transaction hashes into memory at startup.
2.  **Smart Sync**: It queries the Blockscout API only for *new* blocks (deduplication).
3.  **Single-Call Efficiency**: Uses `fetchTransactionDetailsV2` to capture Metadata + Internal Transfers in a single call, optimizing API throughput.

### B. The Enrichment Logic
We flatten nested JSON into a strict **Financial Schema**:
- **Trader Identification**: Separates the User (Initiator) from Routers/Contracts.
- **Token Flow**: Explicitly identifies `Token In` (Sold) and `Token Out` (Bought) relative to the user, correctly resolving complex multi-hop swaps (e.g., `RWM -> cNGN -> SHALOM`).

### C. Volume & Value Attribution (USD)
To accurately track volume without a historical Oracle, we use a **3-Tier Valuation Model**:
1.  **Tier 1 (Direct)**: If a trade involves **USDT/USDC**, we use the raw stablecoin value (Atomic Truth).
2.  **Tier 2 (Fallback)**: For obscure pairs, we apply a fallback derived rate.

### D. Price Discovery (`src/calculate_vwap.ts`)
We calculate the **Daily VWAP (Volume Weighted Average Price)** for charting:
- **Filter**: Strictly `SwapRouter` trades involving `USDT` or `USDC`.
- **Logic**: Filters out low-liquidity noise to find the true market price.

---

## 3. Usage

### Prerequisites
- Node.js v16+
- NPM

### Installation
```bash
npm install
npm run build
```

### 1. Run the Pipeline (Fetch New Data)
This will fetch only new transactions since the last run and append them to `assetchain_cngn_transactions_full.csv`.
```bash
npm start
```

### 2. Update VWAP History
Generates `cngn_vwap_history.csv` (Date, Price, Volume).
```bash
node dist/calculate_vwap.js
```

### 3. Query the Data (Local Analysis)
Run SQL-like queries (Group By, Count) on the local CSVs.
```bash
node dist/query_csv.js
```

---

## 4. Challenges & Evolution

To arrive at this architecture, we successfully solved several edge cases:

*   **Resilience (The 12k Gap)**: Implemented robust exponential backoff to handle API rate limits (429) without data loss.
*   **Liquidity vs Volume**: Added filters to exclude `NonfungiblePositionManager` events (Liquidity Adds/Removes) from being counted as Trading Volume.
*   **Routing Trades**: Adjusted logic to capture indirect volume (e.g., `RWM -> cNGN -> SHALOM`), recognizing cNGN's utility as a routing asset.

---

## 5. Output Files

*   `assetchain_cngn_transactions_full.csv`: The Master Ledger (~196k rows).
*   `assetchain_cngn_trades.csv`: Filtered DEX Swaps only.
*   `cngn_vwap_history.csv`: Daily Price/Volume time series.
