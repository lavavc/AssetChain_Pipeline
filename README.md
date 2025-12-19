# Asset Chain Token Pipeline

This project is a robust data pipeline designed to scrape, process, and enrich transfer history for **any token** on the **Asset Chain** network (Blockscout API).

## Features

*   **Complete History Extraction:** Scrapes the entire history of token transfers from the Blockscout API.
*   **Enriched Data:** Calculates USD values for every transaction using on-chain stablecoin reference prices (e.g., USDT) or fallback exchange rates.
*   **Robust Error Handling:** Handles API rate limits (429) and server errors (500) with intelligent exponential backoff and retry mechanisms.
*   **Resumable:** Automatically resumes from the last saved transaction if stopped, preventing data duplicates.
*   **High Performance:** Uses batched collection and concurrent processing bursts to maximize throughput while respecting strict API limits.
*   **Universal Support:** Can be configured to track any ERC-20 token on the network.

## Prerequisites

*   Node.js (v14 or higher)
*   npm

## Setup

1.  Clone the repository:
    ```bash
    git clone <repository-url>
    cd assetchain-pipeline
    ```

2.  Install dependencies:
    ```bash
    npm install
    ```

## Configuration

To track a different token, update the target contract address in `src/config.ts`.

## Usage

Start the pipeline:

```bash
npm start
```

The script will:
1.  Check for existing records in `assetchain_cngn_token_transfers.csv`.
2.  Scan the API for new transactions.
3.  Process and append new distinct transfers to the CSV file.

## Output

The output is a CSV file containing columns for:
*   Transaction Hash
*   Token Amount
*   USD Value
*   Trader Address
*   Block Time
*   Pool/Dex Information
*   Gas Usage
