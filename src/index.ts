
import { fetchTokenTransfersList, fetchTransactionDetailsV2, fetchTokenTransfersV2 } from './api';
import { FormattedTransaction } from './types';
import * as fs from 'fs';
import * as readline from 'readline';

// Helper to format date like the example: 2025-08-05 10:04
function formatBlockTime(isoDate: string): string {
    if (!isoDate) return '';
    return isoDate.replace('T', ' ').substring(0, 16);
}

const CNGN_USD_FALLBACK = 0.000657; // Approx 1/1522 derived from user example
const CNGN_ADDRESS = '0x7923C0f6FA3d1BA6EAFCAedAaD93e737Fd22FC4F'.toLowerCase();

// CSV Escaping Helper
function escapeCsv(value: any): string {
    if (value === null || value === undefined) return '';
    const stringValue = String(value);
    if (stringValue.includes(',') || stringValue.includes('"') || stringValue.includes('\n')) {
        return `"${stringValue.replace(/"/g, '""')}"`;
    }
    return stringValue;
}

const CSV_HEADERS = [
    'transaction_hash', 'cngn_amount', 'usd_value', 'trader_address', 'block_time',
    'pool_address', 'pool_name', 'token_in_address', 'token_out_address',
    'chain', 'dex', 'gas_used', 'gas_price', 'pool_liquidity_usd',
    'cngn_reserves', 'other_token_reserves', 'liquidity_source', 'slot'
];

export async function processSingleTransaction(hash: string): Promise<FormattedTransaction[]> {
    // 1. Fetch Details & Transfers
    const details = await fetchTransactionDetailsV2(hash);
    const transfersResponse = await fetchTokenTransfersV2(hash);

    if (!details || !transfersResponse || !transfersResponse.items) {
        return [];
    }

    const items = transfersResponse.items;
    const results: FormattedTransaction[] = [];

    // 2. Identify ALL cNGN Transfers in this transaction
    const cngnItems = items.filter((i: any) => i.token.address.toLowerCase() === CNGN_ADDRESS.toLowerCase());

    if (cngnItems.length === 0) {
        return [];
    }

    // Process EACH cNGN transfer found in the transaction separately
    for (const cngnItem of cngnItems) {

        // 3. Identify User and Pool (Context relative to THIS transfer)
        let poolAddress: string | null = null;
        let poolNameVal: string | null = null;
        let traderAddress: string | null = null;

        if (cngnItem.to.name && (cngnItem.to.name.includes('Pool') || cngnItem.to.name.includes('Uniswap'))) {
            poolAddress = cngnItem.to.hash;
            poolNameVal = cngnItem.to.name;
            traderAddress = cngnItem.from.hash;
        } else if (cngnItem.from.name && (cngnItem.from.name.includes('Pool') || cngnItem.from.name.includes('Uniswap'))) {
            poolAddress = cngnItem.from.hash;
            poolNameVal = cngnItem.from.name;
            traderAddress = cngnItem.to.hash;
        } else {
            traderAddress = cngnItem.from.hash;
            poolAddress = cngnItem.to.hash;
        }

        // 4. Determine Token In / Token Out & Other Token based on main trader
        let tokenIn: string | null = null;
        let tokenOut: string | null = null;
        let otherTokenSymbol = '';

        items.forEach((item: any) => {
            if (item.from.hash.toLowerCase() === traderAddress?.toLowerCase()) {
                tokenIn = item.token.address;
            }
            if (item.to.hash.toLowerCase() === traderAddress?.toLowerCase()) {
                tokenOut = item.token.address;
            }
            if (item.token.address.toLowerCase() !== CNGN_ADDRESS.toLowerCase()) {
                otherTokenSymbol = item.token.symbol;
            }
        });

        // 5. Build Pool Name
        let displayPoolName = poolNameVal;
        if (otherTokenSymbol) {
            displayPoolName = `cNGN / ${otherTokenSymbol}`;
        }

        // 6. Calculate Values
        let cngnVal: number | null = null;
        if (cngnItem.total && cngnItem.total.value) {
            const decimals = parseInt(cngnItem.token.decimals || '6');
            const rawVal = BigInt(cngnItem.total.value);
            let s = rawVal.toString();
            while (s.length <= decimals) s = '0' + s;
            const integerPart = s.slice(0, s.length - decimals);
            let fractionalPart = s.slice(s.length - decimals);
            fractionalPart = fractionalPart.replace(/0+$/, '');
            cngnVal = parseFloat(fractionalPart.length > 0 ? `${integerPart}.${fractionalPart}` : integerPart);
        }

        let usdVal: number | null = null;

        // Try to derive USD value from USDT transfer if present
        const usdtItem = items.find((i: any) => i.token.symbol === 'USDT');
        if (usdtItem && usdtItem.total && usdtItem.total.value) {
            const decimals = parseInt(usdtItem.total.decimals || usdtItem.token.decimals || '6');
            const rawVal = BigInt(usdtItem.total.value);
            let s = rawVal.toString();
            while (s.length <= decimals) s = '0' + s;
            const integerPart = s.slice(0, s.length - decimals);
            let fractionalPart = s.slice(s.length - decimals);
            fractionalPart = fractionalPart.replace(/0+$/, '');
            usdVal = parseFloat(fractionalPart.length > 0 ? `${integerPart}.${fractionalPart}` : integerPart);
        }

        // Fallback if no USDT found
        if (usdVal === null) {
            let rate = CNGN_USD_FALLBACK;
            if (cngnItem.token.exchange_rate) rate = parseFloat(cngnItem.token.exchange_rate);
            usdVal = (cngnVal || 0) * rate;
        }

        results.push({
            transaction_hash: hash,
            cngn_amount: cngnVal,
            usd_value: usdVal,
            trader_address: traderAddress || '',
            block_time: formatBlockTime(details.timestamp),
            pool_address: poolAddress || '',
            pool_name: displayPoolName,
            token_in_address: tokenIn,
            token_out_address: tokenOut,
            chain: 'assetchain',
            dex: 'Uniswap',
            gas_used: details.gas_used || '0',
            gas_price: details.gas_price || '0',
            pool_liquidity_usd: null,
            cngn_reserves: null,
            other_token_reserves: null,
            liquidity_source: 'dex_swap',
            slot: details.block_number
        });
    }

    return results;
}

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

async function loadExistingHashes(filePath: string): Promise<Set<string>> {
    const hashes = new Set<string>();
    if (!fs.existsSync(filePath)) return hashes;

    console.log(`Reading existing records from ${filePath}...`);

    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let isFirstLine = true;

    await new Promise<void>((resolve, reject) => {
        rl.on('line', (line) => {
            if (isFirstLine) {
                isFirstLine = false; // Skip header
                return;
            }
            // Assuming hash is the first column
            const parts = line.split(',');
            if (parts.length > 0 && parts[0]) {
                // Remove quotes if present
                const hash = parts[0].replace(/^"|"$/g, '');
                if (hash.startsWith('0x')) {
                    hashes.add(hash);
                }
            }
        });

        rl.on('close', () => {
            resolve();
        });

        rl.on('error', (err) => {
            reject(err);
        });
    });

    console.log(`Loaded ${hashes.size} unique existing hashes.`);
    return hashes;
}

async function pipelineBatch() {
    const TARGET_COUNT = 180000;
    const PROCESS_BATCH_SIZE = 1000;
    const OUTPUT_FILE = 'transactions.csv';

    // Check if file exists, if not write headers
    if (!fs.existsSync(OUTPUT_FILE)) {
        fs.writeFileSync(OUTPUT_FILE, CSV_HEADERS.join(',') + '\n');
    }

    const seenHashes = await loadExistingHashes(OUTPUT_FILE);
    let recordsSaved = seenHashes.size;
    let nextPageParams: any = undefined;

    console.log(`Starting pipeline for target ${TARGET_COUNT} records/transfers...`);

    let scannedCount = 0; // Tracking scanned items

    while (recordsSaved < TARGET_COUNT) {
        // Accumulate hashes until we reach the batch size
        const currentBatchHashes: string[] = [];
        const fetchSize = 1000;

        console.log(`Collecting ${PROCESS_BATCH_SIZE} transactions...`);

        while (currentBatchHashes.length < PROCESS_BATCH_SIZE) {
            const listResponse = await fetchTokenTransfersList(fetchSize, nextPageParams);
            const items = listResponse.items;

            let newHashes = 0;
            if (items && items.length > 0) {
                items.forEach((item: any) => {
                    const h = item.transaction_hash;

                    if (h && !seenHashes.has(h)) {
                        seenHashes.add(h);
                        currentBatchHashes.push(h);
                        newHashes++;
                    }
                });

                scannedCount += items.length;
                if (newHashes === 0) {
                    // Log progress if we are just scanning
                    process.stdout.write(`\r[Scanning] Skipped ${items.length} known... (Total Scanned: ${scannedCount})`);
                } else {
                    console.log(`\nFound ${newHashes} new transactions in this batch.`);
                }

            } else {
                console.log('\nNo more items found from API.');
                break;
            }

            if (listResponse.next_page_params) {
                nextPageParams = listResponse.next_page_params;
            } else {
                console.log('\nNo next page params. End of history.');
                break;
            }
        }

        if (currentBatchHashes.length === 0) {
            console.log('\nNo new transactions found to process. Stopping.');
            break;
        }

        console.log(`\nCollected ${currentBatchHashes.length} unique new transactions. Processing in high-concurrency bursts...`);

        // Process the collected batch in strictly limited concurrency chunks to avoid 500/429 death
        const EXECUTION_CONCURRENCY = 50;

        for (let i = 0; i < currentBatchHashes.length; i += EXECUTION_CONCURRENCY) {
            const chunk = currentBatchHashes.slice(i, i + EXECUTION_CONCURRENCY);

            const promises = chunk.map(async (hash) => {
                let retries = 3;
                while (retries > 0) {
                    try {
                        return await processSingleTransaction(hash);
                    } catch (e: any) {
                        if (e.message && e.message.includes('429')) {
                            await sleep(2000 + Math.random() * 1000); // Jittered backoff
                            retries--;
                        } else if (e.message && e.message.includes('500')) {
                            await sleep(3000); // Longer wait for server error
                            retries--;
                        } else {
                            // console.error(`Failed ${hash}: ${e.message}`);
                            return null;
                        }
                    }
                }
                return null; // Return null (which is filtered out)
            });

            const results = await Promise.all(promises);
            // Flatten the array of arrays
            const validResults = results.reduce<FormattedTransaction[]>((acc, val) => {
                if (val) {
                    return acc.concat(val);
                }
                return acc;
            }, []);

            if (validResults.length > 0) {
                const csvLines = validResults.map(record => {
                    return CSV_HEADERS.map(header => escapeCsv((record as any)[header])).join(',');
                });

                fs.appendFileSync(OUTPUT_FILE, csvLines.join('\n') + '\n');

                recordsSaved += validResults.length;
            }
            // Minimal breathing room for the API
            await sleep(50);

            // Progress update every 200 items or so
            if ((i + chunk.length) % 200 === 0 || (i + chunk.length) >= currentBatchHashes.length) {
                console.log(`Progress: ${recordsSaved} / ${TARGET_COUNT} transfers saved.`);
            }
        }

        if (recordsSaved >= TARGET_COUNT) {
            console.log(`Target ${TARGET_COUNT} reached.`);
            break;
        }

        // If we broke out of collecting because of no next page, break main loop too
        if (!nextPageParams && currentBatchHashes.length < PROCESS_BATCH_SIZE) {
            // We processed what we had, now check if we are truly done
            if (recordsSaved >= TARGET_COUNT) break;
            // If we ran out of pages, we stop
            break;
        }
    }
}

async function main() {
    try {
        await pipelineBatch();
    } catch (error) {
        console.error('Pipeline failed:', error);
    }
}

main();
