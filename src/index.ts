
import { fetchTokenTransfersList, fetchTransactionDetailsV2 } from './api';
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

// NEW Expanded Schema Headers matching assetchain_cngn_transactions_full.csv
const CSV_HEADERS = [
    'transaction_hash',
    'block_number',
    'timestamp',
    'status',
    'trader_address',
    'interacted_contract_address',
    'interacted_contract_name',
    'transaction_method',
    'token_in_address',
    'token_in_symbol',
    'token_in_amount',
    'token_out_address',
    'token_out_symbol',
    'token_out_amount',
    'cngn_amount',
    'usd_value',
    'pool_address',
    'pool_name',
    'transaction_fee_native',
    'gas_used',
    'gas_price'
];

export async function processSingleTransaction(hash: string): Promise<FormattedTransaction[]> {
    // 1. Fetch Details (Includes Transfers in 'token_transfers' array if present in V2)
    const details = await fetchTransactionDetailsV2(hash);

    if (!details) {
        return [];
    }

    // Adapt to structure: details.token_transfers could be an array OR nested in details.token_transfers.items
    let items: any[] = [];
    if (Array.isArray(details.token_transfers)) {
        items = details.token_transfers;
    } else if (details.token_transfers && Array.isArray((details.token_transfers as any).items)) {
        items = (details.token_transfers as any).items;
    }

    // Filter to ensure relevance?
    // The previous logic was: "Identify ALL cNGN Transfers".
    // The new logic (full dataset) was: "Process the TX if it contains cNGN".
    const hasCngn = items.some((i: any) => i.token.address.toLowerCase() === CNGN_ADDRESS.toLowerCase());

    if (!hasCngn) {
        return [];
    }

    // Logic to flatten the transaction into 1 row (aggregating cNGN if multiple?)
    // In process_full_dataset, we did one row per TX hash.

    const traderAddress = details.from.hash;

    // Contract Info
    let interactedAddress = '';
    let interactedName = '';
    const CONTRACT_NAMES: { [key: string]: string } = {
        '0x7923c0f6fa3d1ba6eafcaedaad93e737fd22fc4f': 'cNGN Token',
        '0xe2a45a102b00fad6447d0ad859b43baf8bf6def1': 'UniswapV3Pool (cNGN/USDT)',
        '0x26e490d30e73c36800788dc6d6315946c4bbea24': 'USDT',
        '0x54527b09aeb2be23f99958db8f2f827dab863a28': 'UniswapV3Router',
        '0xec2b2209d710d4283b5d1e29441df0dbb9cee5c3': 'SwapRouter',
        '0x8804e26b04f52b0183ece80b797d1c1079956e56': 'NonfungiblePositionManager'
    };

    if (details.to) {
        interactedAddress = details.to.hash;
        interactedName = details.to.name || CONTRACT_NAMES[interactedAddress.toLowerCase()] || '';
        if (!interactedName && details.to.is_contract) {
            interactedName = 'Unknown Contract';
        }
    }

    // Parse Transfers for In/Out/Amounts
    let tokenInAddr = '';
    let tokenInSym = '';
    let tokenInAmt = 0;
    let tokenOutAddr = '';
    let tokenOutSym = '';
    let tokenOutAmt = 0;
    let cngnAmt = 0;
    let usdValue = 0;

    let poolAddress = '';
    let poolName = '';

    for (const item of items) {
        const isCngn = item.token.address.toLowerCase() === CNGN_ADDRESS.toLowerCase();

        let val = 0;
        if (item.total && item.total.value) {
            const decimals = parseInt(item.token.decimals || '18');
            const rawVal = BigInt(item.total.value);
            const str = rawVal.toString();
            if (str.length > decimals) {
                val = parseFloat(str.slice(0, str.length - decimals) + '.' + str.slice(str.length - decimals));
            } else {
                val = parseFloat('0.' + str.padStart(decimals, '0'));
            }
        }

        if (isCngn) {
            cngnAmt += val;
            if (item.to.name && item.to.name.includes('Pool')) {
                poolAddress = item.to.hash; poolName = item.to.name;
            } else if (item.from.name && item.from.name.includes('Pool')) {
                poolAddress = item.from.hash; poolName = item.from.name;
            }
        }

        if (item.from.hash.toLowerCase() === traderAddress.toLowerCase()) {
            tokenInAddr = item.token.address; tokenInSym = item.token.symbol; tokenInAmt = val;
        } else if (item.to.hash.toLowerCase() === traderAddress.toLowerCase()) {
            tokenOutAddr = item.token.address; tokenOutSym = item.token.symbol; tokenOutAmt = val;
        }

        if (item.token.symbol === 'USDT' || item.token.symbol === 'USDC') usdValue = val;
    }

    if (usdValue === 0 && cngnAmt > 0) {
        usdValue = cngnAmt * CNGN_USD_FALLBACK;
    }

    let feeNative = '0';
    if (details.fee && details.fee.value) feeNative = details.fee.value;

    // Return SINGLE result per transaction hash
    // (Note: Type definition needs update to match this return, or we use 'any' temporarily if types.ts isn't updated?
    // We already updated types.ts somewhat, but let's check matches)

    const result: any = {
        transaction_hash: hash,
        block_number: details.block_number,
        timestamp: formatBlockTime(details.timestamp),
        status: details.status,
        trader_address: traderAddress,
        interacted_contract_address: interactedAddress,
        interacted_contract_name: interactedName,
        transaction_method: details.method || '',
        token_in_address: tokenInAddr,
        token_in_symbol: tokenInSym,
        token_in_amount: tokenInAmt,
        token_out_address: tokenOutAddr,
        token_out_symbol: tokenOutSym,
        token_out_amount: tokenOutAmt,
        cngn_amount: cngnAmt,
        usd_value: usdValue,
        pool_address: poolAddress,
        pool_name: poolName,
        transaction_fee_native: feeNative,
        gas_used: details.gas_used,
        gas_price: details.gas_price
    };

    return [result];
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
    const TARGET_COUNT = 250000;
    const PROCESS_BATCH_SIZE = 1000;
    const OUTPUT_FILE = 'assetchain_cngn_transactions_full.csv';

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
            console.log('\nNo new transactions FOUND in the last scanned batch... Continuing to scan in case intermixed?');
            // If we are strictly scanning "newest first" (likely API default?) then once we see duplicates we might be done?
            // BUT fetchTokenTransfersList might be in random order or oldest first?
            // Actually, if we see 0 new hashes after scanning 1000 items, and we assume API is ordered consistently (e.g. by time), 
            // then we might be done or we might be at the "old" start.
            // Let's rely on nextPageParams to eventually finish.
            // WARNING: If we don't break, this could scan all 170k old records every time.
            // Optimally: If we scan X pages and find 0 new items, we stop.

            // For this specific request, the user assumes "latest" are missing. If API returns newest first, we would find them immediately. 
            // If API returns oldest first, we have to scan ALL old ones to get to the new ones at the end.

            // Let's assume we continue.
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
