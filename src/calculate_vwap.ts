
import * as fs from 'fs';
import * as readline from 'readline';
import * as path from 'path';

// Correct Input File Name
const CSV_PATH = path.join(__dirname, '../assetchain_cngn_transactions_full.csv');
const OUTPUT_FILE = path.join(__dirname, '../cngn_vwap_history.csv');

// Interfaces
interface DailyStat {
    totalUsd: number;
    totalCngn: number;
}

interface VwapRecord {
    date: string;
    vwap: number;
    volumeCngn: number;
    volumeUsd: number;
}

/**
 * Helper to parse "YYYY-MM-DD HH:MM" into "YYYY-MM-DD"
 */
function parseDate(dateStr: string): string {
    if (!dateStr) return '';
    return dateStr.split(' ')[0];
}

/**
 * CSV Line Splitter that handles quoted values correctly.
 */
function splitCsvLine(line: string): string[] {
    const result: string[] = [];
    let current = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
        const char = line[i];
        if (char === '"') {
            if (inQuotes && line[i + 1] === '"') {
                current += '"';
                i++;
            } else {
                inQuotes = !inQuotes;
            }
        } else if (char === ',' && !inQuotes) {
            result.push(current);
            current = '';
        } else {
            current += char;
        }
    }
    result.push(current);
    return result;
}

async function calculateVWAP() {
    console.log('Initialize VWAP calculation...');
    console.log(`Reading from: ${CSV_PATH}`);

    if (!fs.existsSync(CSV_PATH)) {
        console.error(`Error: Data file not found at ${CSV_PATH}`);
        process.exit(1);
    }

    const fileStream = fs.createReadStream(CSV_PATH);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    const dailyStats = new Map<string, DailyStat>();

    let isFirstLine = true;
    let minDate = '';
    let maxDate = '';

    console.log('Processing transactions...');

    // Columns in assetchain_cngn_transactions_full.csv:
    // 0: transaction_hash
    // 1: block_number
    // 2: timestamp (YYYY-MM-DD HH:MM)
    // 3: status
    // 4: trader_address
    // 9: token_in_symbol
    // 12: token_out_symbol
    // 14: cngn_amount
    // 15: usd_value
    // 17: pool_name

    await new Promise<void>((resolve, reject) => {
        rl.on('line', (line) => {
            if (isFirstLine) {
                isFirstLine = false;
                return;
            }

            if (!line || line.trim() === '') return;

            const parts = splitCsvLine(line);

            if (parts.length < 18) return; // Ensure strictly enough columns

            const dateStr = parts[2];
            const contractName = parts[6];
            const tokenInSym = parts[9];
            const tokenOutSym = parts[12];
            const cngnRaw = parts[14];
            const usdRaw = parts[15];
            const poolName = parts[17];

            // Strict Filter:
            // 1. MUST be an interaction with the SwapRouter (Defines it as a Trade)
            // 2. MUST involve USDT or USDC (To get reliable Price)

            const isSwapRouter = contractName === 'SwapRouter';

            const isStablecoinTrade = (
                tokenInSym === 'USDT' || tokenInSym === 'USDC' ||
                tokenOutSym === 'USDT' || tokenOutSym === 'USDC'
            );

            // Also allow if pool name explicitly says USDT/USDC (backup)
            const isPoolMatch = poolName && (poolName.includes('USDT') || poolName.includes('USDC'));

            // COMBINED CHECK
            if (isSwapRouter && (isStablecoinTrade || isPoolMatch)) {
                const dateKey = parseDate(dateStr);
                const cngnAmount = parseFloat(cngnRaw);
                const usdValue = parseFloat(usdRaw);

                if (isNaN(cngnAmount) || isNaN(usdValue)) return;

                // Exclude very small dust
                if (cngnAmount <= 0.000001 || usdValue <= 0.000001) return;

                if (!dailyStats.has(dateKey)) {
                    dailyStats.set(dateKey, { totalUsd: 0, totalCngn: 0 });
                }

                const stat = dailyStats.get(dateKey)!;
                stat.totalCngn += cngnAmount;
                stat.totalUsd += usdValue;

                // Track total range
                if (minDate === '' || dateKey < minDate) minDate = dateKey;
                if (maxDate === '' || dateKey > maxDate) maxDate = dateKey;
            }
        });

        rl.on('close', () => resolve());
        rl.on('error', (err) => reject(err));
    });

    if (minDate === '') {
        console.log('No relevant USDT/USDC trades found in dataset.');
        return;
    }

    console.log(`Processing complete. Range: ${minDate} to ${maxDate}`);

    // Generate consecutive daily records
    const results: VwapRecord[] = [];
    const currentDate = new Date(minDate);
    const endDate = new Date(maxDate);
    // Safety break for infinite loops if dates parsed weirdly
    const SAFETY_YEAR_LIMIT = new Date('2030-01-01');

    let lastKnownVwap = 0;

    // First pass: Find first valid price
    // (If day 1 has 0 volume, we need a seed price. But logic handles this by defaulting to 0 then updating)

    while (currentDate <= endDate && currentDate < SAFETY_YEAR_LIMIT) {
        const dateKey = currentDate.toISOString().split('T')[0];

        let dailyVwap = 0;
        let dailyVolumeCngn = 0;
        let dailyVolumeUsd = 0;

        if (dailyStats.has(dateKey)) {
            const stat = dailyStats.get(dateKey)!;

            if (stat.totalCngn > 0) {
                dailyVwap = stat.totalUsd / stat.totalCngn;
                dailyVolumeCngn = stat.totalCngn;
                dailyVolumeUsd = stat.totalUsd;
                lastKnownVwap = dailyVwap;
            } else {
                dailyVwap = lastKnownVwap;
            }
        } else {
            dailyVwap = lastKnownVwap;
        }

        results.push({
            date: dateKey,
            vwap: dailyVwap,
            volumeCngn: dailyVolumeCngn,
            volumeUsd: dailyVolumeUsd
        });

        currentDate.setDate(currentDate.getDate() + 1);
    }

    // Write output CSV
    const csvHeader = 'date,vwap_price_usd,volume_cngn,volume_usd\n';
    const csvRows = results.map(r =>
        `${r.date},${r.vwap.toFixed(8)},${r.volumeCngn.toFixed(6)},${r.volumeUsd.toFixed(6)}`
    ).join('\n');

    fs.writeFileSync(OUTPUT_FILE, csvHeader + csvRows);

    console.log(`Success! VWAP history written to: ${OUTPUT_FILE}`);
}

calculateVWAP().catch((err) => {
    console.error('Fatal execution error:', err);
    process.exit(1);
});
