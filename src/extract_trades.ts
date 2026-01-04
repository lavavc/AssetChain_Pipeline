
import * as fs from 'fs';
import * as readline from 'readline';
import * as path from 'path';

const INPUT_FILE = path.join(__dirname, '../assetchain_cngn_transactions_full.csv');
const OUTPUT_FILE = path.join(__dirname, '../assetchain_cngn_trades.csv');

const TARGET_ADDRESS = '0xec2b2209d710d4283b5d1e29441df0dbb9cee5c3'; // SwapRouter
const TARGET_NAME = 'SwapRouter';

async function extractTrades() {
    console.log(`Extracting TRADES from ${INPUT_FILE}...`);
    console.log(`Criteria: Address=${TARGET_ADDRESS}, Name=${TARGET_NAME}`);

    const readStream = fs.createReadStream(INPUT_FILE);
    const rl = readline.createInterface({ input: readStream, crlfDelay: Infinity });
    const writeStream = fs.createWriteStream(OUTPUT_FILE);

    let headers: string[] = [];
    let count = 0;

    writeStream.on('error', (err) => console.error('Write Error:', err));

    await new Promise<void>((resolve, reject) => {
        rl.on('line', (line) => {
            // Robust CSV Split
            const parts: string[] = [];
            let current = '';
            let inQuote = false;
            for (let i = 0; i < line.length; i++) {
                const char = line[i];
                if (char === '"') {
                    if (inQuote && line[i + 1] === '"') { current += '"'; i++; }
                    else { inQuote = !inQuote; }
                } else if (char === ',' && !inQuote) {
                    parts.push(current); current = '';
                } else {
                    current += char;
                }
            }
            parts.push(current);

            if (headers.length === 0) {
                headers = parts;
                writeStream.write(line + '\n'); // Write Header
                return;
            }

            // Map keys
            const row: any = {};
            headers.forEach((h, i) => row[h] = parts[i] || '');

            // FILTER CRITERIA
            const addressParams = (row['interacted_contract_address'] || '').toLowerCase();
            const nameParams = (row['interacted_contract_name'] || '');

            if (addressParams === TARGET_ADDRESS && nameParams === TARGET_NAME) {
                writeStream.write(line + '\n');
                count++;
            }
        });

        rl.on('close', resolve);
        rl.on('error', reject);
    });

    console.log(`\nSuccess! Extracted ${count} trades to:`);
    console.log(OUTPUT_FILE);
}

extractTrades().catch(console.error);
