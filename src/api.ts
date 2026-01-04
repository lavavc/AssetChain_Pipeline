
import axios from 'axios';
import { CONFIG } from './config';
import { AssetChainTx, AssetChainLog, AssetChainTxV2, TokenTransfersResponseV2 } from './types';

const SLEEP_MS = 200; // Rate limiting buffer
const REQUEST_TIMEOUT = 30000; // 30 seconds timeout (Increased for stability)

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

export async function fetchLastTransactions(limit: number = 10): Promise<AssetChainTx[]> {
    try {
        const response = await axios.get(CONFIG.API_BASE_URL, {
            params: {
                module: 'account',
                action: 'txlist',
                address: CONFIG.TARGET_ADDRESS,
                page: 1,
                offset: limit,
                sort: 'desc'
            },
            timeout: REQUEST_TIMEOUT
        });

        if (response.data.status === '1') {
            return response.data.result;
        } else {
            console.error('Error fetching transactions:', response.data.message);
            return [];
        }
    } catch (error) {
        console.error('Network error fetching transactions:', error);
        return [];
    }
}

// V2 APIs
export async function fetchTransactionDetailsV2(hash: string): Promise<AssetChainTxV2 | null> {
    await sleep(SLEEP_MS);
    try {
        const url = `https://scan.assetchain.org/api/v2/transactions/${hash}`;
        const response = await axios.get(url, { timeout: REQUEST_TIMEOUT });
        return response.data;
    } catch (error) {
        // console.error(`Error fetching V2 details for ${hash}:`, error); // reduced logs
        return null;
    }
}




interface TokenTransferListResponse {
    items: any[];
    next_page_params: any;
}

export async function fetchTokenTransfersList(limit: number = 50, nextPageParams?: any): Promise<TokenTransferListResponse> {
    let retries = 15; // High retry count for resilience
    let delay = 2000;

    while (retries > 0) {
        try {
            const url = `https://scan.assetchain.org/api/v2/tokens/${CONFIG.CNGN_CONTRACT_ADDRESS}/transfers`;
            const params: any = {
                items_count: limit
            };

            if (nextPageParams) {
                Object.assign(params, nextPageParams);
            }

            const response = await axios.get(url, { params, timeout: REQUEST_TIMEOUT });

            return {
                items: response.data.items || [],
                next_page_params: response.data.next_page_params
            };
        } catch (error: any) {
            if (error.code === 'ECONNABORTED') {
                console.warn(`Timeout fetching transfer list. Retrying...`);
            } else if (error.response && error.response.status === 429) {
                console.warn(`Rate limited (429) fetching transfer list. Retrying in ${delay / 1000}s...`);
            } else {
                console.error('Error fetching token transfers list:', error.message);
                return { items: [], next_page_params: null };
            }

            await sleep(delay);
            delay *= 2; // Exponential backoff
            retries--;
        }
    }
    console.error('Max retries exceeded for fetching token transfers list.');
    return { items: [], next_page_params: null };
}
