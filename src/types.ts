export interface AssetChainTx {
    blockNumber: string;
    timeStamp: string;
    hash: string;
    nonce: string;
    blockHash: string;
    transactionIndex: string;
    from: string;
    to: string;
    value: string;
    gas: string;
    gasPrice: string;
    isError: string;
    txreceipt_status: string;
    input: string;
    contractAddress: string;
    cumulativeGasUsed: string;
    gasUsed: string;
    confirmations: string;
    methodId: string;
    functionName: string;
}

export interface AssetChainLog {
    address: string;
    topics: string[];
    data: string;
    blockNumber: string;
    timeStamp: string;
    gasPrice: string;
    gasUsed: string;
    logIndex: string;
    transactionHash: string;
    transactionIndex: string;
}

export interface DetailedTransaction {
    hash: string;
    blockNumber: number;
    timestamp: string; // Readable date
    from: string;
    to: string;
    value: string; // Formatted value if applicable
    methodId: string;
    status: string; // 'Success' or 'Failed'
    gasUsed: string;
    logs: AssetChainLog[];
}

// ... keep existing V1 types if needed for the list ...

export interface AssetChainTxV2 {
    hash: string;
    timestamp: string;
    block_number: number;
    status: string;
    method: string;
    from: {
        hash: string;
        is_contract: boolean;
        name?: string;
    };
    to: {
        hash: string;
        is_contract: boolean;
        name?: string;
    };
    value: string;
    gas_used: string;
    gas_price: string;
    fee: {
        value: string;
    };
    decoded_input?: {
        method_call: string;
        method_id: string;
        parameters: Array<{
            name: string;
            type: string;
            value: string;
        }>;
    };
    exchange_rate?: string;
}

export interface TokenTransferV2 {
    from: {
        hash: string;
        name?: string;
    };
    to: {
        hash: string;
        name?: string;
    };
    token: {
        address: string;
        name: string;
        symbol: string;
        decimals: string;
        exchange_rate?: string; // USD price per token
    };
    total: {
        value: string;
        decimals?: string;
    };
    type: string;
}

export interface TokenTransfersResponseV2 {
    items: TokenTransferV2[];
}

export interface FormattedTransaction {
    transaction_hash: string;
    cngn_amount: number | null;
    usd_value: number | null;
    trader_address: string;
    block_time: string;
    pool_address: string | null;
    pool_name: string | null;
    token_in_address: string | null;
    token_out_address: string | null;
    chain: string;
    dex: string | null;
    gas_used: string;
    gas_price: string;
    pool_liquidity_usd: number | null;
    cngn_reserves: number | null;
    other_token_reserves: number | null;
    liquidity_source: string | null;
    slot: number;
}
