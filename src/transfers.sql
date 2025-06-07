CREATE TABLE IF NOT EXISTS transfers_raw
(
    account             String,
    block_number        UInt32 CODEC (DoubleDelta, ZSTD),
    transaction_index   UInt16,
    amount              UInt256
) ENGINE = MergeTree()
    ORDER BY (block_number, transaction_index);
