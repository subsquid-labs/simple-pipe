CREATE TABLE IF NOT EXISTS transfers_raw
(
    account             String,
    block_number        UInt32 CODEC (DoubleDelta, ZSTD),
    block_hash          String,
    amount              UInt256,
    token_mint          String,
    token_decimals      UInt16,
--    transaction_index   UInt16,
    sign                Int8
) ENGINE = CollapsingMergeTree(sign)
      ORDER BY (block_number);
