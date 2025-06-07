CREATE TABLE IF NOT EXISTS transfers_raw
(
    timestamp           DateTime CODEC (DoubleDelta, ZSTD),
    pre_balance         UInt256
) ENGINE = MergeTree()
    ORDER BY (timestamp);
