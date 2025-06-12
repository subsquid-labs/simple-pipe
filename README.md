# simple-pipe

A small Solana data pipe powered by Soldexer. Gets the balances of USDT wallets that moved the token and computes hourly averages. Can be used to track the relative dominance of large vs small players chainwide.

See the [Soldexer pipes overview](https://docs.soldexer.dev/pipes/overview) as well as `src/main.ts` and `src/transfers.sql` for details.

This software is in early beta. Interfaces of the components involved can change without notice.

## Running the pipe

```bash
# Install dependencies
yarn install --frozen-lockfile

# Start ClickHouse
docker compose up -d

# Start the swaps indexer
yarn ts-node src/main.ts
```
You can now visit the local [ClickHouse console](http://localhost:8123/play) and observe the data that the pipe produces:
```sql
select * from transfers_raw;
```
```sql
select * from active_balance_stats;
```

## Related repositories

| Name | Description |
|------|-------------|
| [soldexer](https://github.com/subsquid-labs/soldexer) | The original Soldexer repo. Includes a pipe that grabs swaps from Solana's most popular DEx'es. |
| [solana-ingest](https://github.com/subsquid/squid-sdk/tree/master/solana/solana-ingest) | Extracts Solana data and uploads compressed chunks to S3. |
| [solana-data-service](https://github.com/subsquid/squid-sdk/tree/solana-data-service/solana/solana-data-service) | Streams live Solana blocks to Portals via RPC. |
| [sqd-portal](https://github.com/subsquid/sqd-portal) | Handles incoming queries and routes to workers or hotblocks. |
| [worker-rs](https://github.com/subsquid/worker-rs) | Decentralized worker that queries and serves data chunks from S3. |
