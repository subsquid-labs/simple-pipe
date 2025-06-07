import path from 'node:path';
import { PortalAbstractStream } from './core/portal_abstract_stream';
import { createClickhouseClient, ensureTables } from './clickhouse';
import { ClickhouseState } from './core/states/clickhouse_state';
import { createLogger } from './utils';

export type TransferAmount = {
  timestamp: Date;
  pre_balance: bigint;
};

export class TransferAmountsStream extends PortalAbstractStream<
  TransferAmount,
  { token: string; }
> {
  async stream(): Promise<ReadableStream<TransferAmount[]>> {
    const source = await this.getStream({
      type: 'solana',
      fields: {
        block: {
          // Although we're only using timestamp in the final data,
          // we also need slot number and block hash for progress tracking
          number: true,
          hash: true,
          timestamp: true,
        },
        tokenBalance: {
          preAmount: true,
        },
      },
      tokenBalances: [
        {
          preMint: [ this.options.args.token ]
        }
      ]
    });

    const stream = source.pipeThrough(
      new TransformStream({
        transform: ({ blocks }, controller) => {
          const res = blocks.flatMap((block: any) => {
            if (!block.tokenBalances) return [];

            const blockTimestamp = new Date(block.header.timestamp * 1000);

            return block.tokenBalances.map((tb) => ({
              timestamp: blockTimestamp,
              pre_balance: tb.preAmount,
            }));
          });

          controller.enqueue(res);
        },
      }),
    );

    return stream;
  }
}

const TRACKED_TOKEN = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'; // USDT

async function main() {
  const clickhouse = createClickhouseClient();
  const logger = createLogger('transfers');

  const ds = new TransferAmountsStream({
    portal: 'https://portal.sqd.dev/datasets/solana-mainnet',
    blockRange: {
      from: 317617480, // Jan 31, 2025
      // Check out
      // curl https://portal.sqd.dev/datasets/solana-mainnet/metadata
      // for up-to-date info on the earliest available block
    },
    args: {
      token: TRACKED_TOKEN,
    },
    // We use the indexer state to track the last block processed
    // and resume from there
    state: new ClickhouseState(clickhouse, {
      table: 'solana_sync_status',
      id: 'transfers',
    }),
    logger,
  });

  // Ensure tables are created in ClickHouse
  await ensureTables(clickhouse, path.join(__dirname, 'transfers.sql'));

  for await (const transfers of await ds.stream()) {
    await clickhouse.insert({
      table: 'transfers_raw',
      values: transfers,
      format: 'JSONEachRow',
    });

    await ds.ack();
  }
}

void main();
