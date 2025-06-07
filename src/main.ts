import path from 'node:path';
import { PortalAbstractStream } from './core/portal_abstract_stream';
import { createClickhouseClient, ensureTables } from './clickhouse';
import { ClickhouseState } from './core/states/clickhouse_state';
import { createLogger } from './utils';

export type Transfer = {
  account: string;
  block_number: bigint;
  transaction_index: number;
  amount: bigint;
};

export class TransfersStream extends PortalAbstractStream<
  Transfer,
  { token: string; }
> {
  async stream(): Promise<ReadableStream<Transfer[]>> {
    const source = await this.getStream({
      type: 'solana',
      fields: {
        block: {
          number: true,
          // Hash and timestamp are not used in the final data,
          // but are required for progress tracking.o
          hash: true,
          timestamp: true,
        },
        tokenBalance: {
          transactionIndex: true,
          account: true,
          postAmount: true,
        },
      },
      tokenBalances: [
        {
          postMint: [ this.options.args.token ]
        }
      ]
    });

    const stream = source.pipeThrough(
      new TransformStream({
        transform: ({ blocks }, controller) => {
          const res = blocks.flatMap((block: any) => {
            if (!block.tokenBalances) return [];

            const transfers: Transfer[] = [];

            for (const tb of block.tokenBalances) {
              transfers.push({
                account: tb.account,
                block_number: block.header.number,
                transaction_index: tb.transactionIndex,
                amount: tb.postAmount,
              });
            }

            return transfers;
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

  const ds = new TransfersStream({
    portal: 'https://portal.sqd.dev/datasets/solana-mainnet',
    blockRange: {
      from: 332557468,
    },
    args: {
      token: TRACKED_TOKEN,
    },
    // We can use the state to track the last block processed
    // and resume from there.
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
