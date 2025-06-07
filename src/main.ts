import path from 'node:path';
import { PortalAbstractStream } from './core/portal_abstract_stream';
import { createClickhouseClient, ensureTables } from './clickhouse';
import { ClickhouseState } from './core/states/clickhouse_state';
import { createLogger } from './utils';

export type Transfer = {
  account: string;
  block_number: bigint;
  block_hash: string;
  amount: bigint;
  token_mint: string;
  token_decimals: number;
};

export class TransfersStream extends PortalAbstractStream<
  Transfer,
  {
    token: string;
  }
> {
  async stream(): Promise<ReadableStream<Transfer[]>> {
    const { args } = this.options;

    const source = await this.getStream({
      type: 'solana',
      fields: {
        block: {
          number: true,
          hash: true,
          timestamp: true,
        },
        tokenBalance: {
          transactionIndex: true,
          account: true,
          preAmount: true,
          postAmount: true,
          preMint: true,
          postMint: true,
          postDecimals: true,
        },
      },
      tokenBalances: [
        {
          postMint: [ args.token ]
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
                block_hash: block.header.hash,
                amount: tb.postAmount,
                token_mint: tb.postMint,
                token_decimals: tb.postDecimals,
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
      from: process.env.FROM_BLOCK || 332557468,
      to: process.env.TO_BLOCK,
    },
    args: {
      token: TRACKED_TOKEN,
    },
    /**
     * We can use the state to track the last block processed
     * and resume from there.
     */
    state: new ClickhouseState(clickhouse, {
      table: 'solana_sync_status',
      id: 'transfers',
    }),
    logger,
  });

  /**
   * Ensure tables are created in ClickHouse
   */
  await ensureTables(clickhouse, path.join(__dirname, 'transfers.sql'));

  for await (const transfers of await ds.stream()) {
    await clickhouse.insert({
      table: 'transfers_raw',
      values: transfers.map(t => ({ ...t, sign: 1 })),
      format: 'JSONEachRow',
    });

    await ds.ack();
  }
}

void main();
