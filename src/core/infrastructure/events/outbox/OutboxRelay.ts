import logger from '@/core/utils/logger';
import { Loader } from '@/core/infrastructure/loaders/Loader';
import { ChannelEvent } from '../ChannelEvent';
import { OutboxModel } from './OutboxModel';
import { OutboxRepo } from './OutboxRepo';

export interface OutboxRelayOptions {
  sweepIntervalMs?: number;
  batchSize?: number;
  enableSprinter?: boolean;
}

export class OutboxRelay extends Loader {
  private repo: OutboxRepo;
  private sweepIntervalMs: number;
  private batchSize: number;
  private enableSprinter: boolean;

  constructor(options: OutboxRelayOptions = {}) {
    super();
    this.repo = new OutboxRepo();
    this.sweepIntervalMs = options.sweepIntervalMs ?? 5000;
    this.batchSize = options.batchSize ?? 10;
    this.enableSprinter = options.enableSprinter ?? true;
  }

  register(): void {
    this.startSweeper();
    if (this.enableSprinter) {
      this.startSprinter();
    }
    logger.info('OutboxRelay started', {
      sweepIntervalMs: this.sweepIntervalMs,
      batchSize: this.batchSize,
      enableSprinter: this.enableSprinter,
    });
  }

  start(): void {
    this.register();
  }

  private startSweeper(): void {
    setInterval(() => this.sweep(), this.sweepIntervalMs);
    logger.debug('OutboxRelay sweeper started', { intervalMs: this.sweepIntervalMs });
  }

  private startSprinter(): void {
    try {
      const stream = OutboxModel.watch([{ $match: { operationType: 'insert' } }]);

      stream.on('change', () => {
        // Trigger an immediate sweep instead of publishing directly
        // so the sweeper's pessimistic locking handles multi-instance concurrency
        this.sweep().catch((err) => {
          logger.error('OutboxRelay sprinter-triggered sweep error', { message: err.message });
        });
      });

      stream.on('error', (err: Error) => {
        logger.warn('OutboxRelay sprinter error, relying on sweeper only', {
          message: err.message,
        });
      });

      logger.debug('OutboxRelay sprinter started (Change Streams)');
    } catch (err: any) {
      logger.warn('OutboxRelay sprinter failed to start, relying on sweeper only', {
        message: err.message,
      });
    }
  }

  private async sweep(): Promise<void> {
    try {
      const records = await this.repo.claimBatch(this.batchSize);
      if (records.length === 0) return;

      await Promise.all(records.map((record) => this.publishRecord(record)));
    } catch (err: any) {
      logger.error('OutboxRelay sweep error', { message: err.message });
    }
  }

  private async publishRecord(record: any): Promise<void> {
    const channel = ChannelEvent.getChannel('publisher');
    try {
      await channel.publish(record.exchange, record.topic, record.payload, {
        deliveryMode: 2,
        persistent: true,
      });
      await this.repo.markSent(record._id.toString());
      logger.debug('OutboxRelay published', { id: record._id, topic: record.topic });
    } catch (err: any) {
      logger.error('OutboxRelay publish failed', { id: record._id, message: err.message });
      await this.repo.markFailed(
        record._id.toString(),
        err.message,
        (record.retries ?? 0) + 1,
        record.maxRetries ?? 5
      );
    }
  }
}
