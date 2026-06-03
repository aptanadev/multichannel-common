import { ClientSession } from 'mongoose';
import { OutboxDocument, OutboxModel, OutboxStatus } from './OutboxModel';

export interface OutboxEnqueueable {
  outboxData: { exchange: string; topic: string; payload: any };
}

export class OutboxRepo {
  async enqueue(publisher: OutboxEnqueueable, session?: ClientSession): Promise<void> {
    const { exchange, topic, payload } = publisher.outboxData;
    await OutboxModel.create([{ exchange, topic, payload }], session ? { session } : {});
  }

  async claimBatch(limit: number = 10): Promise<OutboxDocument[]> {
    const now = new Date();
    const lockedUntil = new Date(Date.now() + 30_000);
    const records: OutboxDocument[] = [];

    for (let i = 0; i < limit; i++) {
      const record = await OutboxModel.findOneAndUpdate(
        {
          status: OutboxStatus.Pending,
          $or: [{ lockedUntil: { $exists: false } }, { lockedUntil: { $lte: now } }],
        },
        { $set: { status: OutboxStatus.Processing, lockedUntil } },
        { new: true, sort: { createdAt: 1 } }
      );

      if (!record) break;
      records.push(record);
    }

    return records;
  }

  async markSent(id: string): Promise<void> {
    await OutboxModel.findByIdAndUpdate(id, { $set: { status: OutboxStatus.Sent } });
  }

  async markFailed(
    id: string,
    error: string,
    retries: number,
    maxRetries: number
  ): Promise<void> {
    if (retries < maxRetries) {
      await OutboxModel.findByIdAndUpdate(id, {
        $set: { status: OutboxStatus.Pending, retries, lastError: error, lockedUntil: null },
      });
    } else {
      await OutboxModel.findByIdAndUpdate(id, {
        $set: { status: OutboxStatus.Failed, retries, lastError: error },
      });
    }
  }
}
