import mongoose from 'mongoose';

export enum OutboxStatus {
  Pending = 'pending',
  Processing = 'processing',
  Sent = 'sent',
  Failed = 'failed',
}

export interface IOutbox {
  exchange: string;
  topic: string;
  payload: any;
  status: OutboxStatus;
  retries: number;
  maxRetries: number;
  lastError?: string;
  lockedUntil?: Date;
}

const OutboxSchema = new mongoose.Schema<IOutbox>(
  {
    exchange: { type: String, required: true },
    topic: { type: String, required: true },
    payload: { type: mongoose.Schema.Types.Mixed, required: true },
    status: {
      type: String,
      enum: Object.values(OutboxStatus),
      default: OutboxStatus.Pending,
      index: true,
    },
    retries: { type: Number, default: 0 },
    maxRetries: { type: Number, default: 5 },
    lastError: { type: String },
    lockedUntil: { type: Date },
  },
  { timestamps: true }
);

// TTL index: auto-delete SENT records after 7 days
OutboxSchema.index(
  { updatedAt: 1 },
  {
    expireAfterSeconds: 7 * 24 * 60 * 60,
    partialFilterExpression: { status: OutboxStatus.Sent },
  }
);

export type OutboxDocument = mongoose.Document & IOutbox;

// Safe registration: avoid "Cannot overwrite model" error on hot-reload
export const OutboxModel =
  (mongoose.models['Outbox'] as mongoose.Model<OutboxDocument>) ||
  mongoose.model<OutboxDocument>('Outbox', OutboxSchema);
