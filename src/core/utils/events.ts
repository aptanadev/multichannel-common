import { ClientSession } from "mongoose";
import { Event, EventOptions } from "@/core/domain/events/Event";
import { OutboxRepo } from "@/core/infrastructure/events/outbox/OutboxRepo";

export interface DispatcherOptions extends EventOptions {
  outbox?: boolean;
  session?: ClientSession;
}

export async function dispatcher(
  publisher: Event,
  options?: DispatcherOptions
): Promise<any> {
  if (options?.outbox) {
    const repo = new OutboxRepo();
    await repo.enqueue(publisher as any, options.session);
    return;
  }

  const { outbox: _outbox, session: _session, ...amqpOptions } = options ?? {};
  return await publisher.init().publish(amqpOptions);
}

export async function dispatcherWithoutPersistence(
  publisher: Event,
  options?: EventOptions
): Promise<any> {
  return await publisher.init().publishWithoutPersistence!(options);
}
