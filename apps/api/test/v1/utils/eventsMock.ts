import { vi } from "vitest";

export interface MockStaffXEvent {
  id: string;
  type: string;
  aggregateType: string;
  aggregateId: string;
  occurredAt: string;
  orgId: string | null;
  traceId: string | null;
  payload: Record<string, unknown>;
  version: number;
}

type QueryEventsResult = { items: MockStaffXEvent[]; nextCursor: string | null };
type QueryEventsHandler = (input: { orgId?: string | null; since?: string; limit?: number }) => QueryEventsResult | Promise<QueryEventsResult>;

let queryEventsHandler: QueryEventsHandler = async () => ({ items: [], nextCursor: null });
let queryEventsCalls: Array<{ orgId?: string | null; since?: string; limit?: number }> = [];

export const publishEventMock = vi.fn(async (input: {
  type: string;
  aggregateType: string;
  aggregateId: string;
  orgId?: string | null;
  payload: Record<string, unknown>;
  traceId?: string | null;
  version?: number;
  occurredAt?: Date;
}): Promise<MockStaffXEvent> => {
  return {
    id: "evt-" + Math.random().toString(16).slice(2),
    type: input.type,
    aggregateType: input.aggregateType,
    aggregateId: input.aggregateId,
    occurredAt: (input.occurredAt ?? new Date()).toISOString(),
    orgId: input.orgId ?? null,
    traceId: input.traceId ?? null,
    payload: input.payload,
    version: input.version ?? 1,
  };
});

export const queryEventsMock = vi.fn(async (input: { orgId?: string | null; since?: string; limit?: number }) => {
  queryEventsCalls.push(input);
  return queryEventsHandler(input);
});

export const encodeCursor = vi.fn((event: MockStaffXEvent) => {
  return `${encodeURIComponent(event.occurredAt)}|${encodeURIComponent(event.id)}`;
});

export const parseCursor = vi.fn((value: string) => {
  const pieces = value.split("|");
  if (pieces.length !== 2) return null;
  const occurredAt = decodeURIComponent(pieces[0] ?? "");
  const id = decodeURIComponent(pieces[1] ?? "");
  if (!occurredAt || !id) return null;
  const parsedDate = new Date(occurredAt);
  if (Number.isNaN(parsedDate.getTime())) return null;
  return { occurredAt: parsedDate.toISOString(), id };
});

export const STAFFX_EVENT_TYPES = [
  "chat.session.finished",
  "assistant.run.started",
  "assistant.run.progress",
  "assistant.run.waiting_input",
  "assistant.run.completed",
  "assistant.run.failed",
  "assistant.run.cancelled",
  "thread.matrix.changed",
] as const;

export function setQueryEventsHandler(handler: QueryEventsHandler) {
  queryEventsHandler = handler;
}

export function getQueryEventsCalls() {
  return queryEventsCalls.slice();
}

export function clearQueryEvents() {
  queryEventsCalls = [];
  queryEventsMock.mockClear();
  publishEventMock.mockClear();
  encodeCursor.mockClear();
  parseCursor.mockClear();
  queryEventsHandler = async () => ({ items: [], nextCursor: null });
}

export function createEventsMockModule() {
  return {
    publishEvent: publishEventMock,
    queryEvents: queryEventsMock,
    encodeCursor,
    parseCursor,
    STAFFX_EVENT_TYPES,
  };
}
