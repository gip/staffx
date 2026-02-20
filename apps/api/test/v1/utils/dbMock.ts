import { randomUUID } from "node:crypto";
import { vi } from "vitest";

type QueryParams = Array<unknown> | undefined;
type QueryResult = { rows: unknown[]; rowCount: number };
type QueryHandler = (text: string, params?: QueryParams) => QueryResult | Promise<QueryResult>;
type ClientQueryHandler = (text: string, params?: QueryParams) => QueryResult | Promise<QueryResult>;

interface ClientQueryCall {
  text: string;
  params: QueryParams;
}

let defaultQueryHandler: QueryHandler = async () => ({ rows: [], rowCount: 0 });
let connectQueryQueue: ClientQueryHandler[] = [];
const queryLog: { text: string; params: QueryParams }[] = [];
let queryLogEnabled = true;

const queryMock = vi.fn(async (text: string, params: QueryParams = []): Promise<QueryResult> => {
  if (queryLogEnabled) {
    queryLog.push({ text, params: Array.isArray(params) ? [...params] : params });
  }
  return defaultQueryHandler(text, params);
});

const connectMock = vi.fn(async () => {
  const localQueue = [...connectQueryQueue];
  connectQueryQueue = [];
  const clientCalls: ClientQueryCall[] = [];

  const client = {
    query: vi.fn(async (text: string, params: QueryParams = []): Promise<QueryResult> => {
      clientCalls.push({ text, params: Array.isArray(params) ? [...params] : params });

      const next = localQueue.shift();
      if (next) {
        return next(text, params);
      }

      return queryMock(text, params);
    }),
    release: vi.fn(),
    _calls: clientCalls,
  };

  return client;
});

const closeMock = vi.fn(async () => undefined);

export const query = queryMock;
export const pool = {
  connect: connectMock,
} as {
  connect: () => ReturnType<typeof connectMock>;
};

export const close = closeMock;

export function setQueryHandler(handler: QueryHandler) {
  defaultQueryHandler = handler;
}

export function setConnectClientQueries(queries: Array<QueryResult | Error | ClientQueryHandler>) {
  connectQueryQueue = queries.map((entry) => {
    if (entry instanceof Error) {
      return async () => {
        throw entry;
      };
    }

    if (typeof entry === "function") {
      return entry as ClientQueryHandler;
    }

    const rowResult = entry as QueryResult;
    return async () => rowResult;
  });
}

export function resetConnectClientQueries() {
  connectQueryQueue = [];
}

export function clearQueryLog() {
  queryLog.length = 0;
}

export function getQueryLog() {
  return queryLog.slice();
}

export function resetDbMock() {
  setQueryHandler(async () => ({ rows: [], rowCount: 0 }));
  resetConnectClientQueries();
  clearQueryLog();
  queryMock.mockClear();
  connectMock.mockClear();
  closeMock.mockClear();
}

export function queryRows(...rows: unknown[]): QueryResult {
  return { rows, rowCount: rows.length };
}

export function queryNoRows(): QueryResult {
  return { rows: [], rowCount: 0 };
}

export function defaultPaged<T>(rows: T[], limit: number): QueryResult {
  return { rows: rows.slice(0, Math.min(rows.length, limit)), rowCount: rows.slice(0, Math.min(rows.length, limit)).length };
}

export function createDbModuleMock() {
  const poolMock = {
    query,
    connect: connectMock,
  };

  return {
    default: poolMock,
    query,
    pool: poolMock,
    close,
    queryRows,
    randomUUID,
  };
}
