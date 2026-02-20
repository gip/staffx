import { beforeEach, describe, expect, it, vi } from "vitest";
import { buildV1TestApp } from "../utils/app.js";
import * as authMock from "../utils/authMock.js";
import * as eventsMock from "../utils/eventsMock.js";
import { SSE_EVENT } from "../utils/fixtures.js";

vi.mock("../../../src/auth.js", async () => {
  const mocked = await import("../utils/authMock.js");
  return mocked.getAuthMockModule();
});
vi.mock("../../../src/events.js", async () => {
  const mocked = await import("../utils/eventsMock.js");
  return mocked.createEventsMockModule();
});

const AUTH_TOKEN = "token-owner";

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

type StreamServerAddress = { port: number; host: string };

async function bindStreamServer(app: Awaited<ReturnType<typeof buildV1TestApp>>) {
  const candidates = ["127.0.0.1", "localhost", "0.0.0.0"] as const;
  for (const host of candidates) {
    try {
      await app.listen({ port: 0, host });
      const address = app.server.address();
      if (!address || typeof address === "string") return null;
      return { port: address.port, host };
    } catch (error) {
      if (
        error instanceof Error
        && /EPERM|EACCES|EADDRNOTAVAIL/i.test(error.message)
      ) {
        continue;
      }
      throw error;
    }
  }
  return null;
}

describe("/v1 events", () => {
  beforeEach(() => {
    authMock.setAuthToken(AUTH_TOKEN, {
      id: "11111111-1111-1111-1111-111111111111",
      auth0Id: "auth0|event-user",
      email: "events@example.com",
      name: "Events User",
      picture: null,
      handle: "events-user",
      githubHandle: null,
      orgId: "11111111-1111-1111-1111-111111111111",
      scope: null,
      createdAt: new Date("2025-01-01T00:00:00.000Z"),
      updatedAt: new Date("2025-01-01T00:00:00.000Z"),
    });
    authMock.resetAuthMocks();
    authMock.setAuthToken(AUTH_TOKEN, {
      id: "11111111-1111-1111-1111-111111111111",
      auth0Id: "auth0|event-user",
      email: "events@example.com",
      name: "Events User",
      picture: null,
      handle: "events-user",
      githubHandle: null,
      orgId: "11111111-1111-1111-1111-111111111111",
      scope: null,
      createdAt: new Date("2025-01-01T00:00:00.000Z"),
      updatedAt: new Date("2025-01-01T00:00:00.000Z"),
    });
    eventsMock.clearQueryEvents();
  });

  describe("GET /v1/events", () => {
    it("returns default pageSize and query metadata", async () => {
      eventsMock.setQueryEventsHandler(() => ({ items: [SSE_EVENT], nextCursor: "next-cursor" }));

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/events",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(response.json()).toEqual({
        items: [SSE_EVENT],
        nextCursor: "next-cursor",
        page: 1,
        pageSize: 100,
      });
      expect(eventsMock.getQueryEventsCalls().at(-1)?.limit).toBe(100);
    });

    it("clamps invalid limit values", async () => {
      eventsMock.setQueryEventsHandler(() => ({ items: [], nextCursor: null }));

      const app = await buildV1TestApp();
      const zero = await app.inject({
        method: "GET",
        url: "/v1/events?limit=0",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      const huge = await app.inject({
        method: "GET",
        url: "/v1/events?limit=1000",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      const calls = eventsMock.getQueryEventsCalls();
      expect(calls[0]?.limit).toBe(100);
      expect(calls[1]?.limit).toBe(500);
      expect(zero.statusCode).toBe(200);
      expect(huge.statusCode).toBe(200);
      expect(huge.json().pageSize).toBe(500);
    });

    it("passes RFC3339 since values through to events query", async () => {
      const since = "2025-01-01T12:00:00.000Z";
      let capturedSince: string | undefined;
      eventsMock.setQueryEventsHandler((input) => {
        capturedSince = input.since;
        return { items: [SSE_EVENT], nextCursor: null };
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: `/v1/events?since=${encodeURIComponent(since)}`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(capturedSince).toBe(since);
      expect(response.json().pageSize).toBe(100);
    });

    it("passes encoded cursor since values through to events query", async () => {
      const cursor = encodeURIComponent("2025-01-01T12:00:00.000Z|evt-1");
      let capturedSince: string | undefined;
      eventsMock.setQueryEventsHandler((input) => {
        capturedSince = input.since;
        return { items: [], nextCursor: null };
      });

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: `/v1/events?since=${cursor}`,
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(200);
      expect(capturedSince).toBe(decodeURIComponent(cursor));
    });

    it("rejects invalid since values", async () => {
      eventsMock.setQueryEventsHandler(() => ({ items: [SSE_EVENT], nextCursor: null }));

      const app = await buildV1TestApp();
      const response = await app.inject({
        method: "GET",
        url: "/v1/events?since=invalid",
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      await app.close();

      expect(response.statusCode).toBe(400);
      expect(response.json().title).toBe("Invalid cursor");
      expect(eventsMock.getQueryEventsCalls()).toHaveLength(0);
    });
  });

  describe("GET /v1/events/stream", () => {
    it("returns SSE stream with event envelope and retry field", async () => {
      let callCount = 0;
      eventsMock.setQueryEventsHandler(() => {
        callCount += 1;
        if (callCount > 1) {
          return { items: [], nextCursor: null };
        }
        return {
          items: [SSE_EVENT],
          nextCursor: null,
        };
      });

      const app = await buildV1TestApp();
      const address = await bindStreamServer(app);
      if (!address) {
        await app.close();
        return;
      }
      const url = `http://${address.host}:${address.port}/v1/events/stream`;
      const controller = new AbortController();

      const response = await fetch(url, {
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
        signal: controller.signal,
      });
      expect(response.status).toBe(200);

      const reader = response.body?.getReader();
      expect(reader).toBeDefined();
      const chunkText: string[] = [];
      const decoder = new TextDecoder();
      const readTask = (async () => {
        if (!reader) return "";
        try {
          while (true) {
            const next = await reader.read();
            if (next.done) break;
            chunkText.push(decoder.decode(next.value, { stream: true }));
            if (chunkText.join("").includes("data:")) break;
          }
        } catch {
          // Abort or disconnect while consuming the stream.
        }
        return chunkText.join("");
      })();

      await delay(120);
      controller.abort();
      const text = await Promise.race([
        readTask,
        new Promise<string>((_, reject) => setTimeout(() => reject(new Error("stream did not close")), 2000)),
      ]);

      expect(text).toContain("retry: 3000");
      expect(text).toContain(`id: ${encodeURIComponent(SSE_EVENT.occurredAt)}|${encodeURIComponent(SSE_EVENT.id)}`);
      expect(text).toContain(`event: ${SSE_EVENT.type}`);
      expect(text).toContain(`data: ${JSON.stringify(SSE_EVENT)}`);

      await app.close();
      expect(text).toContain("retry: 3000");
    });

    it("honors Last-Event-ID over invalid query since and allows abort cleanup", async () => {
      let capturedSince: string | undefined;
      eventsMock.setQueryEventsHandler((input) => {
        capturedSince = input.since;
        return { items: [], nextCursor: null };
      });

      const app = await buildV1TestApp();
      const address = await bindStreamServer(app);
      if (!address) {
        await app.close();
        return;
      }
      const headerCursor = encodeURIComponent("2025-01-01T00:00:00.000Z|evt-last");
      const url = `http://${address.host}:${address.port}/v1/events/stream?since=${encodeURIComponent("invalid")}`;
      const controller = new AbortController();

      const response = await fetch(url, {
        headers: {
          authorization: `Bearer ${AUTH_TOKEN}`,
          "last-event-id": headerCursor,
        },
        signal: controller.signal,
      });
      expect(response.status).toBe(200);

      await delay(120);
      controller.abort();

      await new Promise<void>((resolve) => setTimeout(resolve, 30));
      await app.close();
      expect(capturedSince).toBe(decodeURIComponent(headerCursor));
    });

    it("returns 400 for invalid since when no Last-Event-ID is provided", async () => {
      const app = await buildV1TestApp();
      const address = await bindStreamServer(app);
      if (!address) {
        await app.close();
        return;
      }

      const response = await fetch(`http://${address.host}:${address.port}/v1/events/stream?since=invalid`, {
        headers: { authorization: `Bearer ${AUTH_TOKEN}` },
      });
      const body = await response.json();
      await app.close();

      expect(response.status).toBe(400);
      expect(body.title).toBe("Invalid cursor");
    });
  });
});
