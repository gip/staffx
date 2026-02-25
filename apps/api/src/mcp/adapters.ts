import type { FastifyInstance } from "fastify";
import { parseResourceUri, type ParsedResourceUri } from "./catalog.js";

interface InjectOptions {
  method: "GET" | "POST" | "PATCH" | "DELETE";
  url: string;
  token: string;
  payload?: unknown;
}

interface V1EventItem {
  id: string;
  type: string;
  aggregateType: string;
  aggregateId: string;
  occurredAt: string;
  traceId: string | null;
  payload: Record<string, unknown>;
  version: number;
}

interface V1EventListResponse {
  items: V1EventItem[];
  nextCursor: string | null;
  page: number;
  pageSize: number;
}

function encodeQueryValue(value: string | number | undefined): string | null {
  if (typeof value === "undefined") return null;
  return String(value);
}

function withQuery(basePath: string, query: Record<string, string | number | undefined>): string {
  const params = new URLSearchParams();
  for (const [key, raw] of Object.entries(query)) {
    const value = encodeQueryValue(raw);
    if (value !== null) params.set(key, value);
  }
  const queryString = params.toString();
  return queryString ? `${basePath}?${queryString}` : basePath;
}

function parseJson(rawBody: string): unknown {
  if (!rawBody) return null;
  try {
    return JSON.parse(rawBody);
  } catch {
    return rawBody;
  }
}

export class AdapterHttpError extends Error {
  statusCode: number;
  body: unknown;

  constructor(statusCode: number, body: unknown, message = "Adapter request failed") {
    super(message);
    this.statusCode = statusCode;
    this.body = body;
  }
}

async function injectJson<T>(app: FastifyInstance, options: InjectOptions): Promise<T> {
  const response = await app.inject({
    method: options.method,
    url: options.url,
    headers: {
      authorization: `Bearer ${options.token}`,
      "content-type": "application/json",
    },
    payload: typeof options.payload === "undefined" ? undefined : JSON.stringify(options.payload),
  });

  const body = parseJson(response.body);
  if (response.statusCode < 200 || response.statusCode >= 300) {
    throw new AdapterHttpError(response.statusCode, body);
  }

  return body as T;
}

function getThreadIdFromEventPayload(event: V1EventItem): string | null {
  const rawThreadId = event.payload?.threadId;
  return typeof rawThreadId === "string" ? rawThreadId : null;
}

function filterEventsToThread(items: V1EventItem[], threadId: string): V1EventItem[] {
  return items.filter((event) => {
    if (event.aggregateType === "thread" && event.aggregateId === threadId) return true;
    if (event.aggregateType === "assistant-run" && getThreadIdFromEventPayload(event) === threadId) return true;
    return getThreadIdFromEventPayload(event) === threadId;
  });
}

async function readResourceByParsedUri(
  app: FastifyInstance,
  token: string,
  parsed: ParsedResourceUri,
): Promise<unknown> {
  if (parsed.kind === "me") {
    return injectJson(app, { method: "GET", url: "/v1/me", token });
  }
  if (parsed.kind === "projects") {
    return injectJson(app, {
      method: "GET",
      url: withQuery("/v1/projects", {
        page: parsed.page,
        pageSize: parsed.pageSize,
        name: parsed.name,
      }),
      token,
    });
  }
  if (parsed.kind === "project") {
    // v1 does not expose project-by-id directly; resolve through visible project list.
    const response = await injectJson<{ items?: Array<Record<string, unknown>> }>(app, {
      method: "GET",
      url: withQuery("/v1/projects", { page: 1, pageSize: 200 }),
      token,
    });
    const item = (response.items ?? []).find((project) => project.id === parsed.projectId);
    if (!item) throw new AdapterHttpError(404, { error: "Project not found" });
    return item;
  }
  if (parsed.kind === "threads") {
    return injectJson(app, {
      method: "GET",
      url: withQuery("/v1/threads", {
        projectId: parsed.projectId,
        page: parsed.page,
        pageSize: parsed.pageSize,
      }),
      token,
    });
  }
  if (parsed.kind === "thread") {
    return injectJson(app, {
      method: "GET",
      url: withQuery(`/v1/threads/${encodeURIComponent(parsed.threadId)}`, { projectId: parsed.projectId }),
      token,
    });
  }
  if (parsed.kind === "threadMatrix") {
    return injectJson(app, {
      method: "GET",
      url: withQuery(`/v1/threads/${encodeURIComponent(parsed.threadId)}/matrix`, { projectId: parsed.projectId }),
      token,
    });
  }
  if (parsed.kind === "run") {
    return injectJson(app, {
      method: "GET",
      url: `/v1/assistant-runs/${encodeURIComponent(parsed.runId)}`,
      token,
    });
  }
  if (parsed.kind === "integrations") {
    return injectJson(app, {
      method: "GET",
      url: "/v1/integrations",
      token,
    });
  }
  if (parsed.kind === "integrationStatus") {
    return injectJson(app, {
      method: "GET",
      url: `/v1/integrations/${encodeURIComponent(parsed.provider)}/status`,
      token,
    });
  }
  if (parsed.kind === "threadEvents") {
    // Confirm thread-level access before returning events.
    await injectJson(app, {
      method: "GET",
      url: `/v1/threads/${encodeURIComponent(parsed.threadId)}`,
      token,
    });

    const events = await injectJson<V1EventListResponse>(app, {
      method: "GET",
      url: withQuery("/v1/events", {
        since: parsed.since,
        limit: parsed.limit,
      }),
      token,
    });

    const scopedItems = filterEventsToThread(events.items, parsed.threadId);
    return {
      items: scopedItems,
      page: events.page,
      pageSize: events.pageSize,
      nextCursor: events.nextCursor,
    };
  }

  throw new AdapterHttpError(400, { error: "Unsupported resource URI" });
}

function asRecord(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null ? (value as Record<string, unknown>) : {};
}

export async function callToolViaV1(
  app: FastifyInstance,
  token: string,
  name: string,
  args: unknown,
): Promise<unknown> {
  const input = asRecord(args);

  if (name === "projects.create") {
    return injectJson(app, {
      method: "POST",
      url: "/v1/projects",
      token,
      payload: {
        name: input.name,
        description: input.description,
        visibility: input.visibility,
      },
    });
  }

  if (name === "projects.check_name") {
    return injectJson(app, {
      method: "GET",
      url: withQuery("/v1/projects/check-name", { name: typeof input.name === "string" ? input.name : undefined }),
      token,
    });
  }

  if (name === "threads.create") {
    return injectJson(app, {
      method: "POST",
      url: "/v1/threads",
      token,
      payload: {
        projectId: input.projectId,
        title: input.title,
        description: input.description,
        sourceThreadId: input.sourceThreadId,
      },
    });
  }

  if (name === "threads.update") {
    const threadId = typeof input.threadId === "string" ? input.threadId : "";
    return injectJson(app, {
      method: "PATCH",
      url: withQuery(`/v1/threads/${encodeURIComponent(threadId)}`, {
        projectId: typeof input.projectId === "string" ? input.projectId : undefined,
      }),
      token,
      payload: {
        title: input.title,
        description: input.description,
        status: input.status,
      },
    });
  }

  if (name === "threads.delete") {
    const threadId = typeof input.threadId === "string" ? input.threadId : "";
    return injectJson(app, {
      method: "DELETE",
      url: withQuery(`/v1/threads/${encodeURIComponent(threadId)}`, {
        projectId: typeof input.projectId === "string" ? input.projectId : undefined,
      }),
      token,
    });
  }

  if (name === "threads.chat") {
    const threadId = typeof input.threadId === "string" ? input.threadId : "";
    return injectJson(app, {
      method: "POST",
      url: withQuery(`/v1/threads/${encodeURIComponent(threadId)}/chat`, {
        projectId: typeof input.projectId === "string" ? input.projectId : undefined,
      }),
      token,
      payload: {
        content: input.content,
        role: input.role,
      },
    });
  }

  if (name === "threads.matrix.patch_layout") {
    const threadId = typeof input.threadId === "string" ? input.threadId : "";
    return injectJson(app, {
      method: "PATCH",
      url: withQuery(`/v1/threads/${encodeURIComponent(threadId)}/matrix`, {
        projectId: typeof input.projectId === "string" ? input.projectId : undefined,
      }),
      token,
      payload: {
        layout: input.layout,
      },
    });
  }

  if (name === "assistant_runs.start") {
    const threadId = typeof input.threadId === "string" ? input.threadId : "";
    const assistantType = typeof input.assistantType === "string" ? input.assistantType : "direct";
    return injectJson(app, {
      method: "POST",
      url: withQuery(`/v1/threads/${encodeURIComponent(threadId)}/assistants/${encodeURIComponent(assistantType)}/runs`, {
        projectId: typeof input.projectId === "string" ? input.projectId : undefined,
      }),
      token,
      payload: {
        prompt: input.prompt,
        chatMessageId: input.chatMessageId,
        model: input.model,
      },
    });
  }

  if (name === "assistant_runs.cancel") {
    const runId = typeof input.runId === "string" ? input.runId : "";
    return injectJson(app, {
      method: "POST",
      url: `/v1/assistant-runs/${encodeURIComponent(runId)}/cancel`,
      token,
      payload: {},
    });
  }

  if (name === "integrations.authorize_url") {
    const provider = typeof input.provider === "string" ? input.provider : "";
    return injectJson(app, {
      method: "GET",
      url: withQuery(`/v1/integrations/${encodeURIComponent(provider)}/authorize-url`, {
        returnTo: typeof input.returnTo === "string" ? input.returnTo : undefined,
      }),
      token,
    });
  }

  if (name === "integrations.disconnect") {
    const provider = typeof input.provider === "string" ? input.provider : "";
    return injectJson(app, {
      method: "POST",
      url: `/v1/integrations/${encodeURIComponent(provider)}/disconnect`,
      token,
      payload: {},
    });
  }

  throw new AdapterHttpError(404, { error: `Unknown tool "${name}"` });
}

export async function readResourceViaV1(app: FastifyInstance, token: string, uri: string): Promise<unknown> {
  const parsed = parseResourceUri(uri);
  if (!parsed) {
    throw new AdapterHttpError(400, { error: "Invalid or unsupported resource URI" });
  }
  return readResourceByParsedUri(app, token, parsed);
}
