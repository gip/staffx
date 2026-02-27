import { MCP_TOOL_SCOPES } from "./scopes.js";

export interface McpToolDefinition {
  name: keyof typeof MCP_TOOL_SCOPES | string;
  description: string;
  inputSchema: Record<string, unknown>;
}

export interface McpResourceDefinition {
  uri: string;
  name: string;
  description: string;
  mimeType: string;
}

export const MCP_TOOL_DEFINITIONS: McpToolDefinition[] = [
  {
    name: "projects.create",
    description: "Create a project.",
    inputSchema: {
      type: "object",
      required: ["name"],
      properties: {
        name: { type: "string", minLength: 1, maxLength: 80 },
        description: { type: "string" },
        visibility: { type: "string", enum: ["public", "private"] },
      },
      additionalProperties: false,
    },
  },
  {
    name: "projects.check_name",
    description: "Check project name availability for the authenticated user.",
    inputSchema: {
      type: "object",
      required: ["name"],
      properties: {
        name: { type: "string", minLength: 1 },
      },
      additionalProperties: false,
    },
  },
  {
    name: "threads.create",
    description: "Create a thread in a project.",
    inputSchema: {
      type: "object",
      required: ["projectId"],
      properties: {
        projectId: { type: "string" },
        title: { type: "string" },
        description: { type: "string" },
        sourceThreadId: { type: "string" },
      },
      additionalProperties: false,
    },
  },
  {
    name: "threads.update",
    description: "Patch thread metadata or status.",
    inputSchema: {
      type: "object",
      required: ["threadId"],
      properties: {
        threadId: { type: "string" },
        projectId: { type: "string" },
        title: { type: "string" },
        description: { anyOf: [{ type: "string" }, { type: "null" }] },
        status: { type: "string", enum: ["open", "closed", "committed"] },
      },
      additionalProperties: false,
    },
  },
  {
    name: "threads.delete",
    description: "Delete a thread.",
    inputSchema: {
      type: "object",
      required: ["threadId"],
      properties: {
        threadId: { type: "string" },
        projectId: { type: "string" },
      },
      additionalProperties: false,
    },
  },
  {
    name: "threads.chat",
    description: "Append a chat message to a thread.",
    inputSchema: {
      type: "object",
      required: ["threadId", "content"],
      properties: {
        threadId: { type: "string" },
        projectId: { type: "string" },
        content: { type: "string", minLength: 1 },
        role: { type: "string", enum: ["User", "Assistant", "System"] },
      },
      additionalProperties: false,
    },
  },
  {
    name: "threads.matrix.patch_layout",
    description: "Update matrix/topology node layout positions for a thread.",
    inputSchema: {
      type: "object",
      required: ["threadId", "layout"],
      properties: {
        threadId: { type: "string" },
        projectId: { type: "string" },
        layout: {
          type: "array",
          items: {
            type: "object",
            required: ["nodeId", "x", "y"],
            properties: {
              nodeId: { type: "string" },
              x: { type: "number" },
              y: { type: "number" },
            },
            additionalProperties: false,
          },
        },
      },
      additionalProperties: false,
    },
  },
  {
    name: "assistant_runs.start",
    description: "Start an assistant run for a thread.",
    inputSchema: {
      type: "object",
      required: ["threadId", "assistantType"],
      properties: {
        threadId: { type: "string" },
        projectId: { type: "string" },
        assistantType: { type: "string", enum: ["direct", "plan"] },
        prompt: { type: "string" },
        chatMessageId: { type: "string" },
        model: { type: "string" },
      },
      additionalProperties: false,
    },
  },
  {
    name: "assistant_runs.cancel",
    description: "Cancel an in-flight assistant run.",
    inputSchema: {
      type: "object",
      required: ["runId"],
      properties: {
        runId: { type: "string" },
      },
      additionalProperties: false,
    },
  },
  {
    name: "integrations.authorize_url",
    description: "Generate an OAuth authorize URL for an integration provider.",
    inputSchema: {
      type: "object",
      required: ["provider"],
      properties: {
        provider: { type: "string", enum: ["google", "notion"] },
        returnTo: { type: "string" },
      },
      additionalProperties: false,
    },
  },
  {
    name: "integrations.disconnect",
    description: "Disconnect an integration provider for the authenticated user.",
    inputSchema: {
      type: "object",
      required: ["provider"],
      properties: {
        provider: { type: "string", enum: ["google", "notion"] },
      },
      additionalProperties: false,
    },
  },
];

export const MCP_RESOURCE_DEFINITIONS: McpResourceDefinition[] = [
  {
    uri: "staffx://me",
    name: "Current User",
    description: "Authenticated user profile.",
    mimeType: "application/json",
  },
  {
    uri: "staffx://projects?page={n}&pageSize={n}&name={filter}",
    name: "Projects",
    description: "Paginated projects visible to the authenticated user.",
    mimeType: "application/json",
  },
  {
    uri: "staffx://projects/{projectId}",
    name: "Project Detail",
    description: "Project detail resolved from visible project inventory.",
    mimeType: "application/json",
  },
  {
    uri: "staffx://threads?projectId={id}&page={n}&pageSize={n}",
    name: "Threads",
    description: "Paginated thread list with optional project filter.",
    mimeType: "application/json",
  },
  {
    uri: "staffx://threads/{threadId}?projectId={id}",
    name: "Thread Detail",
    description: "Thread detail payload.",
    mimeType: "application/json",
  },
  {
    uri: "staffx://threads/{threadId}/matrix?projectId={id}",
    name: "Thread Matrix",
    description: "Matrix and topology data for a thread.",
    mimeType: "application/json",
  },
  {
    uri: "staffx://assistant-runs/{runId}",
    name: "Assistant Run",
    description: "Assistant run status and output summary.",
    mimeType: "application/json",
  },
  {
    uri: "staffx://integrations",
    name: "Integrations",
    description: "Integration status summary for the authenticated user.",
    mimeType: "application/json",
  },
  {
    uri: "staffx://integrations/{provider}/status",
    name: "Integration Status",
    description: "Status for a specific integration provider.",
    mimeType: "application/json",
  },
  {
    uri: "staffx://threads/{threadId}/events?since={cursorOrTimestamp}&limit={n}",
    name: "Thread Events",
    description: "Thread-scoped event stream snapshot.",
    mimeType: "application/json",
  },
];

export type ParsedResourceUri =
  | { kind: "me" }
  | { kind: "projects"; page?: number; pageSize?: number; name?: string }
  | { kind: "project"; projectId: string }
  | { kind: "threads"; page?: number; pageSize?: number; projectId?: string }
  | { kind: "thread"; threadId: string; projectId?: string }
  | { kind: "threadMatrix"; threadId: string; projectId?: string }
  | { kind: "run"; runId: string }
  | { kind: "integrations" }
  | { kind: "integrationStatus"; provider: string }
  | { kind: "threadEvents"; threadId: string; since?: string; limit?: number };

function asPositiveInt(value: string | null): number | undefined {
  if (!value) return undefined;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

export function parseResourceUri(uri: string): ParsedResourceUri | null {
  let url: URL;
  try {
    url = new URL(uri);
  } catch {
    return null;
  }

  if (url.protocol !== "staffx:") return null;

  const parts = url.pathname.split("/").filter(Boolean);
  const host = url.hostname;

  if (host === "me" && parts.length === 0) return { kind: "me" };
  if (host === "projects" && parts.length === 0) {
    return {
      kind: "projects",
      page: asPositiveInt(url.searchParams.get("page")),
      pageSize: asPositiveInt(url.searchParams.get("pageSize")),
      name: url.searchParams.get("name") ?? undefined,
    };
  }
  if (host === "projects" && parts.length === 1) {
    return { kind: "project", projectId: parts[0] };
  }
  if (host === "threads" && parts.length === 0) {
    return {
      kind: "threads",
      projectId: url.searchParams.get("projectId") ?? undefined,
      page: asPositiveInt(url.searchParams.get("page")),
      pageSize: asPositiveInt(url.searchParams.get("pageSize")),
    };
  }
  if (host === "threads" && parts.length === 1) {
    return {
      kind: "thread",
      threadId: parts[0],
      projectId: url.searchParams.get("projectId") ?? undefined,
    };
  }
  if (host === "threads" && parts.length === 2 && parts[1] === "matrix") {
    return {
      kind: "threadMatrix",
      threadId: parts[0],
      projectId: url.searchParams.get("projectId") ?? undefined,
    };
  }
  if (host === "threads" && parts.length === 2 && parts[1] === "events") {
    return {
      kind: "threadEvents",
      threadId: parts[0],
      since: url.searchParams.get("since") ?? undefined,
      limit: asPositiveInt(url.searchParams.get("limit")),
    };
  }
  if (host === "assistant-runs" && parts.length === 1) {
    return { kind: "run", runId: parts[0] };
  }
  if (host === "integrations" && parts.length === 0) {
    return { kind: "integrations" };
  }
  if (host === "integrations" && parts.length === 2 && parts[1] === "status") {
    return { kind: "integrationStatus", provider: parts[0] };
  }

  return null;
}

export function getToolDefinition(name: string): McpToolDefinition | undefined {
  return MCP_TOOL_DEFINITIONS.find((tool) => tool.name === name);
}
