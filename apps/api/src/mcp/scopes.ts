export const MCP_SCOPES = [
  "staffx:projects:read",
  "staffx:projects:write",
  "staffx:threads:read",
  "staffx:threads:write",
  "staffx:matrix:read",
  "staffx:matrix:write",
  "staffx:runs:read",
  "staffx:runs:write",
  "staffx:integrations:read",
  "staffx:integrations:write",
  "staffx:events:read",
] as const;

export type McpScope = (typeof MCP_SCOPES)[number];

export const MCP_TOOL_SCOPES: Record<string, McpScope[]> = {
  "projects.create": ["staffx:projects:write"],
  "projects.check_name": ["staffx:projects:read"],
  "threads.create": ["staffx:threads:write"],
  "threads.update": ["staffx:threads:write"],
  "threads.delete": ["staffx:threads:write"],
  "threads.chat": ["staffx:threads:write"],
  "threads.matrix.patch_layout": ["staffx:matrix:write", "staffx:threads:write"],
  "assistant_runs.start": ["staffx:runs:write", "staffx:threads:write"],
  "assistant_runs.cancel": ["staffx:runs:write"],
  "integrations.authorize_url": ["staffx:integrations:write"],
  "integrations.disconnect": ["staffx:integrations:write"],
};

const READ_RESOURCE_SCOPES: Record<string, McpScope[]> = {
  "staffx://me": ["staffx:projects:read"],
  "me": ["staffx:projects:read"],
  "projects": ["staffx:projects:read"],
  "threads": ["staffx:threads:read"],
  "integrations": ["staffx:integrations:read"],
};

export function requiredScopesForTool(name: string): McpScope[] {
  return MCP_TOOL_SCOPES[name] ?? [];
}

export function requiredScopesForResourceUri(uri: string): McpScope[] {
  let url: URL | null = null;
  try {
    url = new URL(uri);
  } catch {
    url = null;
  }

  if (!url || url.protocol !== "staffx:") return [];

  const host = url.hostname;
  const path = url.pathname;

  if (uri.startsWith("staffx://projects/")) return ["staffx:projects:read"];
  if (uri.startsWith("staffx://threads/") && uri.includes("/matrix")) return ["staffx:matrix:read", "staffx:threads:read"];
  if (uri.startsWith("staffx://threads/") && uri.includes("/events")) return ["staffx:events:read", "staffx:threads:read"];
  if (uri.startsWith("staffx://threads/")) return ["staffx:threads:read"];
  if (uri.startsWith("staffx://assistant-runs/")) return ["staffx:runs:read"];
  if (uri.startsWith("staffx://integrations/") && uri.endsWith("/status")) return ["staffx:integrations:read"];
  if (host === "assistant-runs" && path.split("/").filter(Boolean).length === 1) return ["staffx:runs:read"];
  return READ_RESOURCE_SCOPES[host] ?? READ_RESOURCE_SCOPES[uri] ?? [];
}
