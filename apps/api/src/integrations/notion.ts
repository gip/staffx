const NOTION_AUTH_URL = "https://api.notion.com/v1/oauth/authorize";
const NOTION_TOKEN_URL = "https://api.notion.com/v1/oauth/token";
const NOTION_PAGE_URL = "https://api.notion.com/v1/pages";
const NOTION_BLOCKS_URL = "https://api.notion.com/v1/blocks";

export interface ExternalTokenResult {
  accessToken: string;
  refreshToken: string | null;
  expiresAt: string | null;
  tokenType: string;
  scope: string | null;
  providerAccountId: string | null;
}

export interface ExternalDocumentResult {
  sourceExternalId: string;
  sourceUrl: string;
  title: string;
  text: string;
  sourceMetadata: Record<string, unknown>;
}

export interface SourceLookupResult {
  sourceExternalId: string;
  sourceUrl: string;
}

interface NotionTokenResponse {
  access_token?: string;
  refresh_token?: string;
  expires_in?: number;
  token_type?: string;
  scope?: string;
  owner?: {
    user?: { id?: string };
  };
}

interface NotionPageResponse {
  id?: string;
  url?: string;
  properties?: Record<string, unknown>;
}

interface NotionBlockResponse {
  results?: Array<{
    type?: string;
    paragraph?: { rich_text?: Array<{ plain_text?: string }> };
  }>;
}

function env(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function requireNotionConfig() {
  return {
    clientId: env("NOTION_CLIENT_ID"),
    clientSecret: env("NOTION_CLIENT_SECRET"),
  };
}

function notionNotionAuthHeaders(): HeadersInit {
  const { clientId, clientSecret } = requireNotionConfig();
  const auth = Buffer.from(`${clientId}:${clientSecret}`).toString("base64");
  return {
    Authorization: `Basic ${auth}`,
    "Notion-Version": "2022-06-28",
  };
}

function extractNotionPageId(rawUrl: string): string | null {
  try {
    const url = new URL(rawUrl);
    if (!url.hostname.includes("notion.so")) return null;
    const segments = url.pathname.split("/").filter(Boolean);
    if (segments.length === 0) return null;

    const candidate = segments[segments.length - 1]
      .split("?")[0]
      .split("#")[0]
      .replace(/\.html$/i, "");
    const normalizedCandidate = candidate.replace(/[^0-9a-fA-F]/g, "");
    if (!/^[0-9a-fA-F]{32}$/.test(normalizedCandidate)) return null;

    return normalizedCandidate;
  } catch {
    return null;
  }
}

function normalizeNotionId(rawId: string): string {
  return rawId.toLowerCase();
}

function buildSourceUrl(sourceId: string): string {
  return `https://www.notion.so/${sourceId.replace(/-/g, "")}`;
}

function extractTitleFromProperties(properties: Record<string, unknown>): string | null {
  const values = Object.values(properties);
  for (const value of values) {
    if (!value || typeof value !== "object") continue;
    const item = value as { type?: string; title?: Array<{ plain_text?: string }> ; rich_text?: Array<{ plain_text?: string }> };
    if (item.type === "title" && Array.isArray(item.title)) {
      const title = item.title.map((entry) => entry?.plain_text ?? "").join("");
      if (title.trim()) return title.trim();
    }
    if (item.type === "rich_text" && Array.isArray(item.rich_text)) {
      const title = item.rich_text.map((entry) => entry?.plain_text ?? "").join("");
      if (title.trim()) return title.trim();
    }
  }
  return null;
}

function buildTextFromBlocks(blocks: NotionBlockResponse["results"]): string {
  const lines: string[] = [];
  for (const block of blocks ?? []) {
    if (block.type === "paragraph" && Array.isArray(block.paragraph?.rich_text)) {
      const line = block.paragraph?.rich_text.map((entry) => entry?.plain_text ?? "").join("") ?? "";
      if (line.trim()) lines.push(line.trim());
    }
  }
  return lines.join("\n");
}

async function notionApiRequest(url: string, accessToken: string, options: RequestInit = {}) {
  const response = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Notion-Version": "2022-06-28",
      ...(options.headers ?? {}),
    },
  });
  if (!response.ok) {
    throw new Error(`Notion API request failed: ${response.status}`);
  }
  return response.json();
}

async function requestNotionToken(payload: Record<string, string>): Promise<NotionTokenResponse> {
  const response = await fetch(NOTION_TOKEN_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...notionNotionAuthHeaders(),
    },
    body: JSON.stringify(payload),
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(`Notion token request failed: ${response.status}`);
  }
  return result as NotionTokenResponse;
}

async function requestNotionTokenRevocation(accessToken: string) {
  const response = await fetch("https://api.notion.com/v1/oauth/revoke", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...notionNotionAuthHeaders(),
    },
    body: JSON.stringify({ token: accessToken }),
  });
  if (!response.ok) {
    throw new Error(`Notion revoke request failed: ${response.status}`);
  }
}

export async function buildAuthorizeUrl(state: string, redirectUri: string): Promise<string> {
  const { clientId } = requireNotionConfig();
  const scope = process.env.NOTION_OAUTH_SCOPE ?? "read_content";
  const params = new URLSearchParams({
    client_id: clientId,
    redirect_uri: redirectUri,
    response_type: "code",
    owner: "user",
    state,
    scope,
  });
  return `${NOTION_AUTH_URL}?${params.toString()}`;
}

export async function exchangeCodeForTokens(code: string, redirectUri: string): Promise<ExternalTokenResult> {
  const result = await requestNotionToken({
    grant_type: "authorization_code",
    code,
    redirect_uri: redirectUri,
  });

  if (!result.access_token) {
    throw new Error("Notion token exchange missing access token");
  }

  return {
    accessToken: result.access_token,
    refreshToken: result.refresh_token ?? null,
    expiresAt: result.expires_in ? new Date(Date.now() + result.expires_in * 1000).toISOString() : null,
    tokenType: result.token_type ?? "Bearer",
    scope: result.scope ?? null,
    providerAccountId: result.owner?.user?.id ?? null,
  };
}

export async function refreshAccessToken(refreshToken: string): Promise<ExternalTokenResult> {
  const result = await requestNotionToken({
    grant_type: "refresh_token",
    refresh_token: refreshToken,
  });

  if (!result.access_token) {
    throw new Error("Notion token refresh missing access token");
  }

  return {
    accessToken: result.access_token,
    refreshToken: result.refresh_token ?? refreshToken,
    expiresAt: result.expires_in ? new Date(Date.now() + result.expires_in * 1000).toISOString() : null,
    tokenType: result.token_type ?? "Bearer",
    scope: result.scope ?? null,
    providerAccountId: result.owner?.user?.id ?? null,
  };
}

export async function fetchDocumentByUrl(sourceUrl: string, accessToken: string): Promise<ExternalDocumentResult> {
  const sourceLookup = parseSourceUrl(sourceUrl);
  const page = await notionApiRequest(
    `${NOTION_PAGE_URL}/${encodeURIComponent(sourceLookup.sourceExternalId)}`,
    accessToken,
  ) as NotionPageResponse;

  const properties = (page.properties ?? {}) as Record<string, unknown>;
  const title = extractTitleFromProperties(properties) ?? "Notion Page";

  const blockResult = await notionApiRequest(
    `${NOTION_BLOCKS_URL}/${encodeURIComponent(sourceLookup.sourceExternalId)}/children?page_size=80`,
    accessToken,
  ) as NotionBlockResponse;
  const snippet = buildTextFromBlocks(blockResult.results);
  const text = snippet ? `# ${title}\n\n${snippet}` : `Notion page: ${title}`;

  return {
    sourceExternalId: sourceLookup.sourceExternalId,
    sourceUrl: sourceLookup.sourceUrl,
    title,
    text,
    sourceMetadata: {
      provider: "notion",
      id: sourceLookup.sourceExternalId,
      url: page.url ?? sourceUrl,
      title,
    },
  };
}

export function parseSourceUrl(sourceUrl: string): SourceLookupResult {
  const id = extractNotionPageId(sourceUrl);
  if (!id) throw new Error("Invalid Notion URL");
  const normalized = normalizeNotionId(id);
  return {
    sourceExternalId: normalized,
    sourceUrl: buildSourceUrl(normalized),
  };
}

export async function revokeAccessToken(token: string): Promise<void> {
  await requestNotionTokenRevocation(token);
}
