export type GoogleAuthScope = string;

const GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth";
const GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token";
const GOOGLE_REVOKE_URL = "https://oauth2.googleapis.com/revoke";
const GOOGLE_DOC_URL_PREFIX = "https://docs.google.com/document/";

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

interface GoogleDocResponse {
  title?: string;
  documentId?: string;
  revisionId?: string;
  body?: { content?: Array<unknown> };
}

interface GoogleTokenResponse {
  access_token?: string;
  refresh_token?: string;
  expires_in?: number;
  token_type?: string;
  scope?: string;
}

function env(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function requireGoogleConfig() {
  return {
    clientId: env("GOOGLE_CLIENT_ID"),
    clientSecret: env("GOOGLE_CLIENT_SECRET"),
    defaultScope: "https://www.googleapis.com/auth/documents.readonly",
  };
}

function extractDocumentIdFromUrl(rawUrl: string): string | null {
  try {
    const url = new URL(rawUrl);
    if (!url.hostname.includes("docs.google.com")) return null;
    const match = url.pathname.match(/\/document\/d\/([a-zA-Z0-9_-]+)/);
    return match?.[1] ?? null;
  } catch {
    return null;
  }
}

function normalizeDocumentId(rawId: string): string {
  return rawId.trim();
}

function buildSourceUrl(documentId: string): string {
  return `${GOOGLE_DOC_URL_PREFIX}d/${documentId}/edit`;
}

function toTextFromBodyNode(node: unknown, output: string[]) {
  if (!node || typeof node !== "object") return;

  const typed = node as Record<string, unknown>;
  if (typed.paragraph && typeof typed.paragraph === "object") {
    const paragraph = typed.paragraph as Record<string, unknown>;
    const elements = Array.isArray(paragraph.elements) ? paragraph.elements : [];
    let line = "";
    for (const element of elements) {
      if (!element || typeof element !== "object") continue;
      const child = element as Record<string, unknown>;
      const textRun = child.textRun as { content?: string } | undefined;
      if (typeof textRun?.content === "string") {
        line += textRun.content;
      }
    }
    if (line.trim()) output.push(line.replace(/\n+$/u, ""));
    return;
  }

  if (typed.table && typeof typed.table === "object") {
    const rows = Array.isArray((typed.table as Record<string, unknown>).tableRows)
      ? (typed.table as Record<string, unknown>).tableRows
      : [];
    if (Array.isArray(rows)) {
      for (const row of rows) {
        if (!row || typeof row !== "object") continue;
        const cells = Array.isArray((row as Record<string, unknown>).tableCells)
          ? (row as Record<string, unknown>).tableCells
          : [];
        if (!Array.isArray(cells)) continue;
        for (const cell of cells) {
          toTextFromDocumentNode(cell, output);
        }
      }
    }
    return;
  }

  if (typed.tableRow && Array.isArray((typed.tableRow as Record<string, unknown>).cells)) {
    for (const cell of (typed.tableRow as Record<string, unknown>).cells as unknown[]) {
      toTextFromDocumentNode(cell, output);
    }
    return;
  }

  if (typed.textRun && typeof typed.textRun === "object") {
    const textRun = typed.textRun as { content?: string };
    if (typeof textRun.content === "string") {
      output.push(textRun.content);
    }
    return;
  }

  if (Array.isArray(typed.content)) {
    for (const child of typed.content) {
      toTextFromDocumentNode(child, output);
    }
  }
}

function toTextFromDocumentNode(doc: unknown, output: string[]) {
  if (!doc || typeof doc !== "object") return;
  const node = doc as Record<string, unknown>;
  const maybeContent = Array.isArray(node.content) ? node.content : [];
  for (const child of maybeContent) {
    toTextFromBodyNode(child, output);
  }
  toTextFromBodyNode(doc, output);
}

function buildPlainTextFromDocument(document: GoogleDocResponse): string {
  const output: string[] = [];
  const content = Array.isArray(document.body?.content) ? document.body.content : [];
  for (const node of content) {
    toTextFromBodyNode(node, output);
  }
  return output.join("\n\n").trim();
}

async function requestGoogleToken(payload: Record<string, string>): Promise<GoogleTokenResponse> {
  const { clientId, clientSecret } = requireGoogleConfig();
  const response = await fetch(GOOGLE_TOKEN_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      client_id: clientId,
      client_secret: clientSecret,
      ...payload,
    }),
  });

  const result = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(`Google token request failed: ${response.status}`);
  }
  return result as GoogleTokenResponse;
}

export async function buildAuthorizeUrl(state: string, redirectUri: string, scope?: string) {
  const { clientId, defaultScope } = requireGoogleConfig();
  const params = new URLSearchParams({
    client_id: clientId,
    response_type: "code",
    redirect_uri: redirectUri,
    scope: scope ?? defaultScope,
    access_type: "offline",
    prompt: "consent",
    state,
  });
  return `${GOOGLE_AUTH_URL}?${params.toString()}`;
}

export async function exchangeCodeForTokens(code: string, redirectUri: string): Promise<ExternalTokenResult> {
  const result = await requestGoogleToken({
    grant_type: "authorization_code",
    code,
    redirect_uri: redirectUri,
  });

  if (!result.access_token) throw new Error("Google token exchange missing access token");

  return {
    accessToken: result.access_token,
    refreshToken: result.refresh_token ?? null,
    expiresAt: result.expires_in ? new Date(Date.now() + result.expires_in * 1000).toISOString() : null,
    tokenType: result.token_type ?? "Bearer",
    scope: result.scope ?? null,
    providerAccountId: null,
  };
}

export async function refreshAccessToken(refreshToken: string): Promise<ExternalTokenResult> {
  const result = await requestGoogleToken({
    grant_type: "refresh_token",
    refresh_token: refreshToken,
  });

  if (!result.access_token) throw new Error("Google token refresh missing access token");

  return {
    accessToken: result.access_token,
    refreshToken: result.refresh_token ?? refreshToken,
    expiresAt: result.expires_in ? new Date(Date.now() + result.expires_in * 1000).toISOString() : null,
    tokenType: result.token_type ?? "Bearer",
    scope: result.scope ?? null,
    providerAccountId: null,
  };
}

export async function revokeAccessToken(token: string): Promise<void> {
  await fetch(GOOGLE_REVOKE_URL, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ token }).toString(),
  });
}

export async function fetchDocumentByUrl(sourceUrl: string, accessToken: string): Promise<ExternalDocumentResult> {
  const documentId = extractDocumentIdFromUrl(sourceUrl);
  if (!documentId) {
    throw new Error("Invalid Google Docs URL");
  }

  const normalizedDocumentId = normalizeDocumentId(documentId);
  const response = await fetch(`https://docs.googleapis.com/v1/documents/${encodeURIComponent(normalizedDocumentId)}`, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch Google document: ${response.status}`);
  }

  const doc = (await response.json()) as GoogleDocResponse;
  const plainText = buildPlainTextFromDocument(doc);
  const title = doc.title?.trim() || "Google Document";
  const sourceUrlValue = buildSourceUrl(normalizedDocumentId);
  const text = plainText || "Imported from Google Docs.";

  return {
    sourceExternalId: normalizedDocumentId,
    sourceUrl: sourceUrlValue,
    title,
    text,
    sourceMetadata: {
      provider: "google",
      documentId: normalizedDocumentId,
      sourceUrl: sourceUrlValue,
      title,
      revisionId: doc.revisionId ?? null,
    },
  };
}

export function parseSourceUrl(sourceUrl: string): SourceLookupResult {
  const documentId = extractDocumentIdFromUrl(sourceUrl);
  if (!documentId) {
    throw new Error("Invalid Google Docs URL");
  }
  const normalized = normalizeDocumentId(documentId);
  return {
    sourceExternalId: normalized,
    sourceUrl: buildSourceUrl(normalized),
  };
}
