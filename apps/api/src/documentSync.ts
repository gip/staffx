import { createHash, randomUUID } from "node:crypto";
import { query } from "./db.js";
import { decryptToken, encryptToken } from "./integrations/crypto.js";
import {
  getProviderClient,
  sourceTypeToProvider,
  type DocSourceType,
} from "./integrations/index.js";

type IntegrationProvider = "notion" | "google";
type IntegrationStatus = "connected" | "disconnected" | "expired" | "needs_reauth";

interface SyncDocumentRow {
  system_id: string;
  kind: "Document" | "Skill";
  title: string;
  language: string;
  current_hash: string;
  source_type: "notion" | "google_doc";
  source_url: string;
  source_external_id: string;
  source_connected_user_id: string;
}

interface IntegrationAccessRow {
  status: IntegrationStatus;
  access_token_enc: string | null;
  refresh_token_enc: string | null;
  token_expires_at: Date | null;
}

interface SyncCandidate {
  provider: IntegrationProvider;
  sourceType: Exclude<DocSourceType, "local">;
  sourceUrl: string;
  sourceExternalId: string;
  sourceConnectedUserId: string;
  documents: SyncDocumentRow[];
}

interface StartSyncOptions {
  logger?: {
    info(data: Record<string, unknown>, message: string): void;
    warn(data: Record<string, unknown>, message: string): void;
    error(data: Record<string, unknown>, message: string): void;
  };
}

interface SyncRunResult {
  scanned: number;
  changed: number;
  failed: number;
}

const SYNC_INTERVAL_MS = 30_000;

let intervalId: ReturnType<typeof setInterval> | null = null;
let inFlightSync: Promise<SyncRunResult> | null = null;
let isStopped = false;

function computeDocumentHash(document: {
  kind: "Document" | "Skill";
  title: string;
  language: string;
  body: string;
}) {
  const payload = [document.kind, document.title, document.language, document.body].join("\n");
  return `sha256:${createHash("sha256").update(payload, "utf8").digest("hex")}`;
}

function buildLogger(providedLogger?: StartSyncOptions["logger"]) {
  if (providedLogger) {
    return providedLogger;
  }

  return {
    info: (data: Record<string, unknown>, message: string) => {
      console.log(`[document-sync] ${message}`, data);
    },
    warn: (data: Record<string, unknown>, message: string) => {
      console.warn(`[document-sync] ${message}`, data);
    },
    error: (data: Record<string, unknown>, message: string) => {
      console.error(`[document-sync] ${message}`, data);
    },
  };
}

function isTokenExpired(tokenExpiresAt: Date | null): boolean {
  if (!(tokenExpiresAt instanceof Date)) return false;
  return tokenExpiresAt.getTime() <= Date.now();
}

function buildCandidateKey(
  sourceConnectedUserId: string,
  sourceType: Exclude<DocSourceType, "local">,
  sourceExternalId: string,
) {
  return `${sourceConnectedUserId}:${sourceType}:${sourceExternalId}`;
}

async function markIntegrationNeedsReauth(userId: string, provider: IntegrationProvider) {
  await query(
    `UPDATE user_integrations
       SET status = 'needs_reauth',
           updated_at = now()
       WHERE user_id = $1 AND provider = $2`,
    [userId, provider],
  );
}

async function getIntegrationAccessToken(
  sourceConnectedUserId: string,
  sourceType: Exclude<DocSourceType, "local">,
): Promise<string | null> {
  const provider = sourceTypeToProvider(sourceType);
  const client = getProviderClient(provider);

  const result = await query<IntegrationAccessRow>(
    `SELECT status, access_token_enc, refresh_token_enc, token_expires_at
       FROM user_integrations
      WHERE user_id = $1 AND provider = $2`,
    [sourceConnectedUserId, provider],
  );

  if (result.rowCount === 0) {
    return null;
  }

  const row = result.rows[0];
  if (row.status === "disconnected" || row.status === "needs_reauth") {
    return null;
  }

  if (!row.access_token_enc) {
    await markIntegrationNeedsReauth(sourceConnectedUserId, provider);
    return null;
  }

  if (!isTokenExpired(row.token_expires_at)) {
    return decryptToken(row.access_token_enc);
  }

  if (!row.refresh_token_enc) {
    await markIntegrationNeedsReauth(sourceConnectedUserId, provider);
    return null;
  }

  try {
    const refreshResult = await client.refreshAccessToken(decryptToken(row.refresh_token_enc));
    await query(
      `UPDATE user_integrations
         SET access_token_enc = $3,
             refresh_token_enc = COALESCE($4, refresh_token_enc),
             token_expires_at = $5,
             status = 'connected',
             updated_at = now(),
             disconnected_at = NULL
       WHERE user_id = $1 AND provider = $2`,
      [
        sourceConnectedUserId,
        provider,
        encryptToken(refreshResult.accessToken),
        refreshResult.refreshToken ? encryptToken(refreshResult.refreshToken) : null,
        refreshResult.expiresAt,
      ],
    );
    return refreshResult.accessToken;
  } catch {
    await markIntegrationNeedsReauth(sourceConnectedUserId, provider);
    return null;
  }
}

async function runSyncCycle(
  logger: StartSyncOptions["logger"],
): Promise<SyncRunResult> {
  const result = await query<SyncDocumentRow>(
    `SELECT system_id, kind, title, language, hash AS current_hash, source_type, source_url, source_external_id, source_connected_user_id
       FROM (
         SELECT
           d.system_id,
           d.kind,
           d.title,
           d.language,
           d.hash,
           d.source_type,
           d.source_url,
           d.source_external_id,
           d.source_connected_user_id,
           row_number() OVER (
             PARTITION BY d.system_id, d.source_type, d.source_connected_user_id, d.source_external_id
             ORDER BY d.created_at DESC
           ) AS row_number
         FROM documents d
         WHERE d.source_type IN ('notion', 'google_doc')
           AND d.source_connected_user_id IS NOT NULL
           AND d.source_external_id IS NOT NULL
           AND d.source_url IS NOT NULL
       ) latest
      WHERE row_number = 1`,
  );

  const rows = result.rows;
  if (rows.length === 0) {
    return { scanned: 0, changed: 0, failed: 0 };
  }

  const candidatesByKey = new Map<string, SyncCandidate>();
  for (const row of rows) {
    const key = buildCandidateKey(row.source_connected_user_id, row.source_type, row.source_external_id);
    const existing = candidatesByKey.get(key);
    if (existing) {
      existing.documents.push(row);
      continue;
    }

    candidatesByKey.set(key, {
      provider: sourceTypeToProvider(row.source_type),
      sourceType: row.source_type,
      sourceUrl: row.source_url,
      sourceExternalId: row.source_external_id,
      sourceConnectedUserId: row.source_connected_user_id,
      documents: [row],
    });
  }

  const accessTokenByKey = new Map<string, string | null>();
  let changed = 0;
  let failed = 0;

  for (const candidate of candidatesByKey.values()) {
    const tokenKey = buildCandidateKey(
      candidate.sourceConnectedUserId,
      candidate.sourceType,
      candidate.sourceExternalId,
    );

    let accessToken = accessTokenByKey.get(tokenKey);
    if (accessToken === undefined) {
      accessToken = await getIntegrationAccessToken(candidate.sourceConnectedUserId, candidate.sourceType);
      accessTokenByKey.set(tokenKey, accessToken);
    }

    if (!accessToken) {
      failed += candidate.documents.length;
      logger?.warn(
        {
          provider: candidate.provider,
          sourceExternalId: candidate.sourceExternalId,
          sourceConnectedUserId: candidate.sourceConnectedUserId,
        },
        "Skipping external document sync due to missing access token",
      );
      continue;
    }

    const providerClient = getProviderClient(candidate.provider);
    let remoteDocument;
    try {
      remoteDocument = await providerClient.fetchDocument(candidate.sourceUrl, accessToken);
    } catch (error: unknown) {
      failed += candidate.documents.length;
      logger?.warn(
        {
          provider: candidate.provider,
          sourceExternalId: candidate.sourceExternalId,
          sourceConnectedUserId: candidate.sourceConnectedUserId,
          error: error instanceof Error ? error.message : "unknown",
        },
        "Failed to pull external document",
      );
      continue;
    }

    for (const row of candidate.documents) {
      const nextHash = computeDocumentHash({
        kind: row.kind,
        title: remoteDocument.title,
        language: row.language,
        body: remoteDocument.text,
      });

      if (nextHash === row.current_hash) {
        continue;
      }

      const insertedDocument = await query<{ hash: string }>(
        `INSERT INTO documents (
           hash,
           system_id,
           kind,
           title,
           language,
           text,
           source_type,
           source_url,
           source_external_id,
           source_metadata,
           source_connected_user_id,
           supersedes
         )
         VALUES ($1, $2, $3::doc_kind, $4, $5, $6, $7::doc_source_type, $8, $9, $10, $11, $12)
         ON CONFLICT (system_id, hash) DO NOTHING
         RETURNING hash`,
        [
          nextHash,
          row.system_id,
          row.kind,
          remoteDocument.title,
          row.language,
          remoteDocument.text,
          row.source_type,
          remoteDocument.sourceUrl,
          row.source_external_id,
          remoteDocument.sourceMetadata,
          row.source_connected_user_id,
          row.current_hash,
        ],
      );

      if (insertedDocument.rowCount === 0) {
        continue;
      }

      await query(
        `INSERT INTO external_document_sync_notifications (
           id,
           system_id,
           source_type,
           source_external_id,
           source_url,
           source_connected_user_id,
           old_document_hash,
           new_document_hash,
           old_title,
           new_title,
           source_metadata
         )
         VALUES ($1, $2, $3::doc_source_type, $4, $5, $6, $7, $8, $9, $10, $11)`,
        [
          randomUUID(),
          row.system_id,
          row.source_type,
          row.source_external_id,
          remoteDocument.sourceUrl,
          row.source_connected_user_id,
          row.current_hash,
          nextHash,
          row.title,
          remoteDocument.title,
          remoteDocument.sourceMetadata,
        ],
      );

      changed += 1;
    }
  }

  return { scanned: rows.length, changed, failed };
}

export function startExternalDocumentSync(options: StartSyncOptions = {}) {
  const logger = buildLogger(options.logger);

  if (intervalId !== null) {
    return async () => {};
  }

  const run = async () => {
    if (isStopped || inFlightSync) return;
    inFlightSync = (async () => {
      const startedAt = Date.now();
      try {
        const stats = await runSyncCycle(logger);
        logger.info(
          {
            durationMs: Date.now() - startedAt,
            scanned: stats.scanned,
            changed: stats.changed,
            failed: stats.failed,
          },
          "Completed external document sync run",
        );
      } catch (error: unknown) {
        logger.error(
          {
            error: error instanceof Error ? error.message : "unknown",
          },
          "External document sync run failed",
        );
      } finally {
        inFlightSync = null;
      }
    })();

    await inFlightSync;
  };

  intervalId = setInterval(() => {
    void run();
  }, SYNC_INTERVAL_MS);
  void run();

  return async () => {
    isStopped = true;
    if (intervalId) {
      clearInterval(intervalId);
      intervalId = null;
    }
    if (inFlightSync) {
      await inFlightSync;
    }
    inFlightSync = null;
    intervalId = null;
  };
}

