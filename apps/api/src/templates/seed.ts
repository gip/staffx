import { createHash, randomUUID } from "node:crypto";
import type { PoolClient } from "pg";

import type { TemplateDefinition, TemplateDocument } from "./index.js";

type OpenShipNodeKind = "Root" | "Host" | "Container" | "Process" | "Library";

export const OPENSHIP_ROOT_NODE_ID = "s.root";
const OPENSHIP_KEY_PATTERN = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;

function kindToPrefix(kind: OpenShipNodeKind): string {
  if (kind === "Root") return "s";
  if (kind === "Host") return "h";
  if (kind === "Container") return "c";
  if (kind === "Process") return "p";
  return "l";
}

function normalizeOpenShipKey(raw: string): string {
  const normalized = raw
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");

  if (!OPENSHIP_KEY_PATTERN.test(normalized)) {
    throw new Error(`Invalid OpenShip key "${raw}".`);
  }
  return normalized;
}

function buildTypedNodeId(kind: OpenShipNodeKind, key: string): string {
  if (kind === "Root") return OPENSHIP_ROOT_NODE_ID;
  return `${kindToPrefix(kind)}.${key}`;
}

function computeDocumentHash(document: Pick<TemplateDocument, "kind" | "title" | "language" | "text">) {
  const payload = [document.kind, document.title, document.language, document.text].join("\n");
  return `sha256:${createHash("sha256").update(payload, "utf8").digest("hex")}`;
}

export async function seedSystemConcerns(
  client: PoolClient,
  systemId: string,
  concerns: Array<{ name: string; position: number; isBaseline: boolean; scope?: string | null }>,
) {
  for (const concern of concerns) {
    await client.query(
      `INSERT INTO concerns (system_id, name, position, is_baseline, scope)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (system_id, name) DO NOTHING`,
      [systemId, concern.name, concern.position, concern.isBaseline, concern.scope ?? null],
    );
  }
}

export async function seedSystemTemplate(
  client: PoolClient,
  systemId: string,
  rootNodeId: string,
  template: TemplateDefinition,
) {
  const nodeIds = new Map<string, string>([["root", rootNodeId]]);
  const nodeOpenShipKeys = new Map<string, string>();
  const nodeIdToTemplateKey = new Map<string, string>();
  for (const node of template.nodes) {
    if (nodeIds.has(node.key)) {
      throw new Error(`Template "${template.id}" has duplicate node key "${node.key}"`);
    }
    const openShipKey = normalizeOpenShipKey(node.key);
    const typedNodeId = buildTypedNodeId(node.kind, openShipKey);
    const existingTemplateKey = nodeIdToTemplateKey.get(typedNodeId);
    if (existingTemplateKey) {
      throw new Error(
        `Template "${template.id}" has node id collision after normalization: ` +
        `"${existingTemplateKey}" and "${node.key}" both map to "${typedNodeId}"`,
      );
    }
    nodeIds.set(node.key, typedNodeId);
    nodeOpenShipKeys.set(node.key, openShipKey);
    nodeIdToTemplateKey.set(typedNodeId, node.key);
  }

  for (const node of template.nodes) {
    const nodeId = nodeIds.get(node.key);
    const openShipKey = nodeOpenShipKeys.get(node.key);
    const parentKey = node.parentKey ?? "root";
    const parentNodeId = nodeIds.get(parentKey);
    if (!nodeId || !parentNodeId || !openShipKey) {
      throw new Error(
        `Template "${template.id}" references unknown parent "${parentKey}" for node "${node.key}"`,
      );
    }

    const nodeMetadata = {
      ...(node.layout ? { layout: node.layout } : {}),
      openshipKey: openShipKey,
    };

    await client.query(
      `INSERT INTO nodes (id, system_id, kind, name, parent_id, metadata)
       VALUES ($1, $2, $3::node_kind, $4, $5, $6::jsonb)`,
      [
        nodeId,
        systemId,
        node.kind,
        node.name,
        parentNodeId,
        JSON.stringify(nodeMetadata),
      ],
    );
  }

  for (const edge of template.edges) {
    const fromNodeId = nodeIds.get(edge.fromKey);
    const toNodeId = nodeIds.get(edge.toKey);
    if (!fromNodeId || !toNodeId) {
      throw new Error(
        `Template "${template.id}" edge references unknown nodes "${edge.fromKey}" -> "${edge.toKey}"`,
      );
    }

    const metadata = edge.protocol ? JSON.stringify({ protocol: edge.protocol }) : "{}";
    await client.query(
      `INSERT INTO edges (id, system_id, type, from_node_id, to_node_id, metadata)
       VALUES ($1, $2, $3::edge_type, $4, $5, $6::jsonb)`,
      [randomUUID(), systemId, edge.type, fromNodeId, toNodeId, metadata],
    );
  }

  await seedSystemConcerns(client, systemId, template.concerns);
  const concernNames = new Set(template.concerns.map((concern) => concern.name));

  const documentHashes = new Map<string, string>();
  for (const document of template.documents) {
    if (documentHashes.has(document.key)) {
      throw new Error(`Template "${template.id}" has duplicate document key "${document.key}"`);
    }
    const hash = computeDocumentHash(document);
    documentHashes.set(document.key, hash);

    await client.query(
      `INSERT INTO documents (hash, system_id, kind, title, language, text)
       VALUES ($1, $2, $3::doc_kind, $4, $5, $6)
       ON CONFLICT (system_id, hash) DO NOTHING`,
      [hash, systemId, document.kind, document.title, document.language, document.text],
    );
  }

  for (const ref of template.matrixRefs) {
    const nodeId = nodeIds.get(ref.nodeKey);
    const docHash = documentHashes.get(ref.documentKey);
    if (!nodeId) {
      throw new Error(`Template "${template.id}" matrix ref references unknown node "${ref.nodeKey}"`);
    }
    if (!docHash) {
      throw new Error(
        `Template "${template.id}" matrix ref references unknown document "${ref.documentKey}"`,
      );
    }
    if (!concernNames.has(ref.concern)) {
      throw new Error(`Template "${template.id}" matrix ref references unknown concern "${ref.concern}"`);
    }

    await client.query(
      `INSERT INTO matrix_refs (system_id, node_id, concern, ref_type, doc_hash)
       VALUES ($1, $2, $3, $4::ref_type, $5)
       ON CONFLICT DO NOTHING`,
      [systemId, nodeId, ref.concern, ref.refType, docHash],
    );
  }
}
