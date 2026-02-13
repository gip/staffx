import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  ChevronDown,
  ChevronRight,
  Expand,
  Minimize2,
  Pencil,
  Plus,
  Send,
  X,
} from "lucide-react";
import { Marked } from "marked";
import ReactFlow, {
  Background,
  Controls,
  Handle,
  MarkerType,
  Position,
  useNodesState,
  type NodeMouseHandler,
  type Edge,
  type Node,
  type NodeProps,
} from "reactflow";
import "reactflow/dist/style.css";
import { Link } from "./link";

type DocKind = "Feature" | "Spec" | "Skill";
type MessageRole = "User" | "Assistant" | "System";

export interface ThreadPermissions {
  canEdit: boolean;
  canChat: boolean;
}

export interface TopologyNode {
  id: string;
  name: string;
  kind: string;
  parentId: string | null;
  layoutX?: number | null;
  layoutY?: number | null;
}

export interface TopologyEdge {
  id: string;
  type: string;
  fromNodeId: string;
  toNodeId: string;
  protocol?: string | null;
}

export interface MatrixCellDoc {
  hash: string;
  title: string;
  kind: DocKind;
  language: string;
  refType: DocKind;
}

export interface ArtifactRef {
  id: string;
  type: string;
  language: string;
  text: string | null;
}

export interface MatrixCell {
  nodeId: string;
  concern: string;
  docs: MatrixCellDoc[];
  artifacts: ArtifactRef[];
}

export interface MatrixDocument {
  hash: string;
  kind: DocKind;
  title: string;
  language: string;
  text: string;
}

export interface ChatMessage {
  id: string;
  actionId: string;
  role: MessageRole;
  content: string;
  createdAt: string;
}

export interface ThreadDetail {
  id: string;
  projectThreadId: number;
  title: string;
  description: string | null;
  status: string;
  createdAt: string;
  createdByHandle: string;
  ownerHandle: string;
  projectName: string;
  accessRole: string;
}

export interface ThreadDetailPayload {
  systemId: string;
  thread: ThreadDetail;
  permissions: ThreadPermissions;
  topology: {
    nodes: TopologyNode[];
    edges: TopologyEdge[];
  };
  matrix: {
    concerns: Array<{ name: string; position: number }>;
    nodes: TopologyNode[];
    cells: MatrixCell[];
    documents: MatrixDocument[];
  };
  chat: {
    messages: ChatMessage[];
  };
}

interface MutationError {
  error: string;
}

type MutationResult<T> = T | MutationError | void;

interface MatrixRefInput {
  nodeId: string;
  concern: string;
  docHash: string;
  refType: DocKind;
}

interface MatrixDocumentCreateInput {
  title: string;
  kind: DocKind;
  language: string;
  name: string;
  description: string;
  body: string;
  attach?: {
    nodeId: string;
    concern: string;
    refType: DocKind;
  };
}

interface MatrixDocumentReplaceInput {
  title?: string;
  name?: string;
  description?: string;
  language?: string;
  body?: string;
}

interface MatrixDocumentParsedText {
  name: string;
  description: string;
  body: string;
}

interface MatrixDocumentCreateResponse {
  systemId: string;
  document: MatrixDocument;
  cell?: MatrixCell;
}

interface MatrixDocumentReplaceResponse {
  systemId: string;
  oldHash: string;
  document: MatrixDocument;
  replacedRefs: number;
}

interface MatrixDocGroup {
  feature: MatrixCellDoc[];
  spec: MatrixCellDoc[];
  skill: MatrixCellDoc[];
}

type MatrixDocumentModalSource = "matrix-cell" | "topology-node";

type MatrixDocumentModalMode = "browse" | "create" | "edit";

interface MatrixDocumentModal {
  source: MatrixDocumentModalSource;
  nodeId: string;
  refType: DocKind;
  concern: string;
  kindFilter: "All" | DocKind;
}

interface ThreadPageProps {
  detail: ThreadDetailPayload;
  onUpdateThread?: (payload: { title?: string; description?: string | null }) => Promise<MutationResult<{ thread: ThreadDetail }>>;
  onSaveTopologyLayout?: (payload: { positions: Array<{ nodeId: string; x: number; y: number }> }) => Promise<MutationResult<{ systemId: string }>>;
  onAddMatrixDoc?: (payload: MatrixRefInput) => Promise<MutationResult<{ systemId: string; cell: MatrixCell }>>;
  onRemoveMatrixDoc?: (payload: MatrixRefInput) => Promise<MutationResult<{ systemId: string; cell: MatrixCell }>>;
  onCreateMatrixDocument?: (payload: MatrixDocumentCreateInput) => Promise<MutationResult<MatrixDocumentCreateResponse>>;
  onReplaceMatrixDocument?: (documentHash: string, payload: MatrixDocumentReplaceInput) => Promise<MutationResult<MatrixDocumentReplaceResponse>>;
  onSendChatMessage?: (payload: { content: string }) => Promise<MutationResult<{ messages: ChatMessage[] }>>;
}

const DOC_TYPES: DocKind[] = ["Feature", "Spec", "Skill"];
const DOC_KIND_TO_KEY: Record<DocKind, keyof MatrixDocGroup> = {
  Feature: "feature",
  Spec: "spec",
  Skill: "skill",
};
function chunkIntoPairs<T>(items: T[]): T[][] {
  const rows: T[][] = [];
  for (let index = 0; index < items.length; index += 2) {
    rows.push(items.slice(index, index + 2));
  }
  return rows;
}
const MATRIX_DOC_NAME_PATTERN = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;
const normalizeConcernName = (value: string) => value.trim().toLowerCase();
const DEFAULT_DOCUMENT_LANGUAGE = "en";

function parseDocumentText(rawText: string): MatrixDocumentParsedText {
  const text = (rawText ?? "").replace(/\r\n/g, "\n");
  if (!text.startsWith("---\n")) {
    return { name: "", description: "", body: text.trim() };
  }

  const match = text.match(/^---\n([\s\S]*?)\n---\n?([\s\S]*)$/);
  if (!match) {
    return { name: "", description: "", body: text.trim() };
  }

  const frontMatter = match[1] ?? "";
  const body = (match[2] ?? "").trim();
  const parsed: MatrixDocumentParsedText = { name: "", description: "", body };

  for (const line of frontMatter.split("\n")) {
    const [rawKey, rawValue] = line.split(":", 2);
    if (!rawKey || rawValue === undefined) continue;
    const key = rawKey.trim();
    const value = rawValue.trim();
    if (key === "name") parsed.name = value;
    else if (key === "description") parsed.description = value;
  }

  return parsed;
}

function isValidDocumentName(name: string) {
  return MATRIX_DOC_NAME_PATTERN.test(name);
}

function deriveDocumentName(title: string) {
  return title
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function formatDateTime(value: string) {
  return new Date(value).toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

const markedInstance = new Marked({
  breaks: true,
  gfm: true,
});

function renderMarkdown(source: string): string {
  return markedInstance.parse(source) as string;
}

function timeAgo(value: string) {
  const seconds = Math.floor((Date.now() - new Date(value).getTime()) / 1000);
  if (seconds < 60) return "just now";
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes} minute${minutes === 1 ? "" : "s"} ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours} hour${hours === 1 ? "" : "s"} ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days} day${days === 1 ? "" : "s"} ago`;
  const months = Math.floor(days / 30);
  return `${months} month${months === 1 ? "" : "s"} ago`;
}

function buildMatrixCellKey(nodeId: string, concern: string) {
  return `${nodeId}::${concern}`;
}

function getErrorMessage<T>(result: MutationResult<T>): string | null {
  if (!result || typeof result !== "object") return null;
  if (!("error" in result)) return null;
  const value = result.error;
  return typeof value === "string" ? value : "Request failed";
}

function getStatusLabel(status: string) {
  return status === "open" ? "Working" : status;
}

function sortNodes(a: TopologyNode, b: TopologyNode) {
  const byName = a.name.localeCompare(b.name);
  if (byName !== 0) return byName;
  return a.id.localeCompare(b.id);
}

interface FlowLayoutModel {
  byId: Map<string, TopologyNode>;
  visibleNodes: TopologyNode[];
  hiddenNodeIds: Set<string>;
  visibleParentById: Map<string, string | null>;
  nestedChildrenByHost: Map<string, TopologyNode[]>;
}

interface TopologyNestedChildData {
  id: string;
  name: string;
  kind: string;
  documents: MatrixDocGroup;
}

interface TopologyFlowNodeData {
  nodeId: string;
  name: string;
  kind: string;
  nestedChildren: TopologyNestedChildData[];
  documents: MatrixDocGroup;
  canEdit: boolean;
  onOpenDocPicker: (nodeId: string, refType: DocKind) => void;
  onEditDoc: (doc: MatrixCellDoc, nodeId: string, concern: string) => void;
}

interface FlowEndpoint {
  nodeId: string;
  handleId: string;
}

function TopologyFlowNode({ data }: NodeProps<TopologyFlowNodeData>) {
  const resolveBadgeKindClass = (kind: string) => {
    const normalizedKind = kind.toLowerCase().replace(/\s+/g, "-");
    return ["host", "process", "container", "library"].includes(normalizedKind)
      ? `thread-topology-node-badge--${normalizedKind}`
      : "thread-topology-node-badge--other";
  };

  const nodeKindClass = resolveBadgeKindClass(data.kind);

  const renderDocSections = (nodeLabel: string, nodeId: string, nodeDocuments: MatrixDocGroup) => (
    <div className="thread-topology-doc-sections">
      {DOC_TYPES.map((type) => {
        const key = DOC_KIND_TO_KEY[type];
        const docs = nodeDocuments[key];
        const docRows = chunkIntoPairs(docs);
        return (
          <div key={type} className="thread-topology-doc-section">
            <div className="thread-topology-doc-section-header">
              <span className="matrix-doc-group-label">{type}</span>
              {data.canEdit && (
                <button
                  className="btn-icon thread-topology-doc-add"
                  type="button"
                  aria-label={`Add ${type} document to ${nodeLabel}`}
                  onClick={() => data.onOpenDocPicker(nodeId, type)}
                  title="Add document"
                >
                  <Plus size={12} />
                </button>
              )}
            </div>
            <div className="thread-topology-doc-list">
              {docRows.map((row, rowIndex) => (
                <div key={`${nodeId}-${type}-row-${rowIndex}`} className="matrix-doc-row">
                  {row.map((doc) => (
                    <button
                      className={`matrix-doc-chip matrix-doc-chip--${type.toLowerCase()}`}
                      type="button"
                      key={`${nodeId}-${doc.hash}-${doc.refType}`}
                      onClick={() => data.onEditDoc(doc, nodeId, "")}
                      disabled={!data.canEdit}
                    >
                      <span>{doc.title}</span>
                    </button>
                  ))}
                </div>
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
  return (
    <div className="thread-topology-node">
      <span className={`thread-topology-node-badge ${nodeKindClass}`}>{data.kind}</span>
      <Handle
        type="target"
        position={Position.Left}
        id="host-in"
        className="thread-topology-handle thread-topology-handle--host"
        style={{ top: 22 }}
      />
      <Handle
        type="source"
        position={Position.Right}
        id="host-out"
        className="thread-topology-handle thread-topology-handle--host"
        style={{ top: 22 }}
      />

      <strong>{data.name}</strong>
      <span>{data.kind}</span>

      {renderDocSections(data.name, data.nodeId, data.documents)}

      {data.nestedChildren.length > 0 && (
        <div className="thread-topology-nested-list">
          {data.nestedChildren.map((child) => (
            <div
              className={`thread-topology-nested-item ${
                child.kind.toLowerCase() === "process" ? "thread-topology-nested-item--process" : ""
              }`}
              key={child.id}
            >
              <span className={`thread-topology-node-badge ${resolveBadgeKindClass(child.kind)}`}>{child.kind}</span>
              <Handle
                type="target"
                position={Position.Left}
                id={`child-in:${child.id}`}
                className="thread-topology-handle thread-topology-handle--child"
                style={{ top: "50%" }}
              />
              <Handle
                type="source"
                position={Position.Right}
                id={`child-out:${child.id}`}
                className="thread-topology-handle thread-topology-handle--child"
                style={{ top: "50%" }}
              />
              <strong>{child.name}</strong>
              <span>{child.kind}</span>
              {renderDocSections(child.name, child.id, child.documents)}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

const FLOW_NODE_TYPES = {
  topology: TopologyFlowNode,
};

function buildFlowLayoutModel(nodes: TopologyNode[]): FlowLayoutModel {
  const byId = new Map(nodes.map((node) => [node.id, node]));
  const hiddenNodeIds = new Set<string>();
  const nestedChildrenByHost = new Map<string, TopologyNode[]>();
  const nestedKinds = new Set(["Container", "Process", "Library"]);

  for (const node of nodes) {
    if (!node.parentId) continue;
    const parent = byId.get(node.parentId);
    if (!parent || parent.kind !== "Host") continue;
    if (!nestedKinds.has(node.kind)) continue;
    hiddenNodeIds.add(node.id);
    const nested = nestedChildrenByHost.get(parent.id) ?? [];
    nested.push(node);
    nestedChildrenByHost.set(parent.id, nested);
  }

  for (const nested of nestedChildrenByHost.values()) {
    nested.sort(sortNodes);
  }

  const visibleNodes = nodes.filter((node) => !hiddenNodeIds.has(node.id));
  const visibleParentById = new Map<string, string | null>();
  for (const node of visibleNodes) {
    let parentId = node.parentId;
    while (parentId && hiddenNodeIds.has(parentId)) {
      parentId = byId.get(parentId)?.parentId ?? null;
    }
    if (parentId && !byId.has(parentId)) {
      parentId = null;
    }
    visibleParentById.set(node.id, parentId);
  }

  return {
    byId,
    visibleNodes,
    hiddenNodeIds,
    visibleParentById,
    nestedChildrenByHost,
  };
}

function buildFlowNodes(
  model: FlowLayoutModel,
  nodeDocuments: Map<string, MatrixDocGroup>,
  onOpenDocPicker: (nodeId: string, refType: DocKind) => void,
  onEditDoc: (doc: MatrixCellDoc, nodeId: string, concern: string) => void,
  canEdit: boolean,
): Node[] {
  if (model.visibleNodes.length === 0) return [];

  const children = new Map<string, TopologyNode[]>();
  for (const node of model.visibleNodes) {
    const parentId = model.visibleParentById.get(node.id);
    if (!parentId) continue;
    const list = children.get(parentId) ?? [];
    list.push(node);
    children.set(parentId, list);
  }

  const depths = new Map<string, number>();
  const roots = model.visibleNodes.filter((node) => !model.visibleParentById.get(node.id)).sort(sortNodes);
  const queue: Array<{ node: TopologyNode; depth: number }> = roots.map((node) => ({ node, depth: 0 }));

  while (queue.length > 0) {
    const next = queue.shift();
    if (!next) break;
    const existing = depths.get(next.node.id);
    if (typeof existing === "number" && existing <= next.depth) continue;
    depths.set(next.node.id, next.depth);

    const childNodes = (children.get(next.node.id) ?? []).sort(sortNodes);
    for (const child of childNodes) {
      queue.push({ node: child, depth: next.depth + 1 });
    }
  }

  for (const node of model.visibleNodes) {
    if (!depths.has(node.id)) {
      depths.set(node.id, 0);
    }
  }

  const columns = new Map<number, TopologyNode[]>();
  for (const node of model.visibleNodes) {
    const depth = depths.get(node.id) ?? 0;
    const list = columns.get(depth) ?? [];
    list.push(node);
    columns.set(depth, list);
  }

  const sortedColumns = Array.from(columns.entries()).sort((a, b) => a[0] - b[0]);
  const positions = new Map<string, { x: number; y: number }>();

  for (const [depth, columnNodes] of sortedColumns) {
    columnNodes.sort(sortNodes);
    columnNodes.forEach((node, index) => {
      positions.set(node.id, {
        x: depth * 320,
        y: index * 180,
      });
    });
  }

  return model.visibleNodes.map((node) => {
    const nestedChildren = (model.nestedChildrenByHost.get(node.id) ?? []).map((child) => ({
      id: child.id,
      name: child.name,
      kind: child.kind,
      documents: nodeDocuments.get(child.id) ?? { feature: [], spec: [], skill: [] },
    }));
    const hasSavedPosition =
      typeof node.layoutX === "number" &&
      Number.isFinite(node.layoutX) &&
      typeof node.layoutY === "number" &&
      Number.isFinite(node.layoutY);
    const defaultPosition = positions.get(node.id) ?? { x: 0, y: 0 };

    return {
      id: node.id,
      type: "topology",
      position: hasSavedPosition ? { x: node.layoutX as number, y: node.layoutY as number } : defaultPosition,
      data: {
        nodeId: node.id,
        name: node.name,
        kind: node.kind,
        nestedChildren,
        documents: nodeDocuments.get(node.id) ?? {
          feature: [],
          spec: [],
          skill: [],
        },
        canEdit,
        onOpenDocPicker,
        onEditDoc,
      },
      style: {
        borderRadius: 10,
        border: "1px solid var(--border)",
        background: "var(--bg-secondary)",
        color: "var(--fg)",
        minWidth: nestedChildren.length > 0 ? 240 : 180,
        padding: 8,
        boxShadow: "none",
      },
    };
  });
}

function resolveFlowEndpoint(
  nodeId: string,
  direction: "source" | "target",
  model: FlowLayoutModel,
): FlowEndpoint | null {
  const endpoint = model.byId.get(nodeId);
  if (!endpoint) return null;

  if (!model.hiddenNodeIds.has(nodeId)) {
    return {
      nodeId,
      handleId: direction === "source" ? "host-out" : "host-in",
    };
  }

  let parentId = endpoint.parentId;
  while (parentId) {
    const parentNode = model.byId.get(parentId);
    if (!parentNode) break;
    if (!model.hiddenNodeIds.has(parentNode.id)) {
      return {
        nodeId: parentNode.id,
        handleId: direction === "source" ? `child-out:${endpoint.id}` : `child-in:${endpoint.id}`,
      };
    }
    parentId = parentNode.parentId;
  }

  return null;
}

function buildFlowEdges(edges: TopologyEdge[], model: FlowLayoutModel): Edge[] {
  const flowEdges: Edge[] = [];
  for (const edge of edges) {
    const source = resolveFlowEndpoint(edge.fromNodeId, "source", model);
    const target = resolveFlowEndpoint(edge.toNodeId, "target", model);
    if (!source || !target) continue;

    flowEdges.push({
      id: edge.id,
      source: source.nodeId,
      sourceHandle: source.handleId,
      target: target.nodeId,
      targetHandle: target.handleId,
      markerEnd: { type: MarkerType.ArrowClosed, width: 16, height: 16 },
      style: { stroke: "var(--fg-muted)", strokeWidth: 1.2 },
      label: edge.protocol?.trim() || edge.type,
      labelStyle: { fill: "var(--fg-muted)", fontSize: 11 },
      zIndex: 1000,
    });
  }
  return flowEdges;
}

export function ThreadPage({
  detail,
  onUpdateThread,
  onSaveTopologyLayout,
  onAddMatrixDoc,
  onRemoveMatrixDoc,
  onCreateMatrixDocument,
  onReplaceMatrixDocument,
  onSendChatMessage,
}: ThreadPageProps) {
  const [isTopologyCollapsed, setIsTopologyCollapsed] = useState(false);
  const [isMatrixCollapsed, setIsMatrixCollapsed] = useState(true);
  const [isChatCollapsed, setIsChatCollapsed] = useState(false);
  const [isDescriptionEditing, setIsDescriptionEditing] = useState(false);
  const [isTitleEditing, setIsTitleEditing] = useState(false);
  const [titleDraft, setTitleDraft] = useState(detail.thread.title);
  const [titleError, setTitleError] = useState("");
  const [isSavingTitle, setIsSavingTitle] = useState(false);
  const [descriptionDraft, setDescriptionDraft] = useState(detail.thread.description ?? "");
  const [descriptionTab, setDescriptionTab] = useState<"write" | "preview">("write");
  const [descriptionError, setDescriptionError] = useState("");
  const [isSavingDescription, setIsSavingDescription] = useState(false);
  const [visibleConcerns, setVisibleConcerns] = useState<Set<string>>(
    () => new Set(detail.matrix.concerns.map((c) => c.name)),
  );
  const [matrixError, setMatrixError] = useState("");
  const [activeMatrixMutation, setActiveMatrixMutation] = useState("");
  const [chatInput, setChatInput] = useState("");
  const [chatError, setChatError] = useState("");
  const [isSendingChat, setIsSendingChat] = useState(false);
  const [topologyError, setTopologyError] = useState("");
  const [isSavingTopologyLayout, setIsSavingTopologyLayout] = useState(false);
  const [documentModal, setDocumentModal] = useState<(MatrixDocumentModal & {
    mode: MatrixDocumentModalMode;
    selectedConcern: string;
  }) | null>(null);
  const [docPickerSearch, setDocPickerSearch] = useState("");
  const [docPickerKindFilter, setDocPickerKindFilter] = useState<"All" | DocKind>("All");
  const [docModalMarkdownTab, setDocModalMarkdownTab] = useState<"write" | "preview">("write");
  const [docModalName, setDocModalName] = useState("");
  const [docModalTitle, setDocModalTitle] = useState("");
  const [docModalDescription, setDocModalDescription] = useState("");
  const [docModalLanguage, setDocModalLanguage] = useState("en");
  const [docModalBody, setDocModalBody] = useState("");
  const [docModalValidationError, setDocModalValidationError] = useState("");
  const [docModalEditHash, setDocModalEditHash] = useState<string | null>(null);
  const [isDocumentModalBusy, setIsDocumentModalBusy] = useState(false);
  const [documentModalError, setDocumentModalError] = useState("");

  const titleInputRef = useRef<HTMLInputElement>(null);
  const topologyPanelRef = useRef<HTMLDivElement>(null);
  const matrixPanelRef = useRef<HTMLDivElement>(null);
  const [isTopologyFullscreen, setIsTopologyFullscreen] = useState(false);
  const [isMatrixFullscreen, setIsMatrixFullscreen] = useState(false);

  useEffect(() => {
    if (!isTitleEditing) {
      setTitleDraft(detail.thread.title);
    }
  }, [detail.thread.title, isTitleEditing]);

  useEffect(() => {
    if (!isDescriptionEditing) {
      setDescriptionDraft(detail.thread.description ?? "");
    }
  }, [detail.thread.description, isDescriptionEditing]);

  useEffect(() => {
    if (isTitleEditing && titleInputRef.current) {
      titleInputRef.current.focus();
      titleInputRef.current.select();
    }
  }, [isTitleEditing]);

  useEffect(() => {
    const names = detail.matrix.concerns.map((c) => c.name);
    setVisibleConcerns((prev) => {
      const nextSet = new Set<string>();
      for (const name of names) {
        if (prev.has(name)) nextSet.add(name);
      }
      return nextSet.size > 0 ? nextSet : new Set(names);
    });
  }, [detail.matrix.concerns]);

  const toggleConcern = useCallback((name: string) => {
    setVisibleConcerns((prev) => {
      const next = new Set(prev);
      if (next.has(name)) {
        next.delete(name);
      } else {
        next.add(name);
      }
      return next.size > 0 ? next : prev;
    });
  }, []);

  useEffect(() => {
    const onFullscreenChange = () => {
      setIsTopologyFullscreen(document.fullscreenElement === topologyPanelRef.current);
      setIsMatrixFullscreen(document.fullscreenElement === matrixPanelRef.current);
    };

    document.addEventListener("fullscreenchange", onFullscreenChange);
    return () => document.removeEventListener("fullscreenchange", onFullscreenChange);
  }, []);

  const nodeDocumentGroups = useMemo(() => {
    const groups = new Map<string, MatrixDocGroup>();
    const seenHashes = new Map<string, { feature: Set<string>; spec: Set<string>; skill: Set<string> }>();
    for (const node of detail.topology.nodes) {
      groups.set(node.id, {
        feature: [],
        spec: [],
        skill: [],
      });
      seenHashes.set(node.id, {
        feature: new Set(),
        spec: new Set(),
        skill: new Set(),
      });
    }

    for (const cell of detail.matrix.cells) {
      const nodeGroup = groups.get(cell.nodeId);
      const nodeSeen = seenHashes.get(cell.nodeId);
      if (!nodeGroup || !nodeSeen) continue;

      for (const doc of cell.docs) {
        const key = DOC_KIND_TO_KEY[doc.refType];
        if (nodeSeen[key].has(doc.hash)) continue;
        nodeSeen[key].add(doc.hash);
        nodeGroup[key].push(doc);
      }
    }

    return groups;
  }, [detail.topology.nodes, detail.matrix.cells]);

  const resetDocumentModal = useCallback(() => {
    setDocumentModal(null);
    setDocPickerSearch("");
    setDocPickerKindFilter("All");
    setDocModalMarkdownTab("write");
    setDocModalName("");
    setDocModalTitle("");
    setDocModalDescription("");
    setDocModalLanguage(DEFAULT_DOCUMENT_LANGUAGE);
    setDocModalBody("");
    setDocModalValidationError("");
    setDocModalEditHash(null);
    setDocumentModalError("");
    setIsDocumentModalBusy(false);
  }, []);

  const openDocumentPicker = useCallback(
    (next: MatrixDocumentModal, mode: MatrixDocumentModalMode, editHash: string | null = null) => {
      const initialConcern = next.concern ?? "";
      setDocumentModal({
        ...next,
        selectedConcern: initialConcern,
        mode,
      });
      setDocPickerSearch("");
      setDocPickerKindFilter(next.kindFilter);
      setDocModalMarkdownTab("write");
      setDocumentModalError("");
      setDocModalValidationError("");
      setDocModalEditHash(editHash);

      if (mode === "create") {
        setDocModalName("");
        setDocModalTitle("");
        setDocModalDescription("");
        setDocModalLanguage(DEFAULT_DOCUMENT_LANGUAGE);
        setDocModalBody("");
      } else if (mode === "edit") {
        const existingDocument = detail.matrix.documents.find((doc) => doc.hash === editHash);
        const parsed = existingDocument ? parseDocumentText(existingDocument.text) : { name: "", description: "", body: "" };
        const existingName = parsed.name && isValidDocumentName(parsed.name) ? parsed.name : "";
        setDocModalName(existingName || deriveDocumentName(existingDocument?.title ?? ""));
        setDocModalTitle(existingDocument?.title ?? "");
        setDocModalDescription(parsed.description);
        setDocModalLanguage(existingDocument?.language ?? "en");
        setDocModalBody(parsed.body);
      } else {
        setDocModalName("");
        setDocModalTitle("");
        setDocModalDescription("");
        setDocModalLanguage(DEFAULT_DOCUMENT_LANGUAGE);
        setDocModalBody("");
      }
    },
    [detail.matrix.documents],
  );

  const openMatrixCellDocumentPicker = useCallback(
    (nodeId: string, concern: string, refType: DocKind) => {
      openDocumentPicker(
        {
          source: "matrix-cell",
          nodeId,
          refType,
          concern,
          kindFilter: "All",
        },
        "browse",
      );
    },
    [openDocumentPicker],
  );

  const openTopologyDocumentPicker = useCallback(
    (nodeId: string, refType: DocKind) => {
      const firstConcern = detail.matrix.concerns[0]?.name ?? "";
      const matchingConcern = detail.matrix.concerns.find((concern) => normalizeConcernName(concern.name) === normalizeConcernName(refType))?.name;
      const selectedConcern = matchingConcern ?? (detail.matrix.concerns.length === 1 ? firstConcern : "");
      openDocumentPicker(
        {
          source: "topology-node",
          nodeId,
          refType,
          concern: selectedConcern,
          kindFilter: refType,
        },
        "browse",
      );
    },
    [detail.matrix.concerns, openDocumentPicker],
  );

  const openEditDocumentModal = useCallback(
    (doc: MatrixCellDoc, nodeId: string, concern: string) => {
      if (!detail.matrix.documents.some((current) => current.hash === doc.hash)) return;
      openDocumentPicker(
        {
          source: "matrix-cell",
          nodeId,
          refType: doc.refType,
          concern,
          kindFilter: "All",
        },
        "edit",
        doc.hash,
      );
    },
    [detail.matrix.documents, openDocumentPicker],
  );

  const flowLayoutModel = useMemo(() => buildFlowLayoutModel(detail.topology.nodes), [detail.topology.nodes]);
  const initialFlowNodes = useMemo(
    () =>
      buildFlowNodes(
        flowLayoutModel,
        nodeDocumentGroups,
        openTopologyDocumentPicker,
        openEditDocumentModal,
        detail.permissions.canEdit,
      ),
    [flowLayoutModel, nodeDocumentGroups, openTopologyDocumentPicker, openEditDocumentModal, detail.permissions.canEdit],
  );
  const [flowNodes, setFlowNodes, onFlowNodesChange] = useNodesState(initialFlowNodes);
  const flowEdges = useMemo(() => buildFlowEdges(detail.topology.edges, flowLayoutModel), [detail.topology.edges, flowLayoutModel]);

  useEffect(() => {
    setFlowNodes(initialFlowNodes);
  }, [initialFlowNodes, setFlowNodes]);

  const cellsByKey = useMemo(() => {
    const map = new Map<string, MatrixCell>();
    for (const cell of detail.matrix.cells) {
      map.set(buildMatrixCellKey(cell.nodeId, cell.concern), cell);
    }
    return map;
  }, [detail.matrix.cells]);

  const filteredConcerns = useMemo(
    () => detail.matrix.concerns.filter((c) => visibleConcerns.has(c.name)),
    [detail.matrix.concerns, visibleConcerns],
  );

  const treeOrderedMatrixNodes = useMemo(() => {
    const childrenOf = new Map<string | null, TopologyNode[]>();
    for (const node of detail.matrix.nodes) {
      const key = node.parentId ?? null;
      const list = childrenOf.get(key) ?? [];
      list.push(node);
      childrenOf.set(key, list);
    }
    for (const list of childrenOf.values()) {
      list.sort(sortNodes);
    }

    const result: Array<{ node: TopologyNode; depth: number }> = [];
    const walk = (parentId: string | null, depth: number) => {
      for (const node of childrenOf.get(parentId) ?? []) {
        result.push({ node, depth });
        walk(node.id, depth + 1);
      }
    };
    walk(null, 0);
    return result;
  }, [detail.matrix.nodes]);

  const activeCell = useMemo(() => {
    if (!documentModal) return null;
    return cellsByKey.get(buildMatrixCellKey(documentModal.nodeId, documentModal.selectedConcern)) ?? null;
  }, [documentModal, cellsByKey]);

  const availableDocs = useMemo(() => {
    if (!documentModal || documentModal.mode !== "browse") return [];

    const existingRefs = new Set(
      documentModal.source === "topology-node"
        ? detail.matrix.cells
            .filter((cell) => cell.nodeId === documentModal.nodeId)
            .flatMap((cell) =>
              cell.docs
                .filter((doc) =>
                  documentModal.kindFilter === "All" || doc.refType === documentModal.kindFilter,
                )
                .map((doc) => `${doc.hash}:${doc.refType}`),
            )
        : (activeCell?.docs ?? []).map((doc) => `${doc.hash}:${doc.refType}`),
    );
    const query = docPickerSearch.trim().toLowerCase();

    return detail.matrix.documents.filter((doc) => {
      if (existingRefs.has(`${doc.hash}:${doc.kind}`)) return false;
      if (docPickerKindFilter !== "All" && doc.kind !== docPickerKindFilter) return false;
      if (!query) return true;
      return (
        doc.title.toLowerCase().includes(query) ||
        doc.hash.toLowerCase().includes(query) ||
        doc.language.toLowerCase().includes(query) ||
        doc.text.toLowerCase().includes(query)
      );
    });
  }, [activeCell, detail.matrix.cells, detail.matrix.documents, documentModal, docPickerKindFilter, docPickerSearch]);

  async function toggleFullscreen(ref: { current: HTMLDivElement | null }) {
    if (!ref.current) return;

    try {
      if (document.fullscreenElement === ref.current) {
        await document.exitFullscreen();
      } else {
        await ref.current.requestFullscreen();
      }
    } catch {
      // Ignore fullscreen API failures silently.
    }
  }

  async function handleSaveTitle() {
    if (!onUpdateThread) return;
    const normalizedTitle = titleDraft.trim();
    if (!normalizedTitle) {
      setTitleError("Title cannot be blank.");
      return;
    }
    if (normalizedTitle === detail.thread.title) {
      setIsTitleEditing(false);
      setTitleError("");
      return;
    }

    setTitleError("");
    setIsSavingTitle(true);

    try {
      const result = await onUpdateThread({ title: normalizedTitle });
      const error = getErrorMessage(result);
      if (error) {
        setTitleError(error);
        return;
      }
      setIsTitleEditing(false);
    } catch (error: unknown) {
      if (error instanceof Error && error.message.trim()) {
        setTitleError(error.message);
      } else {
        setTitleError("Failed to save title. Please try again.");
      }
    } finally {
      setIsSavingTitle(false);
    }
  }

  async function handleSaveDescription() {
    if (!onUpdateThread) return;

    setDescriptionError("");
    setIsSavingDescription(true);

    try {
      const result = await onUpdateThread({
        description: descriptionDraft.trim() ? descriptionDraft.trim() : null,
      });

      const error = getErrorMessage(result);
      if (error) {
        setDescriptionError(error);
        return;
      }

      setIsDescriptionEditing(false);
    } catch (error: unknown) {
      if (error instanceof Error && error.message.trim()) {
        setDescriptionError(error.message);
      } else {
        setDescriptionError("Failed to save thread description. Please try again.");
      }
    } finally {
      setIsSavingDescription(false);
    }
  }

  function switchToDocumentCreateMode() {
    if (!documentModal || !detail.permissions.canEdit) return;
    openDocumentPicker(
      {
        source: documentModal.source,
        nodeId: documentModal.nodeId,
        refType: documentModal.refType,
        concern: documentModal.selectedConcern,
        kindFilter: documentModal.refType,
      },
      "create",
    );
    setDocModalTitle("");
    setDocModalDescription("");
    setDocModalName("");
    setDocModalBody("");
    setDocModalLanguage(DEFAULT_DOCUMENT_LANGUAGE);
  }

  async function handleAttachDocument(doc: MatrixDocument) {
    if (!documentModal || !onAddMatrixDoc) return;
    const concern = documentModal.selectedConcern;
    if (documentModal.source === "topology-node" && !concern) {
      setDocModalValidationError("Choose a concern before attaching.");
      return;
    }
    if (!concern) {
      setDocModalValidationError("Missing concern.");
      return;
    }

    const mutationKey = `add:${documentModal.nodeId}:${concern}:${doc.hash}:${doc.kind}`;
    setActiveMatrixMutation(mutationKey);
    setMatrixError("");
    setDocumentModalError("");

    const result = await onAddMatrixDoc({
      nodeId: documentModal.nodeId,
      concern,
      docHash: doc.hash,
      refType: doc.kind,
    });

    setActiveMatrixMutation("");
    const error = getErrorMessage(result);
    if (error) {
      setMatrixError(error);
      setDocumentModalError(error);
      return;
    }

    resetDocumentModal();
  }

  async function handleRemoveDoc(nodeId: string, concern: string, doc: MatrixCellDoc) {
    if (!onRemoveMatrixDoc) return;
    const mutationKey = `remove:${nodeId}:${concern}:${doc.hash}:${doc.refType}`;
    setActiveMatrixMutation(mutationKey);
    setMatrixError("");

    const result = await onRemoveMatrixDoc({
      nodeId,
      concern,
      docHash: doc.hash,
      refType: doc.refType,
    });

    setActiveMatrixMutation("");
    const error = getErrorMessage(result);
    if (error) {
      setMatrixError(error);
    }
  }

  async function handleCreateAndAttachDocument() {
    if (!documentModal || !onCreateMatrixDocument) return;

    const concern = documentModal.selectedConcern;
    if (documentModal.source === "topology-node" && !concern) {
      setDocModalValidationError("Choose a concern before creating.");
      return;
    }
    if (!concern) {
      setDocModalValidationError("Missing concern.");
      return;
    }

    const title = docModalTitle.trim();
    const name = docModalName.trim();
    const description = docModalDescription.trim();
    const language = DEFAULT_DOCUMENT_LANGUAGE;
    const body = docModalBody;

    if (!title) {
      setDocModalValidationError("Title is required.");
      return;
    }
    if (!name || !isValidDocumentName(name)) {
      setDocModalValidationError("Name must be lower-case letters/numbers with dashes and no consecutive or edge dashes.");
      return;
    }

    const payload: MatrixDocumentCreateInput = {
      title,
      kind: documentModal.refType,
      language,
      name,
      description,
      body,
      attach: {
        nodeId: documentModal.nodeId,
        concern,
        refType: documentModal.refType,
      },
    };

    setIsDocumentModalBusy(true);
    setDocumentModalError("");
    setDocModalValidationError("");

    const result = await onCreateMatrixDocument(payload);
    setIsDocumentModalBusy(false);

    const error = getErrorMessage(result);
    if (error) {
      setDocumentModalError(error);
      return;
    }

    resetDocumentModal();
  }

  async function handleReplaceDocument() {
    if (!documentModal || !docModalEditHash || !onReplaceMatrixDocument) return;

    const existing = detail.matrix.documents.find((entry) => entry.hash === docModalEditHash);
    if (!existing) {
      setDocumentModalError("Source document not found.");
      return;
    }

    const title = docModalTitle.trim();
    const name = docModalName.trim();
    const description = docModalDescription.trim();
    const language = docModalLanguage.trim() || "en";
    const body = docModalBody;

    if (!title) {
      setDocModalValidationError("Title is required.");
      return;
    }
    if (!name || !isValidDocumentName(name)) {
      setDocModalValidationError("Name must be lower-case letters/numbers with dashes and no consecutive or edge dashes.");
      return;
    }

    const parsed = parseDocumentText(existing.text);
    const next: MatrixDocumentReplaceInput = {};
    if (title !== existing.title) next.title = title;
    const nextName = name === (parsed.name || deriveDocumentName(existing.title)) ? undefined : name;
    if (typeof nextName === "string") next.name = nextName;
    const nextDescription = description === (parsed.description || "") ? undefined : description;
    if (typeof nextDescription === "string") next.description = nextDescription;
    if (language !== existing.language) next.language = language;
    if (body !== parsed.body) next.body = body;

    if (Object.keys(next).length === 0) {
      setDocModalValidationError("No changes to save.");
      return;
    }

    setIsDocumentModalBusy(true);
    setDocumentModalError("");
    setDocModalValidationError("");

    const result = await onReplaceMatrixDocument(docModalEditHash, next);
    setIsDocumentModalBusy(false);

    const error = getErrorMessage(result);
    if (error) {
      setDocumentModalError(error);
      return;
    }

    resetDocumentModal();
  }

  async function handleUnlinkDocument() {
    if (!documentModal || !docModalEditHash || !onRemoveMatrixDoc) return;

    const references = detail.matrix.cells.flatMap((cell) =>
      cell.docs
        .filter((doc) => doc.hash === docModalEditHash)
        .map((doc) => ({
          nodeId: cell.nodeId,
          concern: cell.concern,
          docHash: doc.hash,
          refType: doc.refType,
        })),
    );

    if (references.length === 0) {
      setDocModalValidationError("This document is not linked to any matrix cell.");
      return;
    }

    const dedupedReferences = Array.from(
      new Map(
        references.map((reference) => [
          `${reference.nodeId}::${reference.concern}::${reference.refType}`,
          reference,
        ]),
      ).values(),
    );

    setIsDocumentModalBusy(true);
    setDocumentModalError("");
    setDocModalValidationError("");
    setMatrixError("");
    setActiveMatrixMutation(`unlink:${docModalEditHash}`);

    let errorMessage = "";
    for (const reference of dedupedReferences) {
      const result = await onRemoveMatrixDoc(reference);
      const nextError = getErrorMessage(result);
      if (nextError) {
        errorMessage = nextError;
        break;
      }
    }

    setActiveMatrixMutation("");
    setIsDocumentModalBusy(false);

    if (errorMessage) {
      setDocumentModalError(errorMessage);
      return;
    }

    resetDocumentModal();
  }

  async function handleSendChat() {
    if (!onSendChatMessage || !detail.permissions.canChat || !chatInput.trim()) return;
    setChatError("");
    setIsSendingChat(true);

    const result = await onSendChatMessage({ content: chatInput.trim() });
    setIsSendingChat(false);
    const error = getErrorMessage(result);
    if (error) {
      setChatError(error);
      return;
    }

    setChatInput("");
  }

  const handleNodeDragStop = useCallback<NodeMouseHandler>(
    async (_event, node) => {
      if (!detail.permissions.canEdit || !onSaveTopologyLayout) return;
      setTopologyError("");
      setIsSavingTopologyLayout(true);
      const result = await onSaveTopologyLayout({
        positions: [{ nodeId: node.id, x: node.position.x, y: node.position.y }],
      });
      setIsSavingTopologyLayout(false);
      const error = getErrorMessage(result);
      if (error) {
        setTopologyError(error);
      }
    },
    [detail.permissions.canEdit, onSaveTopologyLayout],
  );

  return (
    <main className="page thread-view-page">
      <nav className="thread-view-breadcrumb">
        <Link to={`/${detail.thread.ownerHandle}/${detail.thread.projectName}`} className="page-back">
          &larr;
        </Link>
        <span className="thread-view-breadcrumb-text">
          <span className="page-title-muted">
            {detail.thread.ownerHandle} / {detail.thread.projectName}
          </span>
        </span>
      </nav>

      <div className="thread-view-title-row">
        {isTitleEditing ? (
          <div className="thread-view-title-wrap">
            <input
              ref={titleInputRef}
              className="thread-view-title-input"
              value={titleDraft}
              onChange={(e) => setTitleDraft(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  e.preventDefault();
                  handleSaveTitle();
                } else if (e.key === "Escape") {
                  setIsTitleEditing(false);
                  setTitleDraft(detail.thread.title);
                  setTitleError("");
                }
              }}
              onBlur={handleSaveTitle}
              maxLength={200}
              disabled={isSavingTitle}
            />
            {titleError && <p className="field-error">{titleError}</p>}
          </div>
        ) : (
          <>
            <h1 className="thread-view-title">
              {detail.thread.title}{" "}
              <span className="thread-view-title-number">#{detail.thread.projectThreadId}</span>
            </h1>
            {detail.permissions.canEdit && (
              <button
                className="btn btn-secondary thread-view-edit-btn"
                type="button"
                onClick={() => { setIsTitleEditing(true); setTitleError(""); }}
              >
                Edit
              </button>
            )}
          </>
        )}
      </div>

      <div className="thread-view-meta">
        <span className={`thread-status thread-status--${detail.thread.status}`}>
          {getStatusLabel(detail.thread.status)}
        </span>
        <span className="thread-view-meta-text">
          <strong>{detail.thread.createdByHandle}</strong> opened this thread {timeAgo(detail.thread.createdAt)}
        </span>
      </div>

      <section className="thread-card thread-description-card">
        {detail.permissions.canEdit && !isDescriptionEditing && (
          <button
            className="btn-icon thread-card-action thread-description-edit"
            type="button"
            onClick={() => {
              setIsDescriptionEditing(true);
              setDescriptionTab("write");
              setDescriptionError("");
            }}
            aria-label="Edit description"
          >
            <Pencil size={16} />
          </button>
        )}

        {isDescriptionEditing ? (
          <div className="thread-description-editor">
            <div className="md-tabs">
              <button
                type="button"
                className={`md-tab${descriptionTab === "write" ? " md-tab--active" : ""}`}
                onClick={() => setDescriptionTab("write")}
              >
                Write
              </button>
              <button
                type="button"
                className={`md-tab${descriptionTab === "preview" ? " md-tab--active" : ""}`}
                onClick={() => setDescriptionTab("preview")}
              >
                Preview
              </button>
            </div>
            {descriptionTab === "write" ? (
              <textarea
                className="field-input field-textarea md-textarea"
                rows={8}
                value={descriptionDraft}
                onChange={(event) => setDescriptionDraft(event.target.value)}
                placeholder="Add a description (Markdown supported)"
              />
            ) : (
              <div className="md-preview">
                {descriptionDraft.trim() ? (
                  <div
                    className="md-body"
                    dangerouslySetInnerHTML={{ __html: renderMarkdown(descriptionDraft) }}
                  />
                ) : (
                  <p className="thread-description-text">Nothing to preview</p>
                )}
              </div>
            )}
            {descriptionError && <p className="field-error">{descriptionError}</p>}
            <div className="thread-inline-actions">
              <button
                className="btn btn-secondary"
                type="button"
                onClick={() => {
                  setIsDescriptionEditing(false);
                  setDescriptionDraft(detail.thread.description ?? "");
                  setDescriptionError("");
                }}
              >
                Cancel
              </button>
              <button
                className="btn"
                type="button"
                onClick={handleSaveDescription}
                disabled={isSavingDescription}
              >
                {isSavingDescription ? "Saving…" : "Save"}
              </button>
            </div>
          </div>
        ) : detail.thread.description ? (
          <div
            className="md-body thread-description-body"
            dangerouslySetInnerHTML={{ __html: renderMarkdown(detail.thread.description) }}
          />
        ) : (
          <p className="thread-description-text">No description provided yet.</p>
        )}
      </section>

      <section
        ref={topologyPanelRef}
        className={`thread-card thread-collapsible ${isTopologyFullscreen ? "thread-card--fullscreen" : ""}`}
      >
        <div className="thread-card-header" onClick={() => setIsTopologyCollapsed((current) => !current)}>
          <h3>Topology View</h3>
          <div className="thread-card-actions">
            <button
              className="btn-icon thread-card-action"
              type="button"
              onClick={(e) => { e.stopPropagation(); toggleFullscreen(topologyPanelRef); }}
              aria-label={isTopologyFullscreen ? "Exit fullscreen topology" : "Enter fullscreen topology"}
            >
              {isTopologyFullscreen ? <Minimize2 size={16} /> : <Expand size={16} />}
            </button>
            <span className="thread-card-action thread-collapse-icon" aria-hidden>
              {isTopologyCollapsed ? <ChevronRight size={16} /> : <ChevronDown size={16} />}
            </span>
          </div>
        </div>

        {!isTopologyCollapsed && (
          <div className="thread-card-body">
            <div className="thread-topology-canvas">
              <ReactFlow
                nodes={flowNodes}
                edges={flowEdges}
                nodeTypes={FLOW_NODE_TYPES}
                onNodesChange={onFlowNodesChange}
                onNodeDragStop={handleNodeDragStop}
                fitView
                nodesDraggable={detail.permissions.canEdit && Boolean(onSaveTopologyLayout)}
                nodesConnectable={false}
                elementsSelectable={false}
                deleteKeyCode={null}
              >
                <Background gap={14} size={1} />
                <Controls />
              </ReactFlow>
            </div>
            {isSavingTopologyLayout && <p className="matrix-empty">Saving layout…</p>}
            {topologyError && <p className="field-error">{topologyError}</p>}
          </div>
        )}
      </section>

      <section
        ref={matrixPanelRef}
        className={`thread-card thread-collapsible ${isMatrixFullscreen ? "thread-card--fullscreen" : ""}`}
      >
        <div className="thread-card-header" onClick={() => setIsMatrixCollapsed((current) => !current)}>
          <h3>Matrix View</h3>
          <div className="thread-card-actions">
            <button
              className="btn-icon thread-card-action"
              type="button"
              onClick={(e) => { e.stopPropagation(); toggleFullscreen(matrixPanelRef); }}
              aria-label={isMatrixFullscreen ? "Exit fullscreen matrix" : "Enter fullscreen matrix"}
            >
              {isMatrixFullscreen ? <Minimize2 size={16} /> : <Expand size={16} />}
            </button>
            <span className="thread-card-action thread-collapse-icon" aria-hidden>
              {isMatrixCollapsed ? <ChevronRight size={16} /> : <ChevronDown size={16} />}
            </span>
          </div>
        </div>

        {!isMatrixCollapsed && (
          <div className="thread-card-body">
            <div className="matrix-concern-filter">
              <span className="matrix-concern-filter-label">Concerns</span>
              {detail.matrix.concerns.map((c) => (
                <label
                  key={c.name}
                  className={`matrix-concern-chip ${visibleConcerns.has(c.name) ? "matrix-concern-chip--active" : ""}`}
                >
                  <input
                    type="checkbox"
                    checked={visibleConcerns.has(c.name)}
                    onChange={() => toggleConcern(c.name)}
                  />
                  {c.name}
                </label>
              ))}
            </div>
            <div className="matrix-table-wrap">
              <table className="matrix-table">
                <thead>
                  <tr>
                    <th className="matrix-node-header">Node</th>
                    {filteredConcerns.map((concern) => (
                      <th key={concern.name}>{concern.name}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {treeOrderedMatrixNodes.map(({ node, depth }) => {
                    const isSystemNode = node.parentId === null;
                    const displayName = isSystemNode ? "System" : node.name;
                    return (
                    <tr key={node.id}>
                      <th className="matrix-node-cell">
                        <div style={{ paddingLeft: depth * 16 }}>
                          <strong>{displayName}</strong>
                          <span>{node.kind}</span>
                        </div>
                      </th>
                      {filteredConcerns.map((concern) => {
                        const key = buildMatrixCellKey(node.id, concern.name);
                        const cell = cellsByKey.get(key) ?? {
                          nodeId: node.id,
                          concern: concern.name,
                          docs: [],
                          artifacts: [],
                        };

                        const isEmpty = cell.docs.length === 0 && cell.artifacts.length === 0;
                        const activeTypeCount = DOC_TYPES.filter((t) => cell.docs.some((d) => d.refType === t)).length;
                        const showLabels = activeTypeCount > 1;

                        return (
                          <td
                            key={key}
                            className={isEmpty ? "matrix-td-empty" : undefined}
                            onClick={
                              isEmpty && detail.permissions.canEdit
                                ? () => openMatrixCellDocumentPicker(node.id, concern.name, DOC_TYPES[0] as DocKind)
                                : undefined
                            }
                          >
                            <div className="matrix-cell">
                              {DOC_TYPES.map((type) => {
                                const docs = cell.docs.filter((doc) => doc.refType === type);
                                if (docs.length === 0) return null;
                                return (
                                  <div key={type} className="matrix-doc-group">
                                    {showLabels && <span className="matrix-doc-group-label">{type}</span>}
                                    <div className="matrix-doc-list">
                                      {docs.map((doc) => {
                                        const removeKey = `remove:${node.id}:${concern.name}:${doc.hash}:${doc.refType}`;
                                        const isMutating = activeMatrixMutation === removeKey;
                                        return (
                                          <div
                                            key={`${doc.hash}:${doc.refType}`}
                                            className={`matrix-doc-chip matrix-doc-chip--${doc.refType.toLowerCase()}`}
                                            role="button"
                                            tabIndex={0}
                                            onClick={() => {
                                              if (detail.permissions.canEdit) {
                                                openEditDocumentModal(doc, node.id, concern.name);
                                              }
                                            }}
                                            onKeyDown={(event) => {
                                              if (event.key === "Enter" || event.key === " ") {
                                                event.preventDefault();
                                                if (detail.permissions.canEdit) {
                                                  openEditDocumentModal(doc, node.id, concern.name);
                                                }
                                              }
                                            }}
                                          >
                                            <span>{doc.title}</span>
                                            {detail.permissions.canEdit && (
                                              <button
                                                className="matrix-doc-remove"
                                                type="button"
                                                onClick={(event) => {
                                                  event.stopPropagation();
                                                  handleRemoveDoc(node.id, concern.name, doc);
                                                }}
                                                disabled={isMutating}
                                                aria-label={`Remove ${doc.title}`}
                                              >
                                                ×
                                              </button>
                                            )}
                                          </div>
                                        );
                                      })}
                                    </div>
                                  </div>
                                );
                              })}

                              {cell.artifacts.length > 0 && (
                                <div className="matrix-artifacts">
                                  {cell.artifacts.map((artifact) => (
                                    <span key={artifact.id} className="matrix-artifact-badge">
                                      {artifact.type}
                                    </span>
                                  ))}
                                </div>
                              )}

                              {detail.permissions.canEdit && (
                                <button
                                  className="matrix-add-doc-btn"
                                  type="button"
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    openMatrixCellDocumentPicker(node.id, concern.name, DOC_TYPES[0] as DocKind);
                                  }}
                                  aria-label={`Add document to ${displayName} × ${concern.name}`}
                                >
                                  <Plus size={12} />
                                </button>
                              )}
                            </div>
                          </td>
                        );
                      })}
                    </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            {matrixError && <p className="field-error">{matrixError}</p>}
          </div>
        )}
      </section>

      <section className="thread-card thread-collapsible">
        <div className="thread-card-header" onClick={() => setIsChatCollapsed((current) => !current)}>
          <h3>Chat View</h3>
          <div className="thread-card-actions">
            <span className="thread-card-action thread-collapse-icon" aria-hidden>
              {isChatCollapsed ? <ChevronRight size={16} /> : <ChevronDown size={16} />}
            </span>
          </div>
        </div>

        {!isChatCollapsed && (
          <div className={`thread-card-body ${detail.permissions.canChat ? "" : "thread-chat-disabled"}`}>
            <div className="thread-chat-form">
              <textarea
                className="field-input thread-chat-input"
                rows={4}
                placeholder="Ask Ideating anything"
                value={chatInput}
                onChange={(event) => setChatInput(event.target.value)}
                disabled={!detail.permissions.canChat || isSendingChat}
              />
              <button
                className="btn thread-chat-send"
                type="button"
                onClick={handleSendChat}
                disabled={!detail.permissions.canChat || isSendingChat || !chatInput.trim()}
              >
                <Send size={14} /> {isSendingChat ? "Sending…" : "Send"}
              </button>
              {!detail.permissions.canChat && (
                <p className="thread-chat-disabled-copy">Only owners and editors can send messages.</p>
              )}
              {chatError && <p className="field-error">{chatError}</p>}
            </div>

            <h4 className="thread-chat-history-title">Chat History</h4>
            <div className="thread-chat-history">
              {detail.chat.messages.length === 0 ? (
                <p className="matrix-empty">No messages yet</p>
              ) : (
                detail.chat.messages.map((message) => (
                  <article
                    key={message.id}
                    className={`thread-chat-message thread-chat-message--${message.role.toLowerCase()}`}
                  >
                    <header>
                      <strong>{message.role}</strong>
                      <span>{formatDateTime(message.createdAt)}</span>
                    </header>
                    <p>{message.content}</p>
                  </article>
                ))
              )}
            </div>
          </div>
        )}
      </section>

      {documentModal && (
        <div className="modal-overlay" onClick={resetDocumentModal}>
          <div className="modal thread-doc-picker" onClick={(event) => event.stopPropagation()}>
            <div className="thread-doc-picker-header">
              <h3 className="modal-title">
                {documentModal.mode === "create"
                  ? "Create Document"
                  : documentModal.mode === "edit"
                    ? "Edit Document"
                    : "Add Document"}
              </h3>
              <button
                className="btn-icon thread-card-action"
                type="button"
                onClick={resetDocumentModal}
                aria-label="Close add document dialog"
              >
                <X size={16} />
              </button>
            </div>
            <p className="thread-doc-picker-context">
              Node: <strong>{documentModal.nodeId || "selected node"}</strong> · Concern:{" "}
              <strong>{documentModal.selectedConcern || "Select concern"}</strong>
            </p>
            {documentModal.source === "topology-node" && detail.matrix.concerns.length > 0 && (
              <div className="thread-doc-concern">
                <label className="field-label">Concern</label>
                <select
                  className="field-input"
                  value={documentModal.selectedConcern}
                  onChange={(event) => {
                    const nextConcern = event.target.value;
                    setDocumentModal((current) =>
                      current
                        ? {
                            ...current,
                            concern: nextConcern,
                            selectedConcern: nextConcern,
                          }
                        : current,
                    );
                  }}
                >
                  <option value="">Select a concern</option>
                  {detail.matrix.concerns.map((concern) => (
                    <option key={concern.name} value={concern.name}>
                      {concern.name}
                    </option>
                  ))}
                </select>
              </div>
            )}

            {documentModal.mode === "browse" ? (
              <>
                <div className="thread-doc-picker-filters">
                  <input
                    className="field-input"
                    value={docPickerSearch}
                    onChange={(event) => setDocPickerSearch(event.target.value)}
                    placeholder="Search title, hash, language"
                  />
                  <select
                    className="field-input"
                    value={docPickerKindFilter}
                    onChange={(event) => {
                      const nextValue = event.target.value as "All" | DocKind;
                      setDocPickerKindFilter(nextValue);
                    }}
                  >
                    <option value="All">All kinds</option>
                    {DOC_TYPES.map((type) => (
                      <option key={type} value={type}>
                        {type}
                      </option>
                    ))}
                  </select>
                </div>

                {onCreateMatrixDocument && (
                  <div className="thread-inline-actions">
                    <button
                      className="btn btn-secondary"
                      type="button"
                      onClick={switchToDocumentCreateMode}
                      disabled={
                        documentModal.source === "topology-node" &&
                        !documentModal.selectedConcern
                      }
                    >
                      + Create New Document
                    </button>
                  </div>
                )}

                <div className="thread-doc-picker-list">
                  {availableDocs.length === 0 ? (
                    <p className="matrix-empty">No documents available for this cell.</p>
                  ) : (
                    availableDocs.map((doc) => {
                      const addKey = `add:${documentModal.nodeId}:${documentModal.selectedConcern}:${doc.hash}:${doc.kind}`;
                      const isMutating = activeMatrixMutation === addKey;
                      return (
                        <div key={doc.hash} className="thread-doc-picker-row">
                          <div>
                            <strong>{doc.title}</strong>
                            <p>{doc.kind} · {doc.language}</p>
                          </div>
                          <button
                            className="btn"
                            type="button"
                            onClick={() => handleAttachDocument(doc)}
                            disabled={
                              isMutating ||
                              (documentModal.source === "topology-node" && !documentModal.selectedConcern)
                            }
                          >
                            {isMutating ? "Adding…" : "Add"}
                          </button>
                        </div>
                      );
                    })
                  )}
                </div>
              </>
            ) : (
              <>
                <div className="thread-doc-form-fields">
                  <div className="field">
                    <label className="field-label">Name</label>
                    <input
                      className="field-input"
                      value={docModalName}
                      onChange={(event) => {
                        setDocModalName(event.target.value);
                        setDocModalValidationError("");
                      }}
                      placeholder="e.g. login-sso-spec"
                    />
                  </div>
                  <div className="field">
                    <label className="field-label">Title</label>
                    <input
                      className="field-input"
                      value={docModalTitle}
                      onChange={(event) => {
                        setDocModalTitle(event.target.value);
                        setDocModalValidationError("");
                      }}
                    />
                  </div>
                  <div className="field">
                    <label className="field-label">Description</label>
                    <textarea
                      className="field-input field-textarea md-textarea"
                      rows={3}
                      value={docModalDescription}
                      onChange={(event) => {
                        setDocModalDescription(event.target.value);
                        setDocModalValidationError("");
                      }}
                    />
                  </div>
                  <div className="thread-doc-form-markdown">
                    <div className="md-tabs">
                      <button
                        type="button"
                        className={`md-tab${docModalMarkdownTab === "write" ? " md-tab--active" : ""}`}
                        onClick={() => setDocModalMarkdownTab("write")}
                      >
                        Edit
                      </button>
                      <button
                        type="button"
                        className={`md-tab${docModalMarkdownTab === "preview" ? " md-tab--active" : ""}`}
                        onClick={() => setDocModalMarkdownTab("preview")}
                      >
                        View
                      </button>
                    </div>
                    {docModalMarkdownTab === "write" ? (
                      <textarea
                        className="field-input field-textarea md-textarea"
                        rows={8}
                        value={docModalBody}
                        onChange={(event) => setDocModalBody(event.target.value)}
                        placeholder="Write document markdown"
                      />
                    ) : (
                      <div className="md-preview">
                        {docModalBody.trim() ? (
                          <div
                            className="md-body"
                            dangerouslySetInnerHTML={{ __html: renderMarkdown(docModalBody) }}
                          />
                        ) : (
                          <p className="thread-description-text">Nothing to preview</p>
                        )}
                      </div>
                    )}
                  </div>
                </div>
                <div className="thread-inline-actions">
                  <button
                    className="btn btn-secondary"
                    type="button"
                    onClick={() => {
                      if (documentModal.mode === "create") {
                        setDocumentModal((current) => (current ? { ...current, mode: "browse" } : current));
                      } else {
                        resetDocumentModal();
                      }
                    }}
                  >
                    Cancel
                  </button>
                  <button
                    className="btn"
                    type="button"
                    onClick={documentModal.mode === "create" ? handleCreateAndAttachDocument : handleReplaceDocument}
                    disabled={isDocumentModalBusy}
                  >
                    {isDocumentModalBusy
                      ? "Saving…"
                      : documentModal.mode === "create"
                        ? "Create document"
                        : "Save changes"}
                  </button>
                  {documentModal.mode === "edit" && (
                    <button
                      className="btn btn-secondary"
                      type="button"
                      onClick={handleUnlinkDocument}
                      disabled={isDocumentModalBusy}
                    >
                      {isDocumentModalBusy ? "Unlinking…" : "Unlink"}
                    </button>
                  )}
                </div>
              </>
            )}

            {(documentModalError || docModalValidationError) && (
              <p className="field-error">{documentModalError || docModalValidationError}</p>
            )}
          </div>
        </div>
      )}
    </main>
  );
}
