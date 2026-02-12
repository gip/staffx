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

interface ThreadPageProps {
  detail: ThreadDetailPayload;
  onUpdateThread?: (payload: { title?: string; description?: string | null }) => Promise<MutationResult<{ thread: ThreadDetail }>>;
  onSaveTopologyLayout?: (payload: { positions: Array<{ nodeId: string; x: number; y: number }> }) => Promise<MutationResult<{ systemId: string }>>;
  onAddMatrixDoc?: (payload: MatrixRefInput) => Promise<MutationResult<{ systemId: string; cell: MatrixCell }>>;
  onRemoveMatrixDoc?: (payload: MatrixRefInput) => Promise<MutationResult<{ systemId: string; cell: MatrixCell }>>;
  onSendChatMessage?: (payload: { content: string }) => Promise<MutationResult<{ messages: ChatMessage[] }>>;
}

const DOC_TYPES: DocKind[] = ["Feature", "Spec", "Skill"];

function formatDateTime(value: string) {
  return new Date(value).toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
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

interface TopologyFlowNodeData {
  name: string;
  kind: string;
  nestedChildren: TopologyNode[];
}

interface FlowEndpoint {
  nodeId: string;
  handleId: string;
}

function TopologyFlowNode({ data }: NodeProps<TopologyFlowNodeData>) {
  return (
    <div className="thread-topology-node">
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
      {data.nestedChildren.length > 0 && (
        <div className="thread-topology-nested-list">
          {data.nestedChildren.map((child) => (
            <div className="thread-topology-nested-item" key={child.id}>
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

function buildFlowNodes(model: FlowLayoutModel): Node[] {
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
    const nestedChildren = model.nestedChildrenByHost.get(node.id) ?? [];
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
        name: node.name,
        kind: node.kind,
        nestedChildren,
      },
      style: {
        borderRadius: 10,
        border: "1px solid var(--border)",
        background: "var(--bg-secondary)",
        color: "var(--fg)",
        minWidth: nestedChildren.length > 0 ? 240 : 180,
        padding: 10,
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
  onSendChatMessage,
}: ThreadPageProps) {
  const [isTopologyCollapsed, setIsTopologyCollapsed] = useState(false);
  const [isMatrixCollapsed, setIsMatrixCollapsed] = useState(true);
  const [isChatCollapsed, setIsChatCollapsed] = useState(false);
  const [isDescriptionEditing, setIsDescriptionEditing] = useState(false);
  const [titleDraft, setTitleDraft] = useState(detail.thread.title);
  const [descriptionDraft, setDescriptionDraft] = useState(detail.thread.description ?? "");
  const [descriptionError, setDescriptionError] = useState("");
  const [isSavingDescription, setIsSavingDescription] = useState(false);
  const [matrixError, setMatrixError] = useState("");
  const [activeMatrixMutation, setActiveMatrixMutation] = useState("");
  const [chatInput, setChatInput] = useState("");
  const [chatError, setChatError] = useState("");
  const [isSendingChat, setIsSendingChat] = useState(false);
  const [topologyError, setTopologyError] = useState("");
  const [isSavingTopologyLayout, setIsSavingTopologyLayout] = useState(false);
  const [docPicker, setDocPicker] = useState<{
    nodeId: string;
    concern: string;
    search: string;
    kindFilter: "All" | DocKind;
  } | null>(null);

  const topologyPanelRef = useRef<HTMLDivElement>(null);
  const matrixPanelRef = useRef<HTMLDivElement>(null);
  const [isTopologyFullscreen, setIsTopologyFullscreen] = useState(false);
  const [isMatrixFullscreen, setIsMatrixFullscreen] = useState(false);

  useEffect(() => {
    if (!isDescriptionEditing) {
      setTitleDraft(detail.thread.title);
      setDescriptionDraft(detail.thread.description ?? "");
    }
  }, [detail.thread.title, detail.thread.description, isDescriptionEditing]);

  useEffect(() => {
    const onFullscreenChange = () => {
      setIsTopologyFullscreen(document.fullscreenElement === topologyPanelRef.current);
      setIsMatrixFullscreen(document.fullscreenElement === matrixPanelRef.current);
    };

    document.addEventListener("fullscreenchange", onFullscreenChange);
    return () => document.removeEventListener("fullscreenchange", onFullscreenChange);
  }, []);

  const flowLayoutModel = useMemo(() => buildFlowLayoutModel(detail.topology.nodes), [detail.topology.nodes]);
  const initialFlowNodes = useMemo(() => buildFlowNodes(flowLayoutModel), [flowLayoutModel]);
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

  const activeCell = useMemo(() => {
    if (!docPicker) return null;
    return cellsByKey.get(buildMatrixCellKey(docPicker.nodeId, docPicker.concern)) ?? null;
  }, [docPicker, cellsByKey]);

  const availableDocs = useMemo(() => {
    if (!docPicker) return [];

    const existingRefs = new Set(
      (activeCell?.docs ?? []).map((doc) => `${doc.hash}:${doc.refType}`),
    );
    const query = docPicker.search.trim().toLowerCase();

    return detail.matrix.documents.filter((doc) => {
      if (existingRefs.has(`${doc.hash}:${doc.kind}`)) return false;
      if (docPicker.kindFilter !== "All" && doc.kind !== docPicker.kindFilter) return false;
      if (!query) return true;
      return (
        doc.title.toLowerCase().includes(query) ||
        doc.hash.toLowerCase().includes(query) ||
        doc.language.toLowerCase().includes(query)
      );
    });
  }, [detail.matrix.documents, docPicker, activeCell]);

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

  async function handleSaveDescription() {
    if (!onUpdateThread) return;
    const normalizedTitle = titleDraft.trim();
    if (!normalizedTitle) {
      setDescriptionError("Title cannot be blank.");
      return;
    }

    setDescriptionError("");
    setIsSavingDescription(true);

    try {
      const result = await onUpdateThread({
        title: normalizedTitle,
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

  async function handleAddDoc(doc: MatrixDocument) {
    if (!docPicker || !onAddMatrixDoc) return;
    const mutationKey = `add:${docPicker.nodeId}:${docPicker.concern}:${doc.hash}:${doc.kind}`;
    setActiveMatrixMutation(mutationKey);
    setMatrixError("");

    const result = await onAddMatrixDoc({
      nodeId: docPicker.nodeId,
      concern: docPicker.concern,
      docHash: doc.hash,
      refType: doc.kind,
    });

    setActiveMatrixMutation("");
    const error = getErrorMessage(result);
    if (error) {
      setMatrixError(error);
      return;
    }

    setDocPicker(null);
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
      <div className="thread-view-header">
        <Link to={`/${detail.thread.ownerHandle}/${detail.thread.projectName}`} className="page-back">
          &larr;
        </Link>
        <h2 className="page-title">
          <span className="page-title-muted">
            {detail.thread.ownerHandle} / {detail.thread.projectName} /
          </span>{" "}
          #{detail.thread.projectThreadId}
        </h2>
        <span className={`thread-status thread-status--${detail.thread.status}`}>
          {getStatusLabel(detail.thread.status)}
        </span>
      </div>

      <h1 className="thread-view-title">{detail.thread.title}</h1>

      <section className="thread-card">
        <div className="thread-card-header">
          <h3>Description</h3>
          {detail.permissions.canEdit && !isDescriptionEditing && (
            <button
              className="btn-icon thread-card-action"
              type="button"
              onClick={() => {
                setIsDescriptionEditing(true);
                setDescriptionError("");
              }}
              aria-label="Edit title and description"
            >
              <Pencil size={16} />
            </button>
          )}
        </div>

        {isDescriptionEditing ? (
          <div className="thread-description-editor">
            <label className="field">
              <span className="field-label">Title</span>
              <input
                className="field-input"
                value={titleDraft}
                onChange={(event) => setTitleDraft(event.target.value)}
                maxLength={200}
              />
            </label>
            <label className="field">
              <span className="field-label">Description</span>
              <textarea
                className="field-input field-textarea"
                rows={4}
                value={descriptionDraft}
                onChange={(event) => setDescriptionDraft(event.target.value)}
                placeholder="Add a thread description"
              />
            </label>
            {descriptionError && <p className="field-error">{descriptionError}</p>}
            <div className="thread-inline-actions">
              <button
                className="btn btn-secondary"
                type="button"
                onClick={() => {
                  setIsDescriptionEditing(false);
                  setTitleDraft(detail.thread.title);
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
        ) : (
          <p className="thread-description-text">
            {detail.thread.description ?? "No description provided yet."}
          </p>
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
            <div className="matrix-table-wrap">
              <table className="matrix-table">
                <thead>
                  <tr>
                    <th className="matrix-node-header">Node</th>
                    {detail.matrix.concerns.map((concern) => (
                      <th key={concern.name}>{concern.name}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {detail.matrix.nodes.map((node) => (
                    <tr key={node.id}>
                      <th className="matrix-node-cell">
                        <strong>{node.name}</strong>
                        <span>{node.kind}</span>
                      </th>
                      {detail.matrix.concerns.map((concern) => {
                        const key = buildMatrixCellKey(node.id, concern.name);
                        const cell = cellsByKey.get(key) ?? {
                          nodeId: node.id,
                          concern: concern.name,
                          docs: [],
                          artifacts: [],
                        };

                        return (
                          <td key={key}>
                            <div className="matrix-cell">
                              {cell.docs.length === 0 && cell.artifacts.length === 0 && (
                                <p className="matrix-empty">No docs or artifacts</p>
                              )}

                              {DOC_TYPES.map((type) => {
                                const docs = cell.docs.filter((doc) => doc.refType === type);
                                if (docs.length === 0) return null;
                                return (
                                  <div key={type} className="matrix-doc-group">
                                    <span className="matrix-doc-group-label">{type}</span>
                                    <div className="matrix-doc-list">
                                      {docs.map((doc) => {
                                        const removeKey = `remove:${node.id}:${concern.name}:${doc.hash}:${doc.refType}`;
                                        const isMutating = activeMatrixMutation === removeKey;
                                        return (
                                          <div key={`${doc.hash}:${doc.refType}`} className={`matrix-doc-chip matrix-doc-chip--${doc.refType.toLowerCase()}`}>
                                            <span>{doc.title}</span>
                                            {detail.permissions.canEdit && (
                                              <button
                                                className="matrix-doc-remove"
                                                type="button"
                                                onClick={() => handleRemoveDoc(node.id, concern.name, doc)}
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
                                  onClick={() =>
                                    setDocPicker({
                                      nodeId: node.id,
                                      concern: concern.name,
                                      search: "",
                                      kindFilter: "All",
                                    })
                                  }
                                >
                                  <Plus size={13} /> Add doc
                                </button>
                              )}
                            </div>
                          </td>
                        );
                      })}
                    </tr>
                  ))}
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

      {docPicker && (
        <div className="modal-overlay" onClick={() => setDocPicker(null)}>
          <div className="modal thread-doc-picker" onClick={(event) => event.stopPropagation()}>
            <div className="thread-doc-picker-header">
              <h3 className="modal-title">Add Document</h3>
              <button
                className="btn-icon thread-card-action"
                type="button"
                onClick={() => setDocPicker(null)}
                aria-label="Close add document dialog"
              >
                <X size={16} />
              </button>
            </div>
            <p className="thread-doc-picker-context">
              Node: <strong>{docPicker.nodeId}</strong> · Concern: <strong>{docPicker.concern}</strong>
            </p>
            <div className="thread-doc-picker-filters">
              <input
                className="field-input"
                value={docPicker.search}
                onChange={(event) => setDocPicker((current) => (current ? { ...current, search: event.target.value } : current))}
                placeholder="Search title, hash, language"
              />
              <select
                className="field-input"
                value={docPicker.kindFilter}
                onChange={(event) => {
                  const nextValue = event.target.value as "All" | DocKind;
                  setDocPicker((current) => (current ? { ...current, kindFilter: nextValue } : current));
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
            <div className="thread-doc-picker-list">
              {availableDocs.length === 0 ? (
                <p className="matrix-empty">No documents available for this cell.</p>
              ) : (
                availableDocs.map((doc) => {
                  const addKey = `add:${docPicker.nodeId}:${docPicker.concern}:${doc.hash}:${doc.kind}`;
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
                        onClick={() => handleAddDoc(doc)}
                        disabled={isMutating}
                      >
                        {isMutating ? "Adding…" : "Add"}
                      </button>
                    </div>
                  );
                })
              )}
            </div>
          </div>
        </div>
      )}
    </main>
  );
}
