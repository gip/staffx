import { useEffect, useMemo, useRef, useState } from "react";
import {
  ChevronDown,
  ChevronRight,
  Expand,
  Minimize2,
  Pencil,
  Plus,
  Save,
  Send,
  X,
} from "lucide-react";
import ReactFlow, {
  Background,
  Controls,
  MarkerType,
  Position,
  type Edge,
  type Node,
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
}

export interface TopologyEdge {
  id: string;
  type: string;
  fromNodeId: string;
  toNodeId: string;
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

function buildFlowNodes(nodes: TopologyNode[]): Node[] {
  if (nodes.length === 0) return [];

  const byId = new Map(nodes.map((node) => [node.id, node]));
  const children = new Map<string, TopologyNode[]>();

  for (const node of nodes) {
    if (!node.parentId || !byId.has(node.parentId)) continue;
    const list = children.get(node.parentId) ?? [];
    list.push(node);
    children.set(node.parentId, list);
  }

  const depths = new Map<string, number>();
  const roots = nodes.filter((node) => !node.parentId || !byId.has(node.parentId)).sort(sortNodes);
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

  for (const node of nodes) {
    if (!depths.has(node.id)) {
      depths.set(node.id, 0);
    }
  }

  const columns = new Map<number, TopologyNode[]>();
  for (const node of nodes) {
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

  return nodes.map((node) => ({
    id: node.id,
    position: positions.get(node.id) ?? { x: 0, y: 0 },
    sourcePosition: Position.Right,
    targetPosition: Position.Left,
    data: {
      label: (
        <div className="thread-topology-node">
          <strong>{node.name}</strong>
          <span>{node.kind}</span>
        </div>
      ),
    },
    style: {
      borderRadius: 10,
      border: "1px solid var(--border)",
      background: "var(--bg-secondary)",
      color: "var(--fg)",
      minWidth: 170,
      padding: 10,
      boxShadow: "none",
    },
  }));
}

function buildFlowEdges(edges: TopologyEdge[]): Edge[] {
  return edges.map((edge) => ({
    id: edge.id,
    source: edge.fromNodeId,
    target: edge.toNodeId,
    markerEnd: { type: MarkerType.ArrowClosed, width: 16, height: 16 },
    style: { stroke: "var(--fg-muted)", strokeWidth: 1.2 },
    label: edge.type,
    labelStyle: { fill: "var(--fg-muted)", fontSize: 11 },
  }));
}

export function ThreadPage({
  detail,
  onUpdateThread,
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

  const flowNodes = useMemo(() => buildFlowNodes(detail.topology.nodes), [detail.topology.nodes]);
  const flowEdges = useMemo(() => buildFlowEdges(detail.topology.edges), [detail.topology.edges]);

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

    const result = await onUpdateThread({
      title: normalizedTitle,
      description: descriptionDraft.trim() ? descriptionDraft.trim() : null,
    });

    setIsSavingDescription(false);
    const error = getErrorMessage(result);
    if (error) {
      setDescriptionError(error);
      return;
    }

    setIsDescriptionEditing(false);
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
                <X size={14} /> Cancel
              </button>
              <button
                className="btn"
                type="button"
                onClick={handleSaveDescription}
                disabled={isSavingDescription}
              >
                <Save size={14} /> {isSavingDescription ? "Saving…" : "Save"}
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
        <div className="thread-card-header">
          <h3>Topology View</h3>
          <div className="thread-card-actions">
            <button
              className="btn-icon thread-card-action"
              type="button"
              onClick={() => toggleFullscreen(topologyPanelRef)}
              aria-label={isTopologyFullscreen ? "Exit fullscreen topology" : "Enter fullscreen topology"}
            >
              {isTopologyFullscreen ? <Minimize2 size={16} /> : <Expand size={16} />}
            </button>
            <button
              className="btn-icon thread-card-action"
              type="button"
              onClick={() => setIsTopologyCollapsed((current) => !current)}
              aria-label={isTopologyCollapsed ? "Expand topology" : "Collapse topology"}
            >
              {isTopologyCollapsed ? <ChevronRight size={16} /> : <ChevronDown size={16} />}
            </button>
          </div>
        </div>

        {!isTopologyCollapsed && (
          <div className="thread-card-body">
            <div className="thread-topology-canvas">
              <ReactFlow
                nodes={flowNodes}
                edges={flowEdges}
                fitView
                nodesDraggable={false}
                nodesConnectable={false}
                elementsSelectable={false}
                deleteKeyCode={null}
              >
                <Background gap={14} size={1} />
                <Controls />
              </ReactFlow>
            </div>
          </div>
        )}
      </section>

      <section
        ref={matrixPanelRef}
        className={`thread-card thread-collapsible ${isMatrixFullscreen ? "thread-card--fullscreen" : ""}`}
      >
        <div className="thread-card-header">
          <h3>Matrix View</h3>
          <div className="thread-card-actions">
            <button
              className="btn-icon thread-card-action"
              type="button"
              onClick={() => toggleFullscreen(matrixPanelRef)}
              aria-label={isMatrixFullscreen ? "Exit fullscreen matrix" : "Enter fullscreen matrix"}
            >
              {isMatrixFullscreen ? <Minimize2 size={16} /> : <Expand size={16} />}
            </button>
            <button
              className="btn-icon thread-card-action"
              type="button"
              onClick={() => setIsMatrixCollapsed((current) => !current)}
              aria-label={isMatrixCollapsed ? "Expand matrix" : "Collapse matrix"}
            >
              {isMatrixCollapsed ? <ChevronRight size={16} /> : <ChevronDown size={16} />}
            </button>
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
        <div className="thread-card-header">
          <h3>Chat View</h3>
          <div className="thread-card-actions">
            <button
              className="btn-icon thread-card-action"
              type="button"
              onClick={() => setIsChatCollapsed((current) => !current)}
              aria-label={isChatCollapsed ? "Expand chat" : "Collapse chat"}
            >
              {isChatCollapsed ? <ChevronRight size={16} /> : <ChevronDown size={16} />}
            </button>
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
