import { useCallback, useEffect, useState } from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import {
  AuthContext,
  Header,
  Home,
  ProjectPage,
  ProjectSettingsPage,
  ThreadPage,
  UserProfilePage,
  setNavigate,
  type AuthUser,
  type Collaborator,
  type Concern,
  type UserProfile,
  type ChatMessage,
  type MatrixDocument,
  type MatrixCell,
  type MatrixCellDoc,
  type Project,
  type ThreadDetail,
  type ThreadDetailPayload,
} from "@staffx/ui";

const API_URL = import.meta.env.VITE_API_URL ?? "http://localhost:3001";

interface ElectronAuthAPI {
  getState: () => Promise<{ isAuthenticated: boolean }>;
  getAccessToken: () => Promise<string | null>;
  login: () => void;
  logout: () => void;
  onStateChanged: (cb: (state: { isAuthenticated: boolean }) => void) => () => void;
}

interface ElectronAgentAPI {
  start: (params: { prompt: string; cwd?: string; allowedTools?: string[]; systemPrompt?: string; model?: string }) => Promise<{ threadId: string }>;
  stop: (threadId: string) => void;
  getStatus: (threadId: string) => Promise<{ status: string; sessionId: string | null } | null>;
  onMessage: (callback: (data: { threadId: string; message: unknown }) => void) => () => void;
  onDone: (callback: (data: { threadId: string; status: string }) => void) => () => void;
}

declare global {
  interface Window {
    electronAPI: {
      platform: string;
      auth: ElectronAuthAPI;
      agent: ElectronAgentAPI;
    };
  }
}

function upsertMatrixCell(cells: MatrixCell[], nextCell: MatrixCell): MatrixCell[] {
  const index = cells.findIndex(
    (cell) => cell.nodeId === nextCell.nodeId && cell.concern === nextCell.concern,
  );
  if (index === -1) return [...cells, nextCell];
  return cells.map((cell, idx) => (idx === index ? nextCell : cell));
}

function upsertMatrixDocument(documents: MatrixDocument[], next: MatrixDocument): MatrixDocument[] {
  const index = documents.findIndex((document) => document.hash === next.hash);
  if (index === -1) return [...documents, next];
  return documents.map((document, idx) =>
    idx === index
      ? {
          ...document,
          title: next.title,
          kind: next.kind,
          language: next.language,
          text: next.text,
        }
      : document,
  );
}

function replaceMatrixDocumentReferences(
  cells: MatrixCell[],
  oldHash: string,
  nextDoc: MatrixDocument,
): MatrixCell[] {
  return cells.map((cell) => ({
    ...cell,
    docs: cell.docs.map((doc) =>
      doc.hash === oldHash
        ? {
            ...doc,
            hash: nextDoc.hash,
            title: nextDoc.title,
            kind: nextDoc.kind,
            language: nextDoc.language,
          }
        : doc,
    ),
  }));
}

function replaceMatrixDocumentInGlobalList(
  documents: MatrixDocument[],
  oldHash: string,
  nextDoc: MatrixDocument,
): MatrixDocument[] {
  const withoutOld = documents.filter((document) => document.hash !== oldHash);
  if (withoutOld.some((document) => document.hash === nextDoc.hash)) return withoutOld;
  return [...withoutOld, nextDoc];
}

function mergeChatMessages(current: ChatMessage[], incoming: ChatMessage[]) {
  const existing = new Set(current.map((message) => message.id));
  const next = incoming.filter((message) => !existing.has(message.id));
  return [...current, ...next];
}

function applyTopologyPositions(
  detail: ThreadDetailPayload,
  positions: Array<{ nodeId: string; x: number; y: number }>,
): ThreadDetailPayload {
  const byNodeId = new Map(positions.map((position) => [position.nodeId, position]));
  return {
    ...detail,
    topology: {
      ...detail.topology,
      nodes: detail.topology.nodes.map((node) => {
        const position = byNodeId.get(node.id);
        if (!position) return node;
        return { ...node, layoutX: position.x, layoutY: position.y };
      }),
    },
    matrix: {
      ...detail.matrix,
      nodes: detail.matrix.nodes.map((node) => {
        const position = byNodeId.get(node.id);
        if (!position) return node;
        return { ...node, layoutX: position.x, layoutY: position.y };
      }),
    },
  };
}

async function readError(res: Response, fallback: string) {
  const body = await res.json().catch(() => ({}));
  if (typeof body.error === "string" && body.error) return body.error;
  return fallback;
}

function useApi() {
  const apiFetch = useCallback(
    async (path: string, init?: RequestInit) => {
      const token = await window.electronAPI.auth.getAccessToken();
      if (!token) throw new Error("Not authenticated");

      return fetch(`${API_URL}${path}`, {
        ...init,
        headers: { Authorization: `Bearer ${token}`, ...init?.headers },
      });
    },
    [],
  );

  return apiFetch;
}

function NavigateSync() {
  const navigate = useNavigate();

  useEffect(() => {
    setNavigate((to) => navigate(to));
  }, [navigate]);

  return null;
}

function HomeRoute({
  projects,
  setProjects,
}: {
  projects: Project[];
  setProjects: React.Dispatch<React.SetStateAction<Project[]>>;
}) {
  const apiFetch = useApi();

  return (
    <Home
      projects={projects}
      onCheckProjectName={async (name) => {
        try {
          const res = await apiFetch(`/projects/check-name?name=${encodeURIComponent(name)}`);
          if (!res.ok) return true;
          const data = await res.json();
          return data.available;
        } catch {
          return true;
        }
      }}
      onCreateProject={async (data) => {
        try {
          const res = await apiFetch("/projects", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(data),
          });
          if (!res.ok) {
            const body = await res.json().catch(() => ({}));
            return { error: body.error ?? "Failed to create project" };
          }
          const project = await res.json();
          setProjects((prev) => [project, ...prev]);
        } catch (error) {
          if (error instanceof Error && error.message.trim()) {
            return { error: error.message };
          }
          return { error: "Failed to create project" };
        }
      }}
    />
  );
}

function ProfileRoute({ isAuthenticated }: { isAuthenticated: boolean }) {
  const { handle } = useParams<{ handle: string }>();
  const apiFetch = useApi();
  const [profile, setProfile] = useState<UserProfile | null>(null);
  const [notFound, setNotFound] = useState(false);

  useEffect(() => {
    if (!isAuthenticated || !handle) return;

    setNotFound(false);
    setProfile(null);

    apiFetch(`/users/${encodeURIComponent(handle)}`)
      .then(async (res) => {
        if (!res.ok) {
          setNotFound(true);
          return;
        }
        setProfile(await res.json());
      })
      .catch(() => setNotFound(true));
  }, [isAuthenticated, handle, apiFetch]);

  if (notFound) {
    return (
      <main className="main">
        <p className="status-text">User not found</p>
      </main>
    );
  }

  if (!profile) {
    return (
      <main className="main">
        <p className="status-text">Loading…</p>
      </main>
    );
  }

  return <UserProfilePage profile={profile} />;
}

function ProjectRoute({ isAuthenticated }: { isAuthenticated: boolean }) {
  const { handle, project: projectName } = useParams<{ handle: string; project: string }>();
  const apiFetch = useApi();
  const [project, setProject] = useState<Project | null>(null);
  const [notFound, setNotFound] = useState(false);

  useEffect(() => {
    if (!isAuthenticated || !handle || !projectName) return;

    setNotFound(false);
    setProject(null);

    apiFetch(`/projects/${encodeURIComponent(handle)}/${encodeURIComponent(projectName)}`)
      .then(async (res) => {
        if (!res.ok) {
          setNotFound(true);
          return;
        }
        setProject(await res.json());
      })
      .catch(() => setNotFound(true));
  }, [isAuthenticated, handle, projectName, apiFetch]);

  if (notFound) {
    return (
      <main className="main">
        <p className="status-text">Project not found</p>
      </main>
    );
  }

  if (!project) {
    return (
      <main className="main">
        <p className="status-text">Loading…</p>
      </main>
    );
  }

  return <ProjectPage project={project} />;
}

function SettingsRoute({ isAuthenticated }: { isAuthenticated: boolean }) {
  const { handle, project: projectName } = useParams<{ handle: string; project: string }>();
  const apiFetch = useApi();
  const [data, setData] = useState<{
    accessRole: string;
    collaborators: Collaborator[];
    projectRoles: string[];
    concerns: Concern[];
  } | null>(null);
  const [notFound, setNotFound] = useState(false);

  useEffect(() => {
    if (!isAuthenticated || !handle || !projectName) return;

    apiFetch(`/projects/${encodeURIComponent(handle)}/${encodeURIComponent(projectName)}/collaborators`)
      .then(async (res) => {
        if (!res.ok) {
          setNotFound(true);
          return;
        }
        setData(await res.json());
      })
      .catch(() => setNotFound(true));
  }, [isAuthenticated, handle, projectName, apiFetch]);

  if (notFound) {
    return (
      <main className="main">
        <p className="status-text">Project not found</p>
      </main>
    );
  }

  if (!data) {
    return (
      <main className="main">
        <p className="status-text">Loading…</p>
      </main>
    );
  }

  return (
    <ProjectSettingsPage
      projectOwnerHandle={handle!}
      projectName={projectName!}
      accessRole={data.accessRole}
      collaborators={data.collaborators}
      projectRoles={data.projectRoles}
      concerns={data.concerns}
      onSearchUsers={async (q) => {
        const res = await apiFetch(`/users/search?q=${encodeURIComponent(q)}`);
        if (!res.ok) return [];
        return res.json();
      }}
      onAddCollaborator={async (targetHandle, role, projectRoles) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/collaborators`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ handle: targetHandle, role, projectRoles }),
          },
        );
        if (!res.ok) {
          const body = await res.json().catch(() => ({}));
          return { error: body.error ?? "Failed to add collaborator" };
        }
      }}
      onRemoveCollaborator={async (targetHandle) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/collaborators/${encodeURIComponent(targetHandle)}`,
          { method: "DELETE" },
        );
        if (!res.ok && res.status !== 204) {
          const body = await res.json().catch(() => ({}));
          return { error: body.error ?? "Failed to remove collaborator" };
        }
      }}
      onAddRole={async (name) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/roles`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name }),
          },
        );
        if (!res.ok) {
          const body = await res.json().catch(() => ({}));
          return { error: body.error ?? "Failed to add role" };
        }
      }}
      onAddConcern={async (name) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/concerns`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name }),
          },
        );
        if (!res.ok) {
          const body = await res.json().catch(() => ({}));
          return { error: body.error ?? "Failed to add concern" };
        }
      }}
      onDeleteConcern={async (name) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/concerns/${encodeURIComponent(name)}`,
          { method: "DELETE" },
        );
        if (!res.ok && res.status !== 204) {
          const body = await res.json().catch(() => ({}));
          return { error: body.error ?? "Failed to delete concern" };
        }
      }}
      onDeleteRole={async (name) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/roles/${encodeURIComponent(name)}`,
          { method: "DELETE" },
        );
        if (!res.ok && res.status !== 204) {
          const body = await res.json().catch(() => ({}));
          return { error: body.error ?? "Failed to delete role" };
        }
      }}
      onUpdateMemberRoles={async (targetHandle, projectRoles) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/collaborators/${encodeURIComponent(targetHandle)}/roles`,
          {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ projectRoles }),
          },
        );
        if (!res.ok) {
          const body = await res.json().catch(() => ({}));
          return { error: body.error ?? "Failed to update roles" };
        }
      }}
    />
  );
}

function ThreadRoute({ isAuthenticated }: { isAuthenticated: boolean }) {
  const { handle, project: projectName, threadId } = useParams<{
    handle: string;
    project: string;
    threadId: string;
  }>();
  const apiFetch = useApi();
  const [detail, setDetail] = useState<ThreadDetailPayload | null>(null);
  const [notFound, setNotFound] = useState(false);

  useEffect(() => {
    if (!isAuthenticated || !handle || !projectName || !threadId) return;

    setNotFound(false);
    setDetail(null);

    apiFetch(
      `/projects/${encodeURIComponent(handle)}/${encodeURIComponent(projectName)}/thread/${encodeURIComponent(threadId)}`,
    )
      .then(async (res) => {
        if (res.status === 404) {
          setNotFound(true);
          return;
        }
        if (!res.ok) {
          throw new Error(await readError(res, "Failed to load thread"));
        }
        setDetail(await res.json());
      })
      .catch(() => setNotFound(true));
  }, [isAuthenticated, handle, projectName, threadId, apiFetch]);

  if (notFound) {
    return (
      <main className="main">
        <p className="status-text">Thread not found</p>
      </main>
    );
  }

  if (!detail) {
    return (
      <main className="main">
        <p className="status-text">Loading…</p>
      </main>
    );
  }

  return (
    <ThreadPage
      detail={detail}
      onUpdateThread={async (payload) => {
        try {
          const res = await apiFetch(
            `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${encodeURIComponent(threadId!)}`,
            {
              method: "PATCH",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            },
          );
          if (!res.ok) {
            return { error: await readError(res, "Failed to update thread") };
          }
          const data = (await res.json()) as { thread: ThreadDetail };
          setDetail((prev) => (prev ? { ...prev, thread: data.thread } : prev));
          return data;
        } catch (error: unknown) {
          if (error instanceof Error && error.message.trim()) {
            return { error: error.message };
          }
          return { error: "Failed to update thread" };
        }
      }}
      onSaveTopologyLayout={async (payload) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${encodeURIComponent(threadId!)}/topology/layout`,
          {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          },
        );
        if (!res.ok) {
          return { error: await readError(res, "Failed to save topology layout") };
        }
        const data = (await res.json()) as { systemId: string };
        setDetail((prev) => (
          prev
            ? {
                ...applyTopologyPositions(prev, payload.positions),
                systemId: data.systemId,
              }
            : prev
        ));
        return data;
      }}
      onAddMatrixDoc={async (payload) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${encodeURIComponent(threadId!)}/matrix/refs`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          },
        );
        if (!res.ok) {
          return { error: await readError(res, "Failed to add matrix document") };
        }
        const data = (await res.json()) as { systemId: string; cell: MatrixCell };
        setDetail((prev) => (
          prev
            ? {
                ...prev,
                systemId: data.systemId,
                matrix: {
                  ...prev.matrix,
                  cells: (() => {
                    const existingCell = prev.matrix.cells.find((cell) =>
                      cell.nodeId === data.cell.nodeId && cell.concern === data.cell.concern,
                    );
                    if (existingCell) {
                      const docByRef = new Map<string, MatrixCellDoc>(
                        existingCell.docs.map((doc) => [`${doc.hash}:${doc.refType}`, doc]),
                      );
                      for (const doc of data.cell.docs) {
                        docByRef.set(`${doc.hash}:${doc.refType}`, doc);
                      }
                      const mergedCell: MatrixCell = {
                        ...existingCell,
                        docs: Array.from(docByRef.values()),
                        artifacts: data.cell.artifacts,
                      };
                      return upsertMatrixCell(prev.matrix.cells, mergedCell);
                    }
                    return upsertMatrixCell(prev.matrix.cells, data.cell);
                  })(),
                },
              }
            : prev
        ));
        return data;
      }}
      onRemoveMatrixDoc={async (payload) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${encodeURIComponent(threadId!)}/matrix/refs`,
          {
            method: "DELETE",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          },
        );
        if (!res.ok) {
          return { error: await readError(res, "Failed to remove matrix document") };
        }
        const data = (await res.json()) as { systemId: string; cell: MatrixCell };
        setDetail((prev) => (
          prev
            ? {
                ...prev,
                systemId: data.systemId,
                matrix: { ...prev.matrix, cells: upsertMatrixCell(prev.matrix.cells, data.cell) },
              }
            : prev
        ));
        return data;
      }}
      onCreateMatrixDocument={async (payload) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${encodeURIComponent(threadId!)}/matrix/documents`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          },
        );
        if (!res.ok) {
          return { error: await readError(res, "Failed to create matrix document") };
        }
        const data = (await res.json()) as {
          systemId: string;
          document: MatrixDocument;
          cell?: MatrixCell;
        };
        setDetail((prev) => {
          if (!prev) return prev;
          const fallbackCell = (() => {
            if (!payload.attach) return data.cell;
            if (data.cell) return data.cell;

            const existingCell = prev.matrix.cells.find((cell) =>
              cell.nodeId === payload.attach?.nodeId && cell.concern === payload.attach?.concern,
            );
            const nextDoc = {
              hash: data.document.hash,
              title: data.document.title,
              kind: data.document.kind,
              language: data.document.language,
              refType: payload.attach.refType,
            };
            const docs = existingCell?.docs ?? [];
            const deduped = docs.some((doc) => doc.hash === nextDoc.hash && doc.refType === nextDoc.refType)
              ? docs
              : [...docs, nextDoc];

            return {
              nodeId: payload.attach.nodeId,
              concern: payload.attach.concern,
              docs: deduped,
              artifacts: existingCell?.artifacts ?? [],
            };
          })();

          return {
            ...prev,
            systemId: data.systemId,
            matrix: {
              ...prev.matrix,
              documents: upsertMatrixDocument(prev.matrix.documents, data.document),
              cells: fallbackCell ? upsertMatrixCell(prev.matrix.cells, fallbackCell) : prev.matrix.cells,
            },
          };
        });
        return data;
      }}
      onReplaceMatrixDocument={async (documentHash, payload) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${encodeURIComponent(threadId!)}/matrix/documents/${encodeURIComponent(documentHash)}`,
          {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          },
        );
        if (!res.ok) {
          return { error: await readError(res, "Failed to replace matrix document") };
        }
        const data = (await res.json()) as {
          systemId: string;
          oldHash: string;
          document: MatrixDocument;
          replacedRefs: number;
        };
        setDetail((prev) => {
          if (!prev) return prev;
          return {
            ...prev,
            systemId: data.systemId,
            matrix: {
              ...prev.matrix,
              documents: replaceMatrixDocumentInGlobalList(
                prev.matrix.documents,
                data.oldHash,
                data.document,
              ),
              cells: replaceMatrixDocumentReferences(
                prev.matrix.cells,
                data.oldHash,
                data.document,
              ),
            },
          };
        });
        return data;
      }}
      onSendChatMessage={async (payload) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${encodeURIComponent(threadId!)}/chat/messages`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          },
        );
        if (!res.ok) {
          return { error: await readError(res, "Failed to send chat message") };
        }
        const data = (await res.json()) as { messages: ChatMessage[] };
        setDetail((prev) => (
          prev
            ? {
                ...prev,
                chat: {
                  ...prev.chat,
                  messages: mergeChatMessages(prev.chat.messages, data.messages),
                },
              }
            : prev
        ));
        return data;
      }}
    />
  );
}

export function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [user, setUser] = useState<AuthUser | null>(null);
  const [projects, setProjects] = useState<Project[]>([]);

  useEffect(() => {
    const { auth } = window.electronAPI;

    auth.getState().then((state) => {
      setIsAuthenticated(state.isAuthenticated);
      setIsLoading(false);
    });

    const unsubscribe = auth.onStateChanged((state) => {
      setIsAuthenticated(state.isAuthenticated);
      setIsLoading(false);
    });

    return unsubscribe;
  }, []);

  useEffect(() => {
    if (!isAuthenticated) {
      setUser(null);
      setProjects([]);
      return;
    }

    const { auth } = window.electronAPI;

    auth.getAccessToken().then(async (token) => {
      if (!token) return;

      const headers = { Authorization: `Bearer ${token}` };

      try {
        const [meRes, projRes] = await Promise.all([
          fetch(`${API_URL}/me`, { headers }),
          fetch(`${API_URL}/projects`, { headers }),
        ]);

        if (meRes.ok) {
          const me = await meRes.json();
          setUser({ handle: me.handle, email: me.email ?? null, githubHandle: me.githubHandle ?? "", name: me.name, picture: me.picture });
        }

        if (projRes.ok) {
          setProjects(await projRes.json());
        }
      } catch (err) {
        console.error("API fetch failed:", err);
      }
    });
  }, [isAuthenticated]);

  return (
    <AuthContext.Provider
      value={{
        isAuthenticated,
        isLoading,
        user,
        login: () => window.electronAPI.auth.login(),
        logout: () => window.electronAPI.auth.logout(),
      }}
    >
      <NavigateSync />
      <Header />
      <Routes>
        <Route path="/" element={<HomeRoute projects={projects} setProjects={setProjects} />} />
        <Route path="/:handle/:project" element={<ProjectRoute isAuthenticated={isAuthenticated} />} />
        <Route path="/:handle/:project/settings" element={<SettingsRoute isAuthenticated={isAuthenticated} />} />
        <Route path="/:handle" element={<ProfileRoute isAuthenticated={isAuthenticated} />} />
        <Route
          path="/:handle/:project/thread/:threadId"
          element={<ThreadRoute isAuthenticated={isAuthenticated} />}
        />
      </Routes>
    </AuthContext.Provider>
  );
}
