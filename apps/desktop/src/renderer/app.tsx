import { useCallback, useEffect, useState } from "react";
import { Route, Routes, useNavigate, useParams } from "react-router-dom";
import {
  AuthContext,
  Header,
  Home,
  ProjectPage,
  ThreadPage,
  setNavigate,
  type AuthUser,
  type ChatMessage,
  type MatrixCell,
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

declare global {
  interface Window {
    electronAPI: {
      platform: string;
      auth: ElectronAuthAPI;
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

function mergeChatMessages(current: ChatMessage[], incoming: ChatMessage[]) {
  const existing = new Set(current.map((message) => message.id));
  const next = incoming.filter((message) => !existing.has(message.id));
  return [...current, ...next];
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
                matrix: { ...prev.matrix, cells: upsertMatrixCell(prev.matrix.cells, data.cell) },
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
        <Route
          path="/:handle/:project/thread/:threadId"
          element={<ThreadRoute isAuthenticated={isAuthenticated} />}
        />
      </Routes>
    </AuthContext.Provider>
  );
}
