import { useEffect, useState, useCallback } from "react";
import { useAuth0 } from "@auth0/auth0-react";
import { BrowserRouter, Routes, Route, useParams, useNavigate } from "react-router-dom";
import {
  AuthContext,
  Header,
  Home,
  ProjectPage,
  ThreadPage,
  setNavigate,
  type AuthUser,
  type Project,
  type Thread,
} from "@staffx/ui";

const API_URL = import.meta.env.VITE_API_URL ?? "http://localhost:3001";

function useApi() {
  const { getAccessTokenSilently } = useAuth0();

  const apiFetch = useCallback(
    async (path: string, init?: RequestInit) => {
      const token = await getAccessTokenSilently();
      return fetch(`${API_URL}${path}`, {
        ...init,
        headers: { Authorization: `Bearer ${token}`, ...init?.headers },
      });
    },
    [getAccessTokenSilently],
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

function HomePage({
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
        const res = await apiFetch(`/projects/check-name?name=${encodeURIComponent(name)}`);
        if (!res.ok) return true;
        const data = await res.json();
        return data.available;
      }}
      onCreateProject={async (data) => {
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
      }}
    />
  );
}

function ProjectRoute() {
  const { handle, project: projectName } = useParams<{ handle: string; project: string }>();
  const { isAuthenticated } = useAuth0();
  const apiFetch = useApi();
  const [project, setProject] = useState<Project | null>(null);
  const [notFound, setNotFound] = useState(false);

  useEffect(() => {
    if (!isAuthenticated || !handle || !projectName) return;

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

function ThreadRoute() {
  const { handle, project: projectName, threadId } = useParams<{
    handle: string;
    project: string;
    threadId: string;
  }>();
  const { isAuthenticated } = useAuth0();
  const apiFetch = useApi();
  const [thread, setThread] = useState<Thread | null>(null);
  const [notFound, setNotFound] = useState(false);

  useEffect(() => {
    if (!isAuthenticated || !handle || !projectName || !threadId) return;

    apiFetch(`/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}`)
      .then(async (res) => {
        if (!res.ok) {
          setNotFound(true);
          return;
        }
        const project: Project = await res.json();
        const found = project.threads.find((t) => t.projectThreadId === Number(threadId));
        if (!found) {
          setNotFound(true);
          return;
        }
        setThread(found);
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

  if (!thread) {
    return (
      <main className="main">
        <p className="status-text">Loading…</p>
      </main>
    );
  }

  return <ThreadPage thread={thread} ownerHandle={handle!} projectName={projectName!} />;
}

export function App() {
  const { isAuthenticated, isLoading, loginWithRedirect, logout, getAccessTokenSilently } = useAuth0();
  const [user, setUser] = useState<AuthUser | null>(null);
  const [projects, setProjects] = useState<Project[]>([]);

  useEffect(() => {
    if (!isAuthenticated) return;

    getAccessTokenSilently().then(async (token) => {
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
  }, [isAuthenticated, getAccessTokenSilently]);

  return (
    <AuthContext.Provider
      value={{
        isAuthenticated,
        isLoading,
        user,
        login: () => loginWithRedirect(),
        logout: () => logout({ logoutParams: { returnTo: window.location.origin } }),
      }}
    >
      <BrowserRouter>
        <NavigateSync />
        <Header />
        <Routes>
          <Route path="/" element={<HomePage projects={projects} setProjects={setProjects} />} />
          <Route path="/:handle/:project" element={<ProjectRoute />} />
          <Route path="/:handle/:project/thread/:threadId" element={<ThreadRoute />} />
        </Routes>
      </BrowserRouter>
    </AuthContext.Provider>
  );
}
