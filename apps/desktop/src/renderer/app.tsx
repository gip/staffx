import { useEffect, useState } from "react";
import { AuthContext, Header, Home, type AuthUser, type Project } from "@staffx/ui";

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
      <Header />
      <Home
        projects={projects}
        onCreateProject={async (data) => {
          const token = await window.electronAPI.auth.getAccessToken();
          if (!token) return;
          const res = await fetch(`${API_URL}/projects`, {
            method: "POST",
            headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
            body: JSON.stringify(data),
          });
          if (res.ok) {
            const project = await res.json();
            setProjects((prev) => [project, ...prev]);
          }
        }}
      />
    </AuthContext.Provider>
  );
}
