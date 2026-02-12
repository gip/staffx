import { useEffect, useState } from "react";
import { useAuth0 } from "@auth0/auth0-react";
import { AuthContext, Header, Home, type AuthUser, type Project } from "@staffx/ui";

const API_URL = import.meta.env.VITE_API_URL ?? "http://localhost:3001";

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
      <Header />
      <Home
        projects={projects}
        onCheckProjectName={async (name) => {
          const token = await getAccessTokenSilently();
          const res = await fetch(`${API_URL}/projects/check-name?name=${encodeURIComponent(name)}`, {
            headers: { Authorization: `Bearer ${token}` },
          });
          if (!res.ok) return true;
          const data = await res.json();
          return data.available;
        }}
        onCreateProject={async (data) => {
          const token = await getAccessTokenSilently();
          const res = await fetch(`${API_URL}/projects`, {
            method: "POST",
            headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
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
    </AuthContext.Provider>
  );
}
