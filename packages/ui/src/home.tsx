import { useAuth } from "./auth-context";

export function Home() {
  const { isAuthenticated, isLoading } = useAuth();

  if (isLoading) {
    return (
      <main className="main">
        <p className="status-text">Loading…</p>
      </main>
    );
  }

  return (
    <main className="main">
      <p className="status-text">{isAuthenticated ? "Logged In" : "Logged Out"}</p>
    </main>
  );
}
