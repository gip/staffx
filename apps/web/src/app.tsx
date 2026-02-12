import { useEffect } from "react";
import { useAuth0 } from "@auth0/auth0-react";
import { AuthContext, Header, Home } from "@staffx/ui";

const API_URL = import.meta.env.VITE_API_URL ?? "http://localhost:3001";

export function App() {
  const { isAuthenticated, isLoading, loginWithRedirect, logout, getAccessTokenSilently } = useAuth0();

  useEffect(() => {
    if (!isAuthenticated) return;

    getAccessTokenSilently().then((token) =>
      fetch(`${API_URL}/me`, {
        headers: { Authorization: `Bearer ${token}` },
      })
        .then((res) => res.json())
        .then((user) => console.log("API /me:", user))
        .catch((err) => console.error("API /me failed:", err)),
    );
  }, [isAuthenticated, getAccessTokenSilently]);

  return (
    <AuthContext.Provider
      value={{
        isAuthenticated,
        isLoading,
        login: () => loginWithRedirect(),
        logout: () => logout({ logoutParams: { returnTo: window.location.origin } }),
      }}
    >
      <Header />
      <Home />
    </AuthContext.Provider>
  );
}
