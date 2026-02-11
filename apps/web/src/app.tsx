import { useAuth0 } from "@auth0/auth0-react";
import { AuthContext, Header, Home } from "@staffx/ui";

export function App() {
  const { isAuthenticated, isLoading, loginWithRedirect, logout } = useAuth0();

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
