import { useAuth } from "./auth-context";
import { useTheme } from "./theme";

export function Header() {
  const { isAuthenticated, isLoading, login, logout } = useAuth();
  const { theme, toggle } = useTheme();

  return (
    <header className="header">
      <div className="header-left">
        <span className="header-logo">StaffX</span>
      </div>
      <div className="header-right">
        <button className="btn-icon" onClick={toggle} aria-label="Toggle theme">
          {theme === "light" ? "☾" : "☀"}
        </button>
        {isLoading ? null : (
          <button className="btn" onClick={isAuthenticated ? logout : login}>
            {isAuthenticated ? "Log Out" : "Log In"}
          </button>
        )}
      </div>
    </header>
  );
}
