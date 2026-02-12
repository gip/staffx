import { useState, useRef, useEffect } from "react";
import { useAuth } from "./auth-context";
import { useTheme } from "./theme";
import { Link } from "./link";

export function Header() {
  const { isAuthenticated, isLoading, user, login, logout } = useAuth();
  const { theme, toggle } = useTheme();
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!menuOpen) return;
    function handleClick(e: MouseEvent) {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setMenuOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [menuOpen]);

  return (
    <header className="header">
      <div className="header-left">
        <Link to="/" className="header-logo">StaffX</Link>
      </div>
      <div className="header-right">
        <button className="btn-icon" onClick={toggle} aria-label="Toggle theme">
          {theme === "light" ? "☾" : "☀"}
        </button>
        {isLoading ? null : !isAuthenticated ? (
          <button className="btn" onClick={login}>Log In</button>
        ) : (
          <div className="avatar-menu" ref={menuRef}>
            <button className="avatar-btn" onClick={() => setMenuOpen((v) => !v)}>
              {user?.picture ? (
                <img className="avatar-img" src={user.picture} alt="" />
              ) : (
                <span className="avatar-fallback">
                  {(user?.handle ?? "?")[0].toUpperCase()}
                </span>
              )}
            </button>
            {menuOpen && (
              <div className="dropdown">
                <div className="dropdown-user">
                  <span className="dropdown-handle">{user?.handle}</span>
                  {user?.email && <span className="dropdown-email">{user.email}</span>}
                </div>
                <div className="dropdown-divider" />
                <Link to="/" className="dropdown-item" onClick={() => setMenuOpen(false)}>
                  Home Page
                </Link>
                <button className="dropdown-item" onClick={() => { setMenuOpen(false); logout(); }}>
                  Log Out
                </button>
              </div>
            )}
          </div>
        )}
      </div>
    </header>
  );
}
