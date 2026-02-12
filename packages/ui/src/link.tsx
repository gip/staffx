import type { ComponentProps } from "react";

/**
 * A thin <a> wrapper that calls `onNavigate` (set by the host app)
 * instead of doing a full page reload, enabling client-side routing
 * without coupling the UI package to react-router-dom.
 */
let navigateFn: ((to: string) => void) | null = null;

export function setNavigate(fn: (to: string) => void) {
  navigateFn = fn;
}

interface LinkProps extends Omit<ComponentProps<"a">, "href"> {
  to: string;
}

export function Link({ to, children, onClick, ...rest }: LinkProps) {
  return (
    <a
      href={to}
      onClick={(e) => {
        if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey || e.button !== 0) return;
        e.preventDefault();
        onClick?.(e);
        navigateFn?.(to);
      }}
      {...rest}
    >
      {children}
    </a>
  );
}
