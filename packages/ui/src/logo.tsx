export function Logo({ size = 24 }: { size?: number }) {
  return (
    <svg viewBox="0 0 512 512" width={size} height={size} aria-hidden="true">
      <g transform="rotate(55, 256, 256)">
        <ellipse cx="256" cy="256" rx="160" ry="260" fill="none" stroke="currentColor" strokeWidth="28" />
        <circle cx="256" cy="256" r="80" fill="#E53935" />
      </g>
    </svg>
  );
}
