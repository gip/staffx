import { useEffect, useRef } from "react";

interface Node {
  x: number;
  y: number;
  vx: number;
  vy: number;
  radius: number;
}

export function NodeGraph() {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const container = canvas.parentElement!;
    let w = 0;
    let h = 0;
    let dpr = window.devicePixelRatio || 1;
    const NODE_COUNT = 40;
    const EDGE_DISTANCE = 150;
    const REPEL_RADIUS = 120;
    const REPEL_STRENGTH = 0.6;

    let nodeColor = "";
    let edgeColor = "";

    function readColors() {
      const style = getComputedStyle(document.documentElement);
      nodeColor = style.getPropertyValue("--fg-muted").trim() || "#666";
      edgeColor = style.getPropertyValue("--border").trim() || "#a0a0a0";
    }

    readColors();

    // Watch for theme changes
    const themeObserver = new MutationObserver(() => readColors());
    themeObserver.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["data-theme"],
    });

    const nodes: Node[] = [];

    function resize() {
      dpr = window.devicePixelRatio || 1;
      w = container.clientWidth;
      h = container.clientHeight;
      canvas!.width = w * dpr;
      canvas!.height = h * dpr;
      canvas!.style.width = w + "px";
      canvas!.style.height = h + "px";
      ctx!.setTransform(dpr, 0, 0, dpr, 0, 0);
    }

    function initNodes() {
      nodes.length = 0;
      for (let i = 0; i < NODE_COUNT; i++) {
        nodes.push({
          x: Math.random() * w,
          y: Math.random() * h,
          vx: (Math.random() - 0.5) * 0.4,
          vy: (Math.random() - 0.5) * 0.4,
          radius: 2 + Math.random(),
        });
      }
    }

    resize();
    initNodes();

    const resizeObserver = new ResizeObserver(() => {
      resize();
    });
    resizeObserver.observe(container);

    // Mouse tracking
    let mouseX = -1000;
    let mouseY = -1000;

    function handleMouseMove(e: MouseEvent) {
      const rect = canvas!.getBoundingClientRect();
      mouseX = e.clientX - rect.left;
      mouseY = e.clientY - rect.top;
    }

    function handleMouseLeave() {
      mouseX = -1000;
      mouseY = -1000;
    }

    // Listen on parent so pointer-events: none on canvas doesn't block
    container.addEventListener("mousemove", handleMouseMove);
    container.addEventListener("mouseleave", handleMouseLeave);

    let frameId = 0;
    let startTime = performance.now();

    function animate(now: number) {
      frameId = requestAnimationFrame(animate);
      const elapsed = (now - startTime) / 1000;

      ctx!.clearRect(0, 0, w, h);

      // Update nodes
      for (const node of nodes) {
        // Mouse repulsion
        const dx = node.x - mouseX;
        const dy = node.y - mouseY;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < REPEL_RADIUS && dist > 0) {
          const force = (1 - dist / REPEL_RADIUS) * REPEL_STRENGTH;
          node.vx += (dx / dist) * force;
          node.vy += (dy / dist) * force;
        }

        // Dampen velocity
        node.vx *= 0.99;
        node.vy *= 0.99;

        node.x += node.vx;
        node.y += node.vy;

        // Bounce off edges
        if (node.x < 0) { node.x = 0; node.vx = Math.abs(node.vx); }
        if (node.x > w) { node.x = w; node.vx = -Math.abs(node.vx); }
        if (node.y < 0) { node.y = 0; node.vy = Math.abs(node.vy); }
        if (node.y > h) { node.y = h; node.vy = -Math.abs(node.vy); }
      }

      // Draw edges
      const breathe = 0.5 + 0.5 * Math.sin(elapsed * 0.8);
      for (let i = 0; i < nodes.length; i++) {
        for (let j = i + 1; j < nodes.length; j++) {
          const dx = nodes[i].x - nodes[j].x;
          const dy = nodes[i].y - nodes[j].y;
          const dist = Math.sqrt(dx * dx + dy * dy);
          if (dist < EDGE_DISTANCE) {
            const opacity = (1 - dist / EDGE_DISTANCE) * 0.3 * (0.7 + 0.3 * breathe);
            ctx!.strokeStyle = edgeColor;
            ctx!.globalAlpha = opacity;
            ctx!.lineWidth = 1;
            ctx!.beginPath();
            ctx!.moveTo(nodes[i].x, nodes[i].y);
            ctx!.lineTo(nodes[j].x, nodes[j].y);
            ctx!.stroke();
          }
        }
      }

      // Draw nodes
      for (const node of nodes) {
        ctx!.globalAlpha = 0.4;
        ctx!.fillStyle = nodeColor;
        ctx!.beginPath();
        ctx!.arc(node.x, node.y, node.radius, 0, Math.PI * 2);
        ctx!.fill();
      }

      ctx!.globalAlpha = 1;
    }

    frameId = requestAnimationFrame(animate);

    return () => {
      cancelAnimationFrame(frameId);
      resizeObserver.disconnect();
      themeObserver.disconnect();
      container.removeEventListener("mousemove", handleMouseMove);
      container.removeEventListener("mouseleave", handleMouseLeave);
    };
  }, []);

  return (
    <canvas
      ref={canvasRef}
      style={{
        position: "absolute",
        inset: 0,
        pointerEvents: "none",
      }}
    />
  );
}
