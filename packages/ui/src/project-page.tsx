import { Settings } from "lucide-react";
import { Link } from "./link";
import type { Project, Thread } from "./home";

interface ProjectPageProps {
  project: Project;
}

function formatDate(dateStr: string) {
  return new Date(dateStr).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  });
}

export function ProjectPage({ project }: ProjectPageProps) {
  return (
    <main className="page">
      <div className="page-header">
        <Link to="/" className="page-back">&larr;</Link>
        <h2 className="page-title">
          <span className="page-title-muted">{project.ownerHandle} / </span>
          {project.name}
        </h2>
        <div className="page-header-actions">
          <Link to={`/${project.ownerHandle}/${project.name}/settings`} className="btn-icon">
            <Settings size={16} />
          </Link>
        </div>
      </div>

      {project.description && (
        <p className="page-description">{project.description}</p>
      )}

      <section className="thread-section">
        <h3 className="thread-section-title">Threads</h3>
        {project.threads.length === 0 ? (
          <p className="status-text">No threads yet</p>
        ) : (
          <div className="thread-list">
            {project.threads.map((t: Thread) => (
              <Link
                key={t.id}
                to={`/${project.ownerHandle}/${project.name}/thread/${t.projectThreadId}`}
                className="thread-row"
              >
                <span className="thread-row-id">#{t.projectThreadId}</span>
                <span className="thread-row-title">{t.title ?? "Untitled"}</span>
                <span className={`thread-status thread-status--${t.status}`}>
                  {t.status === "open" ? "Open" : t.status === "closed" ? "Committed" : t.status}
                </span>
                <span className="thread-row-date">{formatDate(t.updatedAt)}</span>
              </Link>
            ))}
          </div>
        )}
      </section>
    </main>
  );
}
