import { Link } from "./link";
import type { Thread } from "./home";

interface ThreadPageProps {
  thread: Thread;
  ownerHandle: string;
  projectName: string;
}

export function ThreadPage({ thread, ownerHandle, projectName }: ThreadPageProps) {
  return (
    <main className="page">
      <div className="page-header">
        <Link to={`/${ownerHandle}/${projectName}`} className="page-back">&larr;</Link>
        <h2 className="page-title">
          <span className="page-title-muted">
            {ownerHandle} / {projectName} /
          </span>{" "}
          #{thread.projectThreadId}
        </h2>
      </div>

      <h3 className="thread-detail-title">{thread.title ?? "Untitled"}</h3>

      {thread.description && (
        <p className="page-description">{thread.description}</p>
      )}

      <div className="thread-detail-meta">
        <span className={`thread-status thread-status--${thread.status}`}>
          {thread.status}
        </span>
      </div>

      <div className="thread-detail-placeholder">
        <p className="status-text">Thread timeline coming soon</p>
      </div>
    </main>
  );
}
