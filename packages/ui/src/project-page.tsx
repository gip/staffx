import { useState } from "react";
import { Settings } from "lucide-react";
import { Link } from "./link";
import type { Project, Thread } from "./home";
import type { ThreadDetail } from "./thread-page";

interface MutationError {
  error: string;
}

type MutationResult<T> = T | MutationError | void;

interface ProjectPageProps {
  project: Project;
  onCloneThread?: (
    threadProjectId: number,
    payload: {
      title: string;
      description: string;
    },
  ) => Promise<MutationResult<{ thread: ThreadDetail }>>;
}

function formatDate(dateStr: string) {
  return new Date(dateStr).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  });
}

function getErrorMessage<T>(result: MutationResult<T>): string | null {
  if (!result || typeof result !== "object") return null;
  const normalized = result as Partial<MutationError>;
  if (typeof normalized.error === "string") return normalized.error;
  return null;
}

export function ProjectPage({ project, onCloneThread }: ProjectPageProps) {
  const [cloningThreadId, setCloningThreadId] = useState<string | null>(null);
  const [cloneErrors, setCloneErrors] = useState<Record<string, string>>({});
  const [cloneThreadDraft, setCloneThreadDraft] = useState<Thread | null>(null);
  const [cloneThreadTitle, setCloneThreadTitle] = useState("");
  const [cloneThreadDescription, setCloneThreadDescription] = useState("");
  const [isSubmittingClone, setIsSubmittingClone] = useState(false);
  const canCloneThreads = project.accessRole === "Owner" || project.accessRole === "Editor";

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
                <span className="thread-row-main">
                  <span className="thread-row-id">#{t.projectThreadId}</span>
                  <span className="thread-row-title">{t.title ?? "Untitled"}</span>
                  {canCloneThreads && t.status === "closed" && onCloneThread && (
                    <button
                      className="btn btn-secondary thread-row-action-button thread-row-clone-action"
                      type="button"
                      disabled={cloningThreadId === t.id}
                      onClick={(event) => {
                        event.preventDefault();
                        event.stopPropagation();
                        setCloneThreadDraft(t);
                        setCloneThreadTitle(t.title ?? "");
                        setCloneThreadDescription(t.description ?? "");
                        setCloneErrors((prev) => {
                          const next = { ...prev };
                          delete next[t.id];
                          return next;
                        });
                      }}
                    >
                      {cloningThreadId === t.id ? "Creating…" : "New Thread"}
                    </button>
                  )}
                  <span className={`thread-status thread-status--${t.status} thread-row-status`}>
                    {t.status === "open" ? "Open" : t.status === "closed" ? "Committed" : t.status}
                  </span>
                  <span className="thread-row-date">{formatDate(t.updatedAt)}</span>
                </span>
                {cloneErrors[t.id] && (
                  <span className="thread-row-actions thread-row-error">
                    <p className="field-error">{cloneErrors[t.id]}</p>
                  </span>
                )}
              </Link>
            ))}
          </div>
        )}
      </section>

      {cloneThreadDraft && onCloneThread && (
        <div
          className="modal-overlay"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget && !isSubmittingClone) {
              setCloneThreadDraft(null);
            }
          }}
        >
          <div className="modal">
            <h3 className="modal-title">Create new thread</h3>
            <label className="field">
              <span className="field-label">Name</span>
              <input
                className="field-input"
                type="text"
                value={cloneThreadTitle}
                onChange={(event) => setCloneThreadTitle(event.target.value)}
                disabled={isSubmittingClone}
              />
            </label>
            <label className="field">
              <span className="field-label">Description</span>
              <textarea
                className="field-input field-textarea"
                rows={5}
                value={cloneThreadDescription}
                onChange={(event) => setCloneThreadDescription(event.target.value)}
                disabled={isSubmittingClone}
              />
            </label>
            {cloneErrors[cloneThreadDraft.id] && <p className="field-error">{cloneErrors[cloneThreadDraft.id]}</p>}
            <div className="modal-actions">
              <button
                className="btn btn-secondary"
                type="button"
                disabled={isSubmittingClone}
                onClick={() => setCloneThreadDraft(null)}
              >
                Cancel
              </button>
              <button
                className="btn"
                type="button"
                disabled={isSubmittingClone || !cloneThreadTitle.trim()}
                onClick={async () => {
                  const draft = cloneThreadDraft;
                  if (!draft || typeof draft.projectThreadId !== "number") return;
                  const title = cloneThreadTitle.trim();
                  const description = cloneThreadDescription.trim();
                  if (!title) return;
                  setIsSubmittingClone(true);
                  setCloningThreadId(draft.id);
                  setCloneErrors((prev) => {
                    const next = { ...prev };
                    delete next[draft.id];
                    return next;
                  });
                  try {
                    const result = await onCloneThread(draft.projectThreadId, {
                      title,
                      description,
                    });
                    const error = getErrorMessage(result);
                    if (error) {
                      setCloneErrors((prev) => ({ ...prev, [draft.id]: error }));
                    } else {
                      setCloneThreadDraft(null);
                      setCloneThreadDescription("");
                      setCloneThreadTitle("");
                    }
                  } catch {
                    setCloneErrors((prev) => ({ ...prev, [draft.id]: "Failed to create thread" }));
                  } finally {
                    setCloningThreadId((current) => (current === draft.id ? null : current));
                    setIsSubmittingClone(false);
                  }
                }}
              >
                {isSubmittingClone ? "Creating…" : "Create"}
              </button>
            </div>
          </div>
        </div>
      )}
    </main>
  );
}
