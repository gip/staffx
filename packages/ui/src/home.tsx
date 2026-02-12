import { useState } from "react";
import { Plus } from "lucide-react";
import { useAuth } from "./auth-context";

export interface Project {
  id: string;
  name: string;
  description: string | null;
  accessRole: string;
  createdAt: string;
  threads: { id: string; title: string | null; status: string; updatedAt: string }[];
}

const TEMPLATES = [
  { id: "blank", label: "Blank", description: "Empty project, start from scratch" },
];

interface CreateModalProps {
  onClose: () => void;
  onCreate: (data: { name: string; description: string; template: string }) => Promise<string | null>;
}

function CreateProjectModal({ onClose, onCreate }: CreateModalProps) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [template, setTemplate] = useState("blank");
  const [submitting, setSubmitting] = useState(false);
  const [serverError, setServerError] = useState("");

  const nameTrimmed = name.trim();
  const nameValid = /^[a-zA-Z0-9]([a-zA-Z0-9_-]*[a-zA-Z0-9])?$/.test(nameTrimmed) && !/[-_]{2}/.test(nameTrimmed);
  const nameError = nameTrimmed.length > 0 && !nameValid
    ? "Letters and numbers only, no leading/trailing - or _, no consecutive - or _"
    : name.length > 0 && name !== nameTrimmed
      ? "No spaces at the beginning or end"
      : "";
  const canSubmit = nameTrimmed.length > 0 && nameValid && name === nameTrimmed && !submitting;

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!canSubmit) return;
    setSubmitting(true);
    setServerError("");
    const error = await onCreate({ name: name.trim(), description: description.trim(), template });
    if (error) {
      setServerError(error);
      setSubmitting(false);
    }
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <form className="modal" onClick={(e) => e.stopPropagation()} onSubmit={handleSubmit}>
        <h3 className="modal-title">New Project</h3>

        <label className="field">
          <span className="field-label">Name</span>
          <input
            className={`field-input${nameError ? " field-input--error" : ""}`}
            type="text"
            value={name}
            onChange={(e) => { setName(e.target.value); setServerError(""); }}
            placeholder="my-project"
            autoFocus
          />
          {nameError && <span className="field-error">{nameError}</span>}
          {serverError && <span className="field-error">{serverError}</span>}
        </label>

        <fieldset className="field">
          <span className="field-label">Template</span>
          <div className="template-list">
            {TEMPLATES.map((t) => (
              <label
                key={t.id}
                className={`template-option${template === t.id ? " template-option--selected" : ""}`}
              >
                <input
                  type="radio"
                  name="template"
                  value={t.id}
                  checked={template === t.id}
                  onChange={() => setTemplate(t.id)}
                />
                <span className="template-option-label">{t.label}</span>
                <span className="template-option-desc">{t.description}</span>
              </label>
            ))}
          </div>
        </fieldset>

        <label className="field">
          <span className="field-label">Description</span>
          <textarea
            className="field-input field-textarea"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            placeholder="Optional"
            rows={3}
          />
        </label>

        <div className="modal-actions">
          <button type="button" className="btn btn-secondary" onClick={onClose}>Cancel</button>
          <button type="submit" className="btn" disabled={!canSubmit}>
            {submitting ? "Creating…" : "Create"}
          </button>
        </div>
      </form>
    </div>
  );
}

interface HomeProps {
  projects: Project[];
  onCreateProject?: (data: { name: string; description: string; template: string }) => Promise<string | null>;
}

export function Home({ projects, onCreateProject }: HomeProps) {
  const { isAuthenticated, isLoading, login, user } = useAuth();
  const [showCreate, setShowCreate] = useState(false);

  if (isLoading) {
    return (
      <main className="main">
        <p className="status-text">Loading…</p>
      </main>
    );
  }

  if (!isAuthenticated) {
    return (
      <main className="hero">
        <h1 className="hero-tagline">Staff-level thinking, on demand.</h1>
        <button className="btn hero-cta" onClick={login}>Log In</button>
      </main>
    );
  }

  return (
    <main className="page">
      <div className="page-header">
        <h2 className="page-title">Projects</h2>
        <button
          className="btn-icon btn-icon-round"
          onClick={() => setShowCreate(true)}
          aria-label="New project"
        >
          <Plus size={16} />
        </button>
      </div>
      {projects.length === 0 ? (
        <p className="status-text">No projects yet</p>
      ) : (
        <div className="project-grid">
          {projects.map((p) => (
            <div key={p.id} className="project-card">
              <div className="project-card-name">
                {user?.githubHandle ? `${user.githubHandle} / ` : ""}{p.name}
              </div>
              <span className="project-card-role">{p.accessRole}</span>
              {p.threads.length > 0 && (
                <ul className="project-card-threads">
                  {p.threads.map((t) => (
                    <li key={t.id}>{t.title ?? "Untitled thread"}</li>
                  ))}
                </ul>
              )}
            </div>
          ))}
        </div>
      )}
      {showCreate && (
        <CreateProjectModal
          onClose={() => setShowCreate(false)}
          onCreate={async (data) => {
            const error = await onCreateProject?.(data) ?? null;
            if (!error) setShowCreate(false);
            return error;
          }}
        />
      )}
    </main>
  );
}
