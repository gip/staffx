import { useState, useEffect, useRef } from "react";
import { Plus } from "lucide-react";
import { useAuth } from "./auth-context";
import { Link } from "./link";

export interface Thread {
  id: string;
  title: string | null;
  description: string | null;
  projectThreadId: number | null;
  status: string;
  createdBy?: string;
  createdAt?: string;
  updatedAt: string;
}

export interface Project {
  id: string;
  name: string;
  description: string | null;
  accessRole: string;
  ownerHandle: string;
  createdAt: string;
  threads: Thread[];
}

const TEMPLATES = [
  { id: "blank", label: "Blank", description: "Empty project, start from scratch" },
  {
    id: "webserver-postgres-auth0-google-vercel",
    label: "Webserver + Postgres + Auth0 Google login + Vercel",
    description: "Seeded stack with topology, architecture concern, and spec docs",
  },
];

interface CreateModalProps {
  onClose: () => void;
  onCreate: (data: { name: string; description: string; template: string }) => Promise<{ error?: string } | void>;
  onCheckName?: (name: string) => Promise<boolean>;
}

function CreateProjectModal({ onClose, onCreate, onCheckName }: CreateModalProps) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [template, setTemplate] = useState("blank");
  const [submitting, setSubmitting] = useState(false);
  const [duplicateError, setDuplicateError] = useState("");
  const [submitError, setSubmitError] = useState("");
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(null);

  const nameTrimmed = name.trim();
  const nameValid = /^[a-zA-Z0-9]([a-zA-Z0-9_-]*[a-zA-Z0-9])?$/.test(nameTrimmed) && !/[-_]{2}/.test(nameTrimmed);
  const formatError = nameTrimmed.length > 0 && !nameValid
    ? "Letters and numbers only, no leading/trailing - or _, no consecutive - or _"
    : name.length > 0 && name !== nameTrimmed
      ? "No spaces at the beginning or end"
      : "";
  const nameError = formatError || duplicateError;
  const canSubmit = nameTrimmed.length > 0 && nameValid && name === nameTrimmed && !submitting && !duplicateError;

  useEffect(() => {
    setDuplicateError("");
    if (debounceRef.current) clearTimeout(debounceRef.current);
    if (!nameTrimmed || !nameValid || !onCheckName) return;
    const currentName = nameTrimmed;
    debounceRef.current = setTimeout(async () => {
      const available = await onCheckName(currentName);
      if (!available) setDuplicateError("A project with this name already exists");
    }, 300);
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current); };
  }, [nameTrimmed, nameValid, onCheckName]);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!canSubmit) return;
    setSubmitting(true);
    setSubmitError("");
    const result = await onCreate({ name: name.trim(), description: description.trim(), template });
    if (result?.error) {
      setSubmitError(result.error);
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
            onChange={(e) => setName(e.target.value)}
            placeholder="my-project"
            autoFocus
          />
          {nameError && <span className="field-error">{nameError}</span>}
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

        {submitError && <p className="field-error">{submitError}</p>}
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
  onCreateProject?: (data: { name: string; description: string; template: string }) => Promise<{ error?: string } | void>;
  onCheckProjectName?: (name: string) => Promise<boolean>;
}

export function Home({ projects, onCreateProject, onCheckProjectName }: HomeProps) {
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
            <Link key={p.id} to={`/${p.ownerHandle}/${p.name}`} className="project-card">
              <div className="project-card-name">
                {p.ownerHandle} / {p.name}
              </div>
              <span className="project-card-role">{p.accessRole}</span>
              {p.threads.length > 0 && (
                <ul className="project-card-threads">
                  {p.threads.map((t) => (
                    <li key={t.id}>
                      {t.projectThreadId != null ? `#${t.projectThreadId} ` : ""}
                      {t.title ?? "Untitled thread"}
                    </li>
                  ))}
                </ul>
              )}
            </Link>
          ))}
        </div>
      )}
      {showCreate && (
        <CreateProjectModal
          onClose={() => setShowCreate(false)}
          onCheckName={onCheckProjectName}
          onCreate={async (data) => {
            const result = await onCreateProject?.(data);
            if (result?.error) return result;
            setShowCreate(false);
          }}
        />
      )}
    </main>
  );
}
