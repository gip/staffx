import { useCallback, useEffect, useState } from "react";
import { Route, Routes, useNavigate, useParams, useSearchParams } from "react-router-dom";
import {
  AuthContext,
  Header,
  Home,
  ProjectPage,
  ProjectSettingsPage,
  ThreadPage,
  UserProfilePage,
  setNavigate,
  type AuthUser,
  type AssistantRunResponse,
  type Collaborator,
  type Concern,
  type UserProfile,
  type ChatMessage,
  type MatrixDocument,
  type MatrixCell,
  type MatrixCellDoc,
  type Project,
  type IntegrationProvider,
  type IntegrationConnectionStatus,
  type IntegrationStatusRecord,
  type ThreadDetail,
  type ThreadDetailPayload,
} from "@staffx/ui";

const API_URL = import.meta.env.VITE_API_URL ?? "http://localhost:3001";

interface ElectronAuthAPI {
  getState: () => Promise<{ isAuthenticated: boolean }>;
  getAccessToken: () => Promise<string | null>;
  login: () => void;
  logout: () => void;
  onStateChanged: (cb: (state: { isAuthenticated: boolean }) => void) => () => void;
}

interface ElectronAgentAPI {
  start: (params: { prompt: string; cwd?: string; allowedTools?: string[]; systemPrompt?: string; model?: string }) => Promise<{ threadId: string }>;
  stop: (threadId: string) => void;
  getStatus: (threadId: string) => Promise<{ status: string; sessionId: string | null } | null>;
  onMessage: (callback: (data: { threadId: string; message: unknown }) => void) => () => void;
  onDone: (callback: (data: { threadId: string; status: string }) => void) => () => void;
}

declare global {
  interface Window {
    electronAPI: {
      platform: string;
      auth: ElectronAuthAPI;
      agent: ElectronAgentAPI;
    };
  }
}

function upsertMatrixCell(cells: MatrixCell[], nextCell: MatrixCell): MatrixCell[] {
  const index = cells.findIndex(
    (cell) => cell.nodeId === nextCell.nodeId && cell.concern === nextCell.concern,
  );
  if (index === -1) return [...cells, nextCell];
  return cells.map((cell, idx) => (idx === index ? nextCell : cell));
}

function upsertMatrixDocument(documents: MatrixDocument[], next: MatrixDocument): MatrixDocument[] {
  const index = documents.findIndex((document) => document.hash === next.hash);
  if (index === -1) return [...documents, next];
  return documents.map((document, idx) =>
    idx === index
      ? {
          ...document,
          title: next.title,
          kind: next.kind,
          language: next.language,
          text: next.text,
          sourceType: next.sourceType,
          sourceUrl: next.sourceUrl,
          sourceExternalId: next.sourceExternalId,
          sourceMetadata: next.sourceMetadata,
          sourceConnectedUserId: next.sourceConnectedUserId,
        }
      : document,
  );
}

function replaceMatrixDocumentReferences(
  cells: MatrixCell[],
  oldHash: string,
  nextDoc: MatrixDocument,
): MatrixCell[] {
  return cells.map((cell) => ({
    ...cell,
    docs: cell.docs.map((doc) =>
      doc.hash === oldHash
        ? {
            ...doc,
            hash: nextDoc.hash,
            title: nextDoc.title,
            kind: nextDoc.kind,
            language: nextDoc.language,
            sourceType: nextDoc.sourceType,
            sourceUrl: nextDoc.sourceUrl,
            sourceExternalId: nextDoc.sourceExternalId,
            sourceMetadata: nextDoc.sourceMetadata,
            sourceConnectedUserId: nextDoc.sourceConnectedUserId,
          }
        : doc,
    ),
  }));
}

function replaceMatrixDocumentInGlobalList(
  documents: MatrixDocument[],
  oldHash: string,
  nextDoc: MatrixDocument,
): MatrixDocument[] {
  const withoutOld = documents.filter((document) => document.hash !== oldHash);
  if (withoutOld.some((document) => document.hash === nextDoc.hash)) return withoutOld;
  return [...withoutOld, nextDoc];
}

type MatrixRefMutationResponse = {
  systemId: string;
  cell?: MatrixCell;
  cells?: MatrixCell[];
  messages?: ChatMessage[];
};

interface MatrixDocumentCreateResponse {
  systemId: string;
  document: MatrixDocument;
  cell?: MatrixCell;
  cells?: MatrixCell[];
  messages?: ChatMessage[];
}

interface MatrixDocumentReplaceResponse {
  systemId: string;
  oldHash: string;
  document: MatrixDocument;
  replacedRefs: number;
  messages?: ChatMessage[];
}

function normalizeMutationCells(response: MatrixRefMutationResponse): MatrixCell[] {
  const cells = response.cells?.length ? response.cells : response.cell ? [response.cell] : [];
  return cells.filter((cell): cell is MatrixCell => Boolean(cell.nodeId && cell.concern));
}

function applyMutationCells(
  currentCells: MatrixCell[],
  nextCells: MatrixCell[],
): MatrixCell[] {
  return nextCells.reduce((cells, nextCell) => upsertMatrixCell(cells, nextCell), currentCells);
}

function getAttachConcerns(
  payload?: {
    concern?: string;
    concerns?: string[];
  },
): string[] {
  const concernsFromList = Array.isArray(payload?.concerns)
    ? payload.concerns.map((concern) => concern.trim()).filter(Boolean)
    : [];
  const uniqueConcerns = Array.from(new Set(concernsFromList));
  if (uniqueConcerns.length > 0) return uniqueConcerns;
  const concern = payload?.concern?.trim();
  return concern ? [concern] : [];
}

function buildFallbackAttachedCells(
  existingCells: MatrixCell[],
  nodeId: string,
  concerns: string[],
  refType: "Document" | "Skill",
  document: MatrixDocument,
): MatrixCell[] {
  return concerns.map((concern) => {
    const existingCell = existingCells.find((cell) => cell.nodeId === nodeId && cell.concern === concern);
    const nextDoc: MatrixCellDoc = {
      hash: document.hash,
      title: document.title,
      kind: document.kind,
      language: document.language,
      refType,
      sourceType: document.sourceType,
      sourceUrl: document.sourceUrl,
      sourceExternalId: document.sourceExternalId,
      sourceMetadata: document.sourceMetadata,
      sourceConnectedUserId: document.sourceConnectedUserId,
    };

    const docs = existingCell ? existingCell.docs : [];
    const hasDoc = docs.some((entry) => entry.hash === nextDoc.hash && entry.refType === nextDoc.refType);
    const nextDocs = hasDoc ? docs : [...docs, nextDoc];

    return {
      nodeId,
      concern,
      docs: nextDocs,
      artifacts: existingCell?.artifacts ?? [],
    };
  });
}

function mergeChatMessages(current: ChatMessage[], incoming: ChatMessage[]) {
  const existing = new Set(current.map((message) => message.id));
  const next = incoming.filter((message) => !existing.has(message.id));
  return [...current, ...next];
}

function applyTopologyPositions(
  detail: ThreadDetailPayload,
  positions: Array<{ nodeId: string; x: number; y: number }>,
): ThreadDetailPayload {
  const byNodeId = new Map(positions.map((position) => [position.nodeId, position]));
  return {
    ...detail,
    topology: {
      ...detail.topology,
      nodes: detail.topology.nodes.map((node) => {
        const position = byNodeId.get(node.id);
        if (!position) return node;
        return { ...node, layoutX: position.x, layoutY: position.y };
      }),
    },
    matrix: {
      ...detail.matrix,
      nodes: detail.matrix.nodes.map((node) => {
        const position = byNodeId.get(node.id);
        if (!position) return node;
        return { ...node, layoutX: position.x, layoutY: position.y };
      }),
    },
  };
}

async function readError(res: Response, fallback: string) {
  const body = await res.json().catch(() => ({}));
  if (typeof body.code === "string" && body.code === "INTEGRATION_RECONNECT") {
    const provider = typeof body.provider === "string" ? body.provider : null;
    const status = typeof body.status === "string" ? body.status : null;
    if (provider) {
      return `${body.error ?? "Reauthentication required"} (${provider}${
        status ? ` - ${status}` : ""
      })`;
    }
  }
  if (typeof body.error === "string" && body.error) return body.error;
  return fallback;
}

function useApi() {
  const apiFetch = useCallback(
    async (path: string, init?: RequestInit) => {
      const token = await window.electronAPI.auth.getAccessToken();
      if (!token) throw new Error("Not authenticated");

      return fetch(`${API_URL}${path}`, {
        ...init,
        headers: { Authorization: `Bearer ${token}`, ...init?.headers },
      });
    },
    [],
  );

  return apiFetch;
}

function NavigateSync() {
  const navigate = useNavigate();

  useEffect(() => {
    setNavigate((to) => navigate(to));
  }, [navigate]);

  return null;
}

function HomeRoute({
  projects,
  setProjects,
}: {
  projects: Project[];
  setProjects: React.Dispatch<React.SetStateAction<Project[]>>;
}) {
  const apiFetch = useApi();

  return (
    <Home
      projects={projects}
      onCheckProjectName={async (name) => {
        try {
          const res = await apiFetch(`/projects/check-name?name=${encodeURIComponent(name)}`);
          if (!res.ok) return true;
          const data = await res.json();
          return data.available;
        } catch {
          return true;
        }
      }}
      onCreateProject={async (data) => {
        try {
          const res = await apiFetch("/projects", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(data),
          });
          if (!res.ok) {
            const body = await res.json().catch(() => ({}));
            return { error: body.error ?? "Failed to create project" };
          }
          const project = await res.json();
          setProjects((prev) => [project, ...prev]);
        } catch (error) {
          if (error instanceof Error && error.message.trim()) {
            return { error: error.message };
          }
          return { error: "Failed to create project" };
        }
      }}
    />
  );
}

function ProfileRoute({ isAuthenticated }: { isAuthenticated: boolean }) {
  const { handle } = useParams<{ handle: string }>();
  const apiFetch = useApi();
  const [searchParams, setSearchParams] = useSearchParams();
  const [profile, setProfile] = useState<UserProfile | null>(null);
  const [notFound, setNotFound] = useState(false);
  const [integrationStatuses, setIntegrationStatuses] = useState<IntegrationStatusRecord>({
    notion: "disconnected",
    google: "disconnected",
  });

  const refreshIntegrationStatuses = useCallback(async () => {
    const nextStatuses: IntegrationStatusRecord = {
      notion: "disconnected",
      google: "disconnected",
    };
    await Promise.all(
      (["notion", "google"] as IntegrationProvider[]).map(async (provider) => {
        try {
          const res = await apiFetch(`/integrations/${provider}/status`);
          if (!res.ok) return;
          const data = (await res.json()) as { status: IntegrationStatusRecord[IntegrationProvider] };
          nextStatuses[provider] = data.status;
        } catch {
          // Keep disconnected fallback.
        }
      }),
    );
    setIntegrationStatuses(nextStatuses);
  }, [apiFetch]);

  useEffect(() => {
    if (!isAuthenticated) return;
    void refreshIntegrationStatuses();
  }, [isAuthenticated, refreshIntegrationStatuses]);

  useEffect(() => {
    const provider = searchParams.get("integration");
    const status = searchParams.get("integration_status");
    if (!provider || !status) return;
    if (isAuthenticated) {
      void refreshIntegrationStatuses();
    }
    setSearchParams((params) => {
      params.delete("integration");
      params.delete("integration_status");
      return params;
    }, { replace: true });
  }, [isAuthenticated, refreshIntegrationStatuses, searchParams, setSearchParams]);

  useEffect(() => {
    if (!isAuthenticated || !handle) return;

    setNotFound(false);
    setProfile(null);

    apiFetch(`/users/${encodeURIComponent(handle)}`)
      .then(async (res) => {
        if (!res.ok) {
          setNotFound(true);
          return;
        }
        setProfile(await res.json());
      })
      .catch(() => setNotFound(true));
  }, [isAuthenticated, handle, apiFetch]);

  if (notFound) {
    return (
      <main className="main">
        <p className="status-text">User not found</p>
      </main>
    );
  }

  if (!profile) {
    return (
      <main className="main">
        <p className="status-text">Loading…</p>
      </main>
    );
  }

  return (
    <UserProfilePage
      profile={profile}
      integrationStatuses={integrationStatuses}
      onConnectIntegration={async (provider, returnTo) => {
        const targetReturnTo = returnTo || `/${encodeURIComponent(profile.handle)}`;
        try {
          const res = await apiFetch(
            `/integrations/${provider}/authorize-url?returnTo=${encodeURIComponent(targetReturnTo)}`,
          );
          if (!res.ok) {
            const body = await res.json().catch(() => ({}));
            return { error: body.error ?? `Failed to connect ${provider}` };
          }
          const data = (await res.json()) as { url?: string };
          if (!data.url) {
            return { error: `Failed to retrieve ${provider} authorization URL` };
          }
          window.location.assign(data.url);
          return { status: "connected" };
        } catch {
          return { error: `Failed to connect ${provider}` };
        }
      }}
      onDisconnectIntegration={async (provider) => {
        try {
          const res = await apiFetch(`/integrations/${provider}/disconnect`, { method: "POST" });
          if (!res.ok) {
            return { error: await readError(res, `Failed to disconnect ${provider}`) };
          }
          const data = (await res
            .json()
            .catch(() => ({ status: "disconnected" as IntegrationConnectionStatus }))) as {
            status: IntegrationConnectionStatus;
          };
          await refreshIntegrationStatuses();
          return data;
        } catch {
          return { error: `Failed to disconnect ${provider}` };
        }
      }}
    />
  );
}

function ProjectRoute({ isAuthenticated }: { isAuthenticated: boolean }) {
  const { handle, project: projectName } = useParams<{ handle: string; project: string }>();
  const apiFetch = useApi();
  const [project, setProject] = useState<Project | null>(null);
  const [notFound, setNotFound] = useState(false);

  useEffect(() => {
    if (!isAuthenticated || !handle || !projectName) return;

    setNotFound(false);
    setProject(null);

    apiFetch(`/projects/${encodeURIComponent(handle)}/${encodeURIComponent(projectName)}`)
      .then(async (res) => {
        if (!res.ok) {
          setNotFound(true);
          return;
        }
        setProject(await res.json());
      })
      .catch(() => setNotFound(true));
  }, [isAuthenticated, handle, projectName, apiFetch]);

  if (notFound) {
    return (
      <main className="main">
        <p className="status-text">Project not found</p>
      </main>
    );
  }

  if (!project) {
    return (
      <main className="main">
        <p className="status-text">Loading…</p>
      </main>
    );
  }

  return (
    <ProjectPage
      project={project}
      onCloseThread={async (threadProjectId) => {
        try {
          const res = await apiFetch(
            `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${threadProjectId}/close`,
            { method: "POST" },
          );
          if (!res.ok) {
            return { error: await readError(res, "Failed to close thread") };
          }
          const data = (await res.json()) as { thread: ThreadDetail };
          setProject((current) => (
            current
              ? {
                  ...current,
                  threads: current.threads.map((thread) =>
                    thread.projectThreadId === threadProjectId ? { ...thread, status: data.thread.status } : thread,
                  ),
                }
              : current
          ));
          return data;
        } catch (error: unknown) {
          if (error instanceof Error && error.message.trim()) {
            return { error: error.message };
          }
          return { error: "Failed to close thread" };
        }
      }}
      onCommitThread={async (threadProjectId) => {
        try {
          const res = await apiFetch(
            `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${threadProjectId}/commit`,
            { method: "POST" },
          );
          if (!res.ok) {
            return { error: await readError(res, "Failed to commit thread") };
          }
          const data = (await res.json()) as { thread: ThreadDetail };
          setProject((current) => (
            current
              ? {
                  ...current,
                  threads: current.threads.map((thread) =>
                    thread.projectThreadId === threadProjectId ? { ...thread, status: data.thread.status } : thread,
                  ),
                }
              : current
          ));
          return data;
        } catch (error: unknown) {
          if (error instanceof Error && error.message.trim()) {
            return { error: error.message };
          }
          return { error: "Failed to commit thread" };
        }
      }}
      onCloneThread={async (threadProjectId, payload) => {
        const title = typeof payload?.title === "string" ? payload.title.trim() : "";
        const description = typeof payload?.description === "string" ? payload.description.trim() : "";
        try {
          const res = await apiFetch(
            `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${threadProjectId}/clone`,
            {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ title, description }),
            },
          );
          if (!res.ok) {
            return { error: await readError(res, "Failed to create thread") };
          }
          const data = (await res.json()) as { thread: ThreadDetail };
          if (!data?.thread?.projectThreadId) {
            return { error: "New thread not found" };
          }
          return data;
        } catch (error: unknown) {
          if (error instanceof Error && error.message.trim()) {
            return { error: error.message };
          }
          return { error: "Failed to create thread" };
        }
      }}
    />
  );
}

function SettingsRoute({ isAuthenticated }: { isAuthenticated: boolean }) {
  const { handle, project: projectName } = useParams<{ handle: string; project: string }>();
  const apiFetch = useApi();
  const [data, setData] = useState<{
    accessRole: string;
    visibility: "public" | "private";
    collaborators: Collaborator[];
    projectRoles: string[];
    concerns: Concern[];
  } | null>(null);
  const [notFound, setNotFound] = useState(false);

  useEffect(() => {
    if (!isAuthenticated || !handle || !projectName) return;

    apiFetch(`/projects/${encodeURIComponent(handle)}/${encodeURIComponent(projectName)}/collaborators`)
      .then(async (res) => {
        if (!res.ok) {
          setNotFound(true);
          return;
        }
        setData(await res.json());
      })
      .catch(() => setNotFound(true));
  }, [isAuthenticated, handle, projectName, apiFetch]);

  if (notFound) {
    return (
      <main className="main">
        <p className="status-text">Project not found</p>
      </main>
    );
  }

  if (!data) {
    return (
      <main className="main">
        <p className="status-text">Loading…</p>
      </main>
    );
  }

  return (
    <ProjectSettingsPage
      projectOwnerHandle={handle!}
      projectName={projectName!}
      accessRole={data.accessRole}
      visibility={data.visibility}
      collaborators={data.collaborators}
      projectRoles={data.projectRoles}
      concerns={data.concerns}
      onSearchUsers={async (q) => {
        const res = await apiFetch(`/users/search?q=${encodeURIComponent(q)}`);
        if (!res.ok) return [];
        return res.json();
      }}
      onAddCollaborator={async (targetHandle, role, projectRoles) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/collaborators`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ handle: targetHandle, role, projectRoles }),
          },
        );
        if (!res.ok) {
          const body = await res.json().catch(() => ({}));
          return { error: body.error ?? "Failed to add collaborator" };
        }
      }}
      onRemoveCollaborator={async (targetHandle) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/collaborators/${encodeURIComponent(targetHandle)}`,
          { method: "DELETE" },
        );
        if (!res.ok && res.status !== 204) {
          const body = await res.json().catch(() => ({}));
          return { error: body.error ?? "Failed to remove collaborator" };
        }
      }}
      onAddRole={async (name) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/roles`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name }),
          },
        );
        if (!res.ok) {
          const body = await res.json().catch(() => ({}));
          return { error: body.error ?? "Failed to add role" };
        }
      }}
      onAddConcern={async (name) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/concerns`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name }),
          },
        );
        if (!res.ok) {
          const body = await res.json().catch(() => ({}));
          return { error: body.error ?? "Failed to add concern" };
        }
      }}
      onDeleteConcern={async (name) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/concerns/${encodeURIComponent(name)}`,
          { method: "DELETE" },
        );
        if (!res.ok && res.status !== 204) {
          const body = await res.json().catch(() => ({}));
          return { error: body.error ?? "Failed to delete concern" };
        }
      }}
      onDeleteRole={async (name) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/roles/${encodeURIComponent(name)}`,
          { method: "DELETE" },
        );
        if (!res.ok && res.status !== 204) {
          const body = await res.json().catch(() => ({}));
          return { error: body.error ?? "Failed to delete role" };
        }
      }}
      onUpdateMemberRoles={async (targetHandle, projectRoles) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/collaborators/${encodeURIComponent(targetHandle)}/roles`,
          {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ projectRoles }),
          },
        );
        if (!res.ok) {
          const body = await res.json().catch(() => ({}));
          return { error: body.error ?? "Failed to update roles" };
        }
      }}
      onUpdateVisibility={async (visibility) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/visibility`,
          {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ visibility }),
          },
        );
        if (!res.ok) {
          const body = await res.json().catch(() => ({}));
          return { error: body.error ?? "Failed to update visibility" };
        }
      }}
    />
  );
}

function ThreadRoute({ isAuthenticated }: { isAuthenticated: boolean }) {
  const { handle, project: projectName, threadId } = useParams<{
    handle: string;
    project: string;
    threadId: string;
  }>();
  const apiFetch = useApi();
  const [searchParams, setSearchParams] = useSearchParams();
  const [detail, setDetail] = useState<ThreadDetailPayload | null>(null);
  const [notFound, setNotFound] = useState(false);
  const [integrationStatuses, setIntegrationStatuses] = useState<IntegrationStatusRecord>({
    notion: "disconnected",
    google: "disconnected",
  });

  const refreshIntegrationStatuses = useCallback(async () => {
    const nextStatuses: IntegrationStatusRecord = {
      notion: "disconnected",
      google: "disconnected",
    };
    await Promise.all(
      (["notion", "google"] as IntegrationProvider[]).map(async (provider) => {
        try {
          const res = await apiFetch(`/integrations/${provider}/status`);
          if (!res.ok) return;
          const data = (await res.json()) as { status: IntegrationStatusRecord[IntegrationProvider] };
          nextStatuses[provider] = data.status;
        } catch {
          // Keep fallback disconnected.
        }
      }),
    );
    setIntegrationStatuses(nextStatuses);
  }, [apiFetch]);

  useEffect(() => {
    if (!isAuthenticated) return;
    void refreshIntegrationStatuses();
  }, [isAuthenticated, refreshIntegrationStatuses]);

  useEffect(() => {
    const provider = searchParams.get("integration");
    const status = searchParams.get("integration_status");
    if (!provider || !status) return;
    if (isAuthenticated) {
      void refreshIntegrationStatuses();
    }
    setSearchParams((params) => {
      params.delete("integration");
      params.delete("integration_status");
      return params;
    }, { replace: true });
  }, [isAuthenticated, refreshIntegrationStatuses, searchParams, setSearchParams]);

  useEffect(() => {
    if (!isAuthenticated || !handle || !projectName || !threadId) return;

    setNotFound(false);
    setDetail(null);

    apiFetch(
      `/projects/${encodeURIComponent(handle)}/${encodeURIComponent(projectName)}/thread/${encodeURIComponent(threadId)}`,
    )
      .then(async (res) => {
        if (res.status === 404) {
          setNotFound(true);
          return;
        }
        if (!res.ok) {
          throw new Error(await readError(res, "Failed to load thread"));
        }
        setDetail(await res.json());
      })
      .catch(() => setNotFound(true));
  }, [isAuthenticated, handle, projectName, threadId, apiFetch]);

  if (notFound) {
    return (
      <main className="main">
        <p className="status-text">Thread not found</p>
      </main>
    );
  }

  if (!detail) {
    return (
      <main className="main">
        <p className="status-text">Loading…</p>
      </main>
    );
  }

    return (
    <ThreadPage
      detail={detail}
      integrationStatuses={integrationStatuses}
      onUpdateThread={async (payload) => {
        try {
          const res = await apiFetch(
            `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${encodeURIComponent(threadId!)}`,
            {
              method: "PATCH",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            },
          );
          if (!res.ok) {
            return { error: await readError(res, "Failed to update thread") };
          }
          const data = (await res.json()) as { thread: ThreadDetail };
          setDetail((prev) => (prev ? { ...prev, thread: data.thread } : prev));
          return data;
        } catch (error: unknown) {
          if (error instanceof Error && error.message.trim()) {
            return { error: error.message };
          }
          return { error: "Failed to update thread" };
        }
      }}
      onSaveTopologyLayout={async (payload) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${encodeURIComponent(threadId!)}/topology/layout`,
          {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          },
        );
        if (!res.ok) {
          return { error: await readError(res, "Failed to save topology layout") };
        }
        const data = (await res.json()) as { systemId: string };
        setDetail((prev) => (
          prev
            ? {
                ...applyTopologyPositions(prev, payload.positions),
                systemId: data.systemId,
              }
            : prev
        ));
        return data;
      }}
      onAddMatrixDoc={async (payload) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${encodeURIComponent(threadId!)}/matrix/refs`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          },
        );
        if (!res.ok) {
          return { error: await readError(res, "Failed to add matrix document") };
        }
        const data = (await res.json()) as MatrixRefMutationResponse;
        const nextCells = normalizeMutationCells(data);
        setDetail((prev) => (
          prev
            ? {
                ...prev,
                systemId: data.systemId,
                matrix: { ...prev.matrix, cells: applyMutationCells(prev.matrix.cells, nextCells) },
                chat: {
                  ...prev.chat,
                  messages: mergeChatMessages(prev.chat.messages, data.messages ?? []),
                },
              }
            : prev
        ));
        return data;
      }}
      onRemoveMatrixDoc={async (payload) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${encodeURIComponent(threadId!)}/matrix/refs`,
          {
            method: "DELETE",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          },
        );
        if (!res.ok) {
          return { error: await readError(res, "Failed to remove matrix document") };
        }
        const data = (await res.json()) as MatrixRefMutationResponse;
        const nextCells = normalizeMutationCells(data);
        setDetail((prev) => (
          prev
            ? {
                ...prev,
                systemId: data.systemId,
                matrix: { ...prev.matrix, cells: applyMutationCells(prev.matrix.cells, nextCells) },
                chat: {
                  ...prev.chat,
                  messages: mergeChatMessages(prev.chat.messages, data.messages ?? []),
                },
              }
            : prev
        ));
        return data;
      }}
      onCreateMatrixDocument={async (payload) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${encodeURIComponent(threadId!)}/matrix/documents`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          },
        );
        if (!res.ok) {
          return { error: await readError(res, "Failed to create matrix document") };
        }
        const data = (await res.json()) as MatrixDocumentCreateResponse;
        setDetail((prev) => {
          if (!prev) return prev;
          const nextCells = normalizeMutationCells(data);
          const attachConcerns = payload.attach ? getAttachConcerns(payload.attach) : [];
          const fallbackCells =
            nextCells.length > 0
              ? nextCells
              : payload.attach && attachConcerns.length > 0
                ? buildFallbackAttachedCells(
                    prev.matrix.cells,
                    payload.attach.nodeId,
                    attachConcerns,
                    payload.attach.refType,
                    data.document,
                  )
                : data.cell
                  ? [data.cell]
                  : [];

          return {
            ...prev,
            systemId: data.systemId,
            matrix: {
              ...prev.matrix,
              documents: upsertMatrixDocument(prev.matrix.documents, data.document),
              cells: applyMutationCells(prev.matrix.cells, fallbackCells),
            },
            chat: {
              ...prev.chat,
              messages: mergeChatMessages(prev.chat.messages, data.messages ?? []),
            },
          };
        });
        return data;
      }}
      onReplaceMatrixDocument={async (documentHash, payload) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${encodeURIComponent(threadId!)}/matrix/documents/${encodeURIComponent(documentHash)}`,
          {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          },
        );
        if (!res.ok) {
          return { error: await readError(res, "Failed to replace matrix document") };
        }
        const data = (await res.json()) as MatrixDocumentReplaceResponse;
        setDetail((prev) => {
          if (!prev) return prev;
          return {
            ...prev,
            systemId: data.systemId,
            matrix: {
              ...prev.matrix,
              documents: replaceMatrixDocumentInGlobalList(
                prev.matrix.documents,
                data.oldHash,
                data.document,
              ),
              cells: replaceMatrixDocumentReferences(
                prev.matrix.cells,
                data.oldHash,
                data.document,
              ),
            },
            chat: {
              ...prev.chat,
              messages: mergeChatMessages(prev.chat.messages, data.messages ?? []),
            },
          };
        });
        return data;
      }}
      onSendChatMessage={async (payload) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${encodeURIComponent(threadId!)}/chat/messages`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          },
        );
        if (!res.ok) {
          return { error: await readError(res, "Failed to send chat message") };
        }
        const data = (await res.json()) as { messages: ChatMessage[] };
        setDetail((prev) => (
          prev
            ? {
                ...prev,
                chat: {
                  ...prev.chat,
                  messages: mergeChatMessages(prev.chat.messages, data.messages),
                },
              }
            : prev
        ));
        return data;
      }}
      onRunAssistant={async (payload) => {
        const res = await apiFetch(
          `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${encodeURIComponent(threadId!)}/assistant/run`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          },
        );
        if (!res.ok) {
          return { error: await readError(res, "Failed to run assistant") };
        }
        const data = (await res.json()) as AssistantRunResponse;
        setDetail((prev) => (
          prev
            ? {
                ...prev,
                systemId: data.systemId,
                chat: {
                  ...prev.chat,
                  messages: mergeChatMessages(prev.chat.messages, data.messages),
                },
              }
            : prev
        ));
        return data;
      }}
      onCloseThread={async () => {
        try {
          const res = await apiFetch(
            `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${encodeURIComponent(threadId!)}/close`,
            { method: "POST" },
          );
          if (!res.ok) {
            return { error: await readError(res, "Failed to close thread") };
          }
          const data = (await res.json()) as { thread: ThreadDetail };
          setDetail((prev) => (prev ? { ...prev, thread: data.thread } : prev));
          return data;
        } catch (error: unknown) {
          if (error instanceof Error && error.message.trim()) {
            return { error: error.message };
          }
          return { error: "Failed to close thread" };
        }
      }}
      onCommitThread={async () => {
        try {
          const res = await apiFetch(
            `/projects/${encodeURIComponent(handle!)}/${encodeURIComponent(projectName!)}/thread/${encodeURIComponent(threadId!)}/commit`,
            { method: "POST" },
          );
          if (!res.ok) {
            return { error: await readError(res, "Failed to commit thread") };
          }
          const data = (await res.json()) as { thread: ThreadDetail };
          setDetail((prev) => (prev ? { ...prev, thread: data.thread } : prev));
          return data;
        } catch (error: unknown) {
          if (error instanceof Error && error.message.trim()) {
            return { error: error.message };
          }
          return { error: "Failed to commit thread" };
        }
      }}
    />
  );
}

export function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [user, setUser] = useState<AuthUser | null>(null);
  const [projects, setProjects] = useState<Project[]>([]);

  useEffect(() => {
    const { auth } = window.electronAPI;

    auth.getState().then((state) => {
      setIsAuthenticated(state.isAuthenticated);
      setIsLoading(false);
    });

    const unsubscribe = auth.onStateChanged((state) => {
      setIsAuthenticated(state.isAuthenticated);
      setIsLoading(false);
    });

    return unsubscribe;
  }, []);

  useEffect(() => {
    if (!isAuthenticated) {
      setUser(null);
      setProjects([]);
      return;
    }

    const { auth } = window.electronAPI;

    auth.getAccessToken().then(async (token) => {
      if (!token) return;

      const headers = { Authorization: `Bearer ${token}` };

      try {
        const [meRes, projRes] = await Promise.all([
          fetch(`${API_URL}/me`, { headers }),
          fetch(`${API_URL}/projects`, { headers }),
        ]);

        if (meRes.ok) {
          const me = await meRes.json();
          setUser({ handle: me.handle, email: me.email ?? null, githubHandle: me.githubHandle ?? "", name: me.name, picture: me.picture });
        }

        if (projRes.ok) {
          setProjects(await projRes.json());
        }
      } catch (err) {
        console.error("API fetch failed:", err);
      }
    });
  }, [isAuthenticated]);

  return (
    <AuthContext.Provider
      value={{
        isAuthenticated,
        isLoading,
        user,
        login: () => window.electronAPI.auth.login(),
        logout: () => window.electronAPI.auth.logout(),
      }}
    >
      <NavigateSync />
      <Header variant="desktop" />
      <Routes>
        <Route path="/" element={<HomeRoute projects={projects} setProjects={setProjects} />} />
        <Route path="/:handle/:project" element={<ProjectRoute isAuthenticated={isAuthenticated} />} />
        <Route path="/:handle/:project/settings" element={<SettingsRoute isAuthenticated={isAuthenticated} />} />
        <Route path="/:handle" element={<ProfileRoute isAuthenticated={isAuthenticated} />} />
        <Route
          path="/:handle/:project/thread/:threadId"
          element={<ThreadRoute isAuthenticated={isAuthenticated} />}
        />
      </Routes>
    </AuthContext.Provider>
  );
}
