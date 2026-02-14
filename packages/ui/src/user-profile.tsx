import { useState } from "react";
import { ArrowLeft } from "lucide-react";
import { Link } from "./link";
import { useAuth } from "./auth-context";
import type { IntegrationConnectionStatus, IntegrationProvider, IntegrationStatusRecord } from "./thread-page";

type MutationError = { error: string };
type MutationResult<T> = T | MutationError | void;

export interface UserProfile {
  handle: string;
  name: string | null;
  picture: string | null;
  githubHandle: string | null;
  memberSince: string;
  projects: UserProfileProject[];
}

export interface UserProfileProject {
  name: string;
  description: string | null;
  visibility: "public" | "private";
  ownerHandle: string;
  role: string;
  createdAt: string;
}

interface UserProfilePageProps {
  profile: UserProfile;
  integrationStatuses?: IntegrationStatusRecord;
  onConnectIntegration?: (provider: IntegrationProvider, returnTo: string) => Promise<MutationResult<{ status: IntegrationConnectionStatus }>>;
  onDisconnectIntegration?: (provider: IntegrationProvider) => Promise<MutationResult<{ status: IntegrationConnectionStatus }>>;
}

const INTEGRATION_LABELS: Record<IntegrationProvider, string> = {
  notion: "Notion",
  google: "Google Docs",
};

function getStatusLabel(status?: IntegrationConnectionStatus) {
  if (!status) return "Disconnected";
  if (status === "connected") return "Connected";
  return `Disconnected (${status})`;
}

const getErrorMessage = (result: MutationResult<{ status: IntegrationConnectionStatus }>) => {
  if (!result || typeof result !== "object") return null;
  const error = (result as MutationError).error;
  return typeof error === "string" ? error : "Failed to update integration";
};

function IntegrationRow({
  provider,
  status,
  isBusy,
  onConnect,
  onDisconnect,
}: {
  provider: IntegrationProvider;
  status: IntegrationConnectionStatus;
  isBusy: boolean;
  onConnect: () => Promise<void>;
  onDisconnect: () => Promise<void>;
}) {
  const isConnected = status === "connected";
  return (
    <div className="profile-integration-row">
      <div>
        <div className="profile-integration-name">{INTEGRATION_LABELS[provider]}</div>
        <div className="profile-integration-status">{getStatusLabel(status)}</div>
      </div>
      <button
        className="btn btn-secondary profile-integration-action"
        type="button"
        onClick={isConnected ? onDisconnect : onConnect}
        disabled={isBusy}
      >
        {isBusy ? (isConnected ? "Disconnecting..." : "Connecting...") : isConnected ? "Disconnect" : "Connect"}
      </button>
    </div>
  );
}

export function UserProfilePage({
  profile,
  integrationStatuses = { notion: "disconnected", google: "disconnected" },
  onConnectIntegration,
  onDisconnectIntegration,
}: UserProfilePageProps) {
  const { user } = useAuth();
  const [activeProvider, setActiveProvider] = useState<IntegrationProvider | null>(null);
  const [integrationError, setIntegrationError] = useState("");
  const isOwnProfile = user?.handle === profile.handle;

  const handleConnect = async (provider: IntegrationProvider) => {
    if (!onConnectIntegration) return;
    setIntegrationError("");
    setActiveProvider(provider);
    const result = await onConnectIntegration(provider, `/${profile.handle}`);
    setActiveProvider(null);
    const error = getErrorMessage(result);
    if (error) {
      setIntegrationError(error);
    }
  };

  const handleDisconnect = async (provider: IntegrationProvider) => {
    if (!onDisconnectIntegration) return;
    setIntegrationError("");
    setActiveProvider(provider);
    const result = await onDisconnectIntegration(provider);
    setActiveProvider(null);
    const error = getErrorMessage(result);
    if (error) {
      setIntegrationError(error);
    }
  };

  return (
    <div className="page">
      <div className="page-header">
        <Link to="/" className="page-back">
          <ArrowLeft size={20} />
        </Link>
        <h1 className="page-title">Profile</h1>
      </div>

      <div className="profile-header">
        {profile.picture ? (
          <img className="profile-avatar" src={profile.picture} alt="" />
        ) : (
          <span className="profile-avatar profile-avatar-fallback">
            {(profile.handle)[0].toUpperCase()}
          </span>
        )}
        <div className="profile-info">
          <span className="profile-name">{profile.name ?? profile.handle}</span>
          <span className="profile-handle">@{profile.handle}</span>
          {profile.githubHandle && (
            <span className="profile-github">github.com/{profile.githubHandle}</span>
          )}
          <span className="profile-member-since">
            Member since {new Date(profile.memberSince).toLocaleDateString(undefined, { year: "numeric", month: "long" })}
          </span>
        </div>
      </div>

      {isOwnProfile ? (
        <section className="thread-section profile-integrations">
          <h2 className="thread-section-title">Document integrations</h2>
          <p className="page-description">
            Connect Notion and Google Docs here so you can import remote documents from those sources.
          </p>
          <div className="profile-integration-list">
            {(["notion", "google"] as const).map((provider) => (
              <IntegrationRow
                key={provider}
                provider={provider}
                status={integrationStatuses[provider]}
                isBusy={activeProvider === provider}
                onConnect={() => handleConnect(provider)}
                onDisconnect={() => handleDisconnect(provider)}
              />
            ))}
          </div>
          {integrationError ? <p className="field-error">{integrationError}</p> : null}
        </section>
      ) : null}

      <div className="thread-section">
        <h2 className="thread-section-title">
          {isOwnProfile ? "Your projects" : "Shared projects"}
        </h2>
        {profile.projects.length === 0 ? (
          <p className="page-description">
            {isOwnProfile ? "You have no projects yet." : "No shared projects."}
          </p>
        ) : (
          <div className="project-grid">
            {profile.projects.map((project) => (
              <Link
                key={`${project.ownerHandle}/${project.name}`}
                to={`/${project.ownerHandle}/${project.name}`}
                className="project-card"
              >
                <div className="project-card-name">{project.name}</div>
                {project.description && (
                  <div className="project-card-role">{project.description}</div>
                )}
                <div className="project-card-role">{project.role} · {project.visibility}</div>
              </Link>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
