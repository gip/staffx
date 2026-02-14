import { ArrowLeft } from "lucide-react";
import { Link } from "./link";
import { useAuth } from "./auth-context";

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
}

export function UserProfilePage({
  profile,
}: UserProfilePageProps) {
  const { user } = useAuth();
  const isOwnProfile = user?.handle === profile.handle;

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
