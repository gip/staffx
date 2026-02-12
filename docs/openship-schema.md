# OpenShip Domain Model

Reference for the OpenShip schema. Source of truth: `apps/api/src/migrations/001_init.sql`.

## Core Concepts

### System

An **immutable snapshot** of an entire architecture description. Everything (nodes, edges, concerns, documents, artifacts) is scoped to a `system_id`. When changes happen, the system is **forked** via `fork_system()` into a new snapshot rather than mutated in place. This gives full version history.

- Table: `systems`
- Key fields: `id` (text PK), `name`, `spec_version` (default `openship/v1`), `system_node_id`, `metadata` (jsonb)

### Node

A component in the architecture graph. Nodes form a **tree** (via `parent_id`, self-referencing with deferred FK) and come in five kinds:

| Kind | Purpose |
|------|---------|
| `System` | Top-level system boundary |
| `Host` | Machine / VM / server |
| `Container` | Docker container, app runtime |
| `Process` | Running process or service |
| `Library` | Code library or module |

- Table: `nodes` — composite PK `(system_id, id)`
- Parent FK is `deferrable initially deferred` so insert order doesn't matter

### Edge

A **directed relationship** between two nodes within the same system.

| Type | Meaning |
|------|---------|
| `Runtime` | Calls/invokes at runtime |
| `Dataflow` | Data flows from source to target |
| `Dependency` | Build/deploy-time dependency |

- Table: `edges` — composite PK `(system_id, id)`
- FKs to `nodes` with `on delete cascade`

### Concern

A **cross-cutting dimension** of the system (e.g. "Authentication", "Logging", "Error Handling"). Concerns form the **columns** of the concern matrix.

- Table: `concerns` — composite PK `(system_id, name)`
- `position`: display/sort order
- `is_baseline`: marks fundamental concerns always present
- `scope`: optional narrowing

### Document

A **content-addressed input document**, deduplicated by hash. Three kinds:

| Kind | Purpose |
|------|---------|
| `Feature` | What the system should do |
| `Spec` | How it should work |
| `Skill` | Domain knowledge / capability |

- Table: `documents` — composite PK `(system_id, hash)`
- `supersedes`: hash of the previous version (for doc evolution)
- `language`: defaults to `'en'`

### Concern Matrix (`matrix_refs`)

The **node x concern grid**. Each cell links to documents by ref type. For example, node "Auth Service" + concern "Security" might reference a Feature doc, a Spec doc, and a Skill doc.

- Table: `matrix_refs` — composite PK `(system_id, node_id, concern, ref_type, doc_hash)`
- `ref_type` mirrors `doc_kind`: `Feature`, `Spec`, `Skill`
- Materialized view `matrix_view` pivots refs into `feature_refs`, `spec_refs`, `skill_refs` jsonb arrays per cell

### Artifact

An **output** produced for a specific node + concern cell.

| Type | Content |
|------|---------|
| `Summary` | Text description (stored in `text` column) |
| `Code` | Linked to files via `artifact_files` |
| `Docs` | Generated documentation |

- Table: `artifacts` — composite PK `(system_id, id)`
- FK to both `nodes` and `concerns`

### File Contents & Artifact Files

A **content-addressed file store**. Files are stored once globally, keyed by `sha256(file_path + '\n' + file_content)`.

- `file_contents`: global table, PK is the hash
- `artifact_files`: join table linking `(system_id, artifact_id)` → `file_hash`
- `upsert_file_content(path, content)`: helper function that computes hash, inserts if new, returns hash
- View `artifact_files_view` joins them for easy querying

## Collaboration Layer

### Project

A **workspace** owned by a user that groups threads together.

- Table: `projects` — text PK, FK to `users(id)` via `owner_id`
- `project_collaborators`: join table with `collaborator_role` enum (`Editor`, `Viewer`)
- View `user_projects`: resolves access role (Owner/Editor/Viewer) per user
- View `project_summary`: thread counts, collaborator count

### Thread

A **conversation/editing session** within a project. Starts from a `seed_system_id` (initial snapshot) and transforms it through a sequence of actions. Can be **cloned** from another thread.

- Table: `threads` — global text PK (`id`) plus project-scoped integer (`project_thread_id`)
- `title`: required thread name
- `description`: optional thread description
- `project_thread_id`: monotonic increasing per project (gaps allowed, no reuse)
- `seed_system_id`: the starting system snapshot
- `source_thread_id`: if cloned, points to the origin thread
- `status`: `'open'` or `'closed'`

### Action

A **step** within a thread, ordered by `position`.

| Type | Purpose |
|------|---------|
| `Chat` | Conversation with messages |
| `Edit` | Direct edit to the system |
| `Import` | Import external data |
| `Custom` | Extension point |

- Table: `actions` — composite PK `(thread_id, id)`
- `output_system_id`: the forked system snapshot produced by this action (null if no changes)
- `begin_action()` forks the current system; `commit_action_empty()` cleans up if nothing changed

### Message

Chat messages within a `Chat`-type action.

- Table: `messages` — composite PK `(thread_id, action_id, id)`
- `role`: `User`, `Assistant`, `System`
- Ordered by `position`

### Change

An **audit log entry** recording what a specific action did.

- Table: `changes` — composite PK `(thread_id, action_id, id)`
- `target_table`: which table was affected
- `operation`: `Create`, `Update`, `Delete`
- `previous` / `current`: jsonb snapshots for diffing

## Data Flow

```
Project
  └── Thread (seed_system_id → initial snapshot)
        ├── Action 1 (Chat) → fork → System v1.a.1
        │     ├── Messages (user ↔ assistant)
        │     └── Changes (audit log)
        ├── Action 2 (Edit) → fork → System v1.a.1.a.2
        │     └── Changes
        └── Action 3 (Chat) → no changes → output_system_id = null
```

### System Resolution

`thread_current_system(thread_id)` returns the latest system snapshot: the `output_system_id` of the most recent action that produced one, or the thread's `seed_system_id` if no action has produced changes yet.

### Content Deduplication

- **Documents**: content-addressed by spec-defined hash, stored once per `(system_id, hash)`
- **Files**: content-addressed by `sha256(path + '\n' + content)`, stored once globally in `file_contents`
- `fork_system()` deep-copies all system-scoped data but file contents are shared globally

## Key Functions

| Function | Purpose |
|----------|---------|
| `fork_system(source, new_id, name?)` | Deep-copy a system snapshot |
| `upsert_file_content(path, content)` | Store file, return hash |
| `thread_current_system(thread_id)` | Get latest snapshot for a thread |
| `create_thread(...)` | Start a new thread from a seed system |
| `clone_thread(...)` | Fork a thread from another thread's current state |
| `begin_action(thread_id, action_id, type)` | Fork system, create action |
| `commit_action_empty(thread_id, action_id)` | Clean up fork if action had no changes |
| `close_thread(thread_id)` | Mark thread as closed |
| `diff_artifact_files(sys_a, sys_b, artifact_id)` | Compare files between snapshots |
| `diff_thread(thread_id)` | All changes across a thread's actions |

## Key Views

| View | Purpose |
|------|---------|
| `matrix_view` (materialized) | Pivoted concern matrix with feature/spec/skill ref arrays |
| `artifact_files_view` | Artifacts joined with file contents |
| `node_overview` | Node stats: children, edges, refs, artifacts |
| `user_projects` | Projects accessible to each user with role |
| `thread_timeline` | Actions with input/output system IDs and change counts |
| `project_summary` | Project stats: threads, collaborators |
