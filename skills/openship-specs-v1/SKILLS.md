---
name: openship-specs-v1
description: "OpenShip v1 specification. Use this skill whenever working with OpenShip system descriptions, validating payloads, generating or parsing OpenShip file-based or JSON representations, or building tooling that reads/writes the OpenShip format. Covers the node and edge model, concern matrix, shared input documents, node-local artifacts, hash identity, and conformance rules."
---

# OpenShip v1 Specification

Status: Draft v1 (normative where stated)

## 1. Purpose and Scope

OpenShip v1 defines a portable way to describe and share the architecture and implementation state of a software product.

OpenShip models a system as interconnected nodes plus a concern matrix (`node × concern`) that links reusable input documents to each node.

This specification defines:

- the node and edge model
- the concern matrix model
- shared input document types (`Feature`, `Spec`, `Skill`)
- node-local output artifact types (`Summary`, `Code`, `Docs`)
- a canonical file-based representation
- a canonical JSON representation
- strict validation rules
- extensibility and compatibility guidance

## 2. Terminology and Conventions

### 2.1 Normative Language

The key words `MUST`, `MUST NOT`, `SHOULD`, `SHOULD NOT`, and `MAY` are to be interpreted as normative requirements.

### 2.2 Core Terms

- **Node**: a typed architectural element (`System`, `Host`, `Container`, `Process`, `Library`).
- **Edge**: a directed, typed connection between eligible node endpoints.
- **Concern**: a category used in the matrix (for example, `Interfaces`).
- **Cell**: one `(node, concern)` entry in the matrix.
- **Input Document**: reusable shared source content (`Feature`, `Spec`, `Skill`).
- **Artifact**: output content produced for a single node (`Summary`, `Code`, `Docs`).
- **Content Hash**: deterministic identifier derived from canonical content.

### 2.3 Identifier Formats

- Shared document IDs MUST be content hashes in the form `sha256:<64 lowercase hex chars>`.
- Node IDs, edge IDs, and artifact IDs MUST be unique within a system payload.
- IDs MUST match regex `^[a-zA-Z0-9._:-]+$`. Payloads containing IDs that violate this pattern MUST be rejected by conforming validators.

## 3. Core Model and Graph Semantics

An OpenShip system is composed of:

- exactly one `System` node
- zero or more `Host`, `Container`, `Process`, and `Library` nodes
- zero or more typed edges
- a concern matrix over all nodes
- global shared input document stores
- node-local output artifacts

The model has two graph dimensions:

- **Containment graph** (parent-child): host/container/process hierarchy.
- **Connectivity graph** (edges): directed runtime/data connections.

Rules:

- `System` is freestanding and MUST NOT participate in edges.
- `Library` nodes may participate in `Dependency` edges only (see Section 5).
- Input documents are shared, reusable, and globally stored at system scope.
- Artifacts are node-local outputs and MUST NOT be shared across nodes.

A `Host` node with no `parentId` is implicitly contained by the `System` node. This does not alter the System node's freestanding status for edge purposes; it only establishes logical containment scope.

## 4. Node Types

### 4.1 Node Kind Definitions

- **System**: logical root for the full product description.
- **Host**: execution environment that may contain containers and/or processes.
- **Container**: grouped runtime unit that may contain processes.
- **Process**: executable runtime component.
- **Library**: reusable code module, freestanding for containment in v1.

### 4.2 Allowed Containment

- `Host` MAY contain `Container` and `Process`.
- `Container` MAY contain `Process`.
- `Process` MUST NOT contain children.
- `Library` MUST be freestanding (no parent, no children).
- `System` MUST be freestanding for containment (no parent).
- `Host` nodes without a `parentId` are implicitly scoped under the `System` node.

### 4.3 Canonical Node Shape

```json
{
  "id": "p.api",
  "kind": "Process",
  "name": "API",
  "parentId": "c.backend",
  "metadata": {
    "runtime": "node",
    "language": "typescript"
  },
  "matrix": {
    "Interfaces": {
      "featureRefs": ["sha256:..."],
      "specRefs": ["sha256:..."],
      "skillRefs": ["sha256:..."]
    }
  }
}
```

Field requirements:

- `id`, `kind`, `name`, `matrix` are REQUIRED.
- `parentId` is REQUIRED for `Container` and `Process`; forbidden for `System` and `Library`; optional for `Host`.
- `metadata` is OPTIONAL and extensible.
- `matrix` keys MUST reference known concerns.

## 5. Edge Model and Metadata

Edges describe directed runtime, data, or dependency interactions between nodes.

### 5.1 Edge Types

OpenShip v1 defines three edge types:

| Edge Type | Semantics | Constraints |
|-----------|-----------|-------------|
| `Runtime` | Synchronous or asynchronous runtime communication (HTTP, gRPC, message queues) | Cycles permitted. Source: `Process`. Target: `Process` or `Container`. |
| `Dataflow` | Directed data pipeline or transformation dependency | MUST be acyclic (DAG). Source: `Process`. Target: `Process` or `Container`. |
| `Dependency` | Build-time or code-level dependency on a library | MUST be acyclic (DAG). Source: `Process`. Target: `Library`. |

### 5.2 Canonical Edge Shape

```json
{
  "id": "e.api.db",
  "type": "Runtime",
  "fromNodeId": "p.api",
  "toNodeId": "p.db",
  "metadata": {
    "protocol": "pgwire",
    "speedMbps": 1000,
    "latencyMs": 2,
    "notes": "primary DB path"
  }
}
```

### 5.3 Endpoint Rules

- For `Runtime` and `Dataflow` edges: `fromNodeId` MUST reference a `Process` node. `toNodeId` MUST reference a `Process` or `Container` node.
- For `Dependency` edges: `fromNodeId` MUST reference a `Process` node. `toNodeId` MUST reference a `Library` node.
- Edges to or from `System` are forbidden for all edge types.

### 5.4 Edge Target Semantics for Containers

When a `Runtime` or `Dataflow` edge targets a `Container` node, this represents an opaque routing boundary. The edge indicates that the source Process communicates with the Container as a unit, without specifying which internal Process handles the interaction. Tooling SHOULD interpret this as: the Container exposes a stable entry point (such as a load balancer, ingress, or service mesh endpoint) and internal routing is an implementation detail.

For analysis or flattening purposes, tooling MAY expand a Container-targeted edge into individual Process-targeted edges based on the Container's children, but this expansion is not required for conformance.

### 5.5 Metadata Rules

- `metadata.protocol` is OPTIONAL. When present, it describes the wire protocol (e.g., `"http"`, `"grpc"`, `"pgwire"`, `"filesystem"`).
- `metadata.description` is OPTIONAL. A short human-readable description of the edge's purpose.
- `metadata.speedMbps` is OPTIONAL numeric throughput.
- Additional metadata fields MAY be included.

## 6. Concern Matrix (`node × concern`)

### 6.1 Baseline Concerns

The OpenShip v1 baseline concern set, with scoping guidance:

| Concern | Scope |
|---------|-------|
| `Features` | Functional and non-functional requirements allocated to a node. |
| `General Specs` | Cross-cutting design specifications not captured by a domain-specific concern. |
| `General Skills` | Cross-cutting execution guidance not captured by a domain-specific concern. |
| `Data Model` | Schema, entity relationships, and data lifecycle for a node. |
| `Interfaces` | API contracts, message schemas, and integration surfaces. |
| `Connectivity` | Network topology, service discovery, and routing configuration. |
| `Security` | Authentication, authorization, encryption, and threat model. |
| `Implementation` | Language, framework, and architectural pattern choices. |
| `Deployment` | Build, packaging, release, and infrastructure configuration. |

### 6.2 Extensible Concern Registry

A system MAY define additional concerns (for example `Performance`, `Measurement`).

Rules:

- Baseline concerns MUST retain exact spelling.
- Additional concerns MUST be listed in the system concern registry.
- Unknown concerns SHOULD be preserved by tooling even if ignored.

### 6.3 Cell Model

Each cell contains only input document references:

```json
{
  "featureRefs": ["sha256:..."],
  "specRefs": ["sha256:..."],
  "skillRefs": ["sha256:..."]
}
```

Rules:

- Arrays MAY be empty.
- References MUST resolve in shared document stores.
- A single input document MAY be referenced by multiple cells across multiple nodes.

## 7. Input Documents and Output Artifacts

### 7.1 Shared Input Documents (Reusable)

Input document types:

- **Feature**: functional or non-functional requirement description.
- **Spec**: implementation or design specification for one aspect.
- **Skill**: guidance that enables specialized execution tasks.

Canonical shared document shape:

```json
{
  "kind": "Feature",
  "hash": "sha256:...",
  "title": "Public API authentication",
  "language": "en",
  "text": "..."
}
```

Rules:

- `kind` is REQUIRED and MUST be one of `Feature`, `Spec`, or `Skill`.
- `language` SHOULD be BCP-47 (`en` default).
- `text` MAY be Markdown or plain text.
- Input documents are system-scoped shared assets.

### 7.2 Output Artifacts (Node-Local, Non-Shared)

Artifact types:

- **Summary**: short synthesis of implementation or changes.
- **Code**: a set of source files belonging to a node, stored as real files on disk in the file-based representation.
- **Docs**: implementation documentation.

Canonical artifact shape (Summary, Docs):

```json
{
  "id": "a.p.api.interfaces.summary.v1",
  "nodeId": "p.api",
  "concern": "Interfaces",
  "type": "Summary",
  "text": "...",
  "language": "en"
}
```

Code artifact shape:

```json
{
  "id": "a.p.api.interfaces.code.v1",
  "nodeId": "p.api",
  "concern": "Interfaces",
  "type": "Code",
  "files": [
    { "filePath": "src/routes/auth.ts", "fileContent": "export ..." }
  ]
}
```

Code artifact rules:

- In the file-based representation, code files are stored as real files on disk under `nodes/<nodeId>/artifacts/code/` (see Section 10). The `filePath` is relative to that directory.
- In the JSON representation, code files are inline as `filePath` + `fileContent` pairs.
- A single file MAY be referenced by multiple `Code` artifacts within the same node. The file exists once on disk; multiple artifacts may list it.
- File content MUST be valid UTF-8 without NUL bytes.

General artifact rules:

- Every artifact MUST belong to exactly one node (`nodeId`).
- Artifacts MUST NOT be referenced by other nodes.
- Artifact IDs MUST be unique within the system.

## 8. Input Document Versioning

### 8.1 Problem Statement

Because input documents are content-addressed, any change to a document's text, title, or kind produces a new hash and thus a new document identity. Without additional structure, there is no way to express that one document is a revision of another.

### 8.2 Supersedes Relation

An input document MAY include an optional `supersedes` field containing the hash of the document it replaces:

```json
{
  "kind": "Spec",
  "hash": "sha256:new...",
  "title": "API auth contract v2",
  "language": "en",
  "text": "...",
  "supersedes": "sha256:old..."
}
```

### 8.3 Rules

- `supersedes` is OPTIONAL. When absent, the document is treated as an original (no predecessor).
- The `supersedes` hash SHOULD reference a document of the same kind, but cross-kind supersession (e.g., a Spec superseding a Feature) is permitted.
- Supersession chains MUST be acyclic. Tooling SHOULD detect and reject cycles.
- The superseded document MAY still exist in the store for historical purposes. Tooling MUST NOT require the superseded document to be present for validation to pass.

### 8.4 Hash Computation

The `supersedes` field is excluded from hash computation (see Section 12). This means a document's identity is determined solely by its `kind`, `title`, `language`, and `text`. The supersedes relation is metadata layered on top of content identity.

### 8.5 Artifact Versioning

Artifact IDs are opaque local identifiers. This specification does not mandate a versioning scheme for artifacts. However, the convention of suffixing with `.v1`, `.v2`, etc. is RECOMMENDED for human readability. Tooling MAY implement artifact version tracking as an extension.

## 9. Normative Validation Rules

A payload is OpenShip v1 conformant only if all rules pass.

1. Exactly one node has `kind = System`.
2. All node IDs are unique.
3. All edge IDs are unique.
4. All artifact IDs are unique.
5. Containment graph is acyclic.
6. `System` and `Library` are freestanding and have no parent.
7. `Host` may parent only `Container` or `Process`.
8. `Container` may parent only `Process`.
9. `Process` and `Library` have no children.
10. Every edge MUST have a `type` field with value `Runtime`, `Dataflow`, or `Dependency`.
11. For `Runtime` and `Dataflow` edges: source MUST be `Process`; target MUST be `Process` or `Container`.
12. For `Dependency` edges: source MUST be `Process`; target MUST be `Library`.
13. No edge of any type may include `System` as source or target.
14. The projected graph of `Dataflow` edges (Process-to-Process only) MUST be a DAG. `Runtime` edges are exempt from cycle constraints.
15. The projected graph of `Dependency` edges MUST be a DAG.
16. All matrix concern keys are declared in concern registry.
17. All `featureRefs` resolve in `documents.featuresByHash`.
18. All `specRefs` resolve in `documents.specsByHash`.
19. All `skillRefs` resolve in `documents.skillsByHash`.
20. Shared document hashes MUST match canonical hash algorithm (Section 12).
21. Baseline concerns MUST exist in registry with exact names.
22. Artifacts MUST reference an existing node.
23. All IDs MUST match regex `^[a-zA-Z0-9._:-]+$`.
24. Supersession chains in input documents MUST be acyclic.
25. Code artifact `filePath` values MUST be valid relative paths (no `..`, no absolute paths, no NUL bytes).

## 10. File-Based Canonical Representation

### 10.1 Directory Layout

```text
.
├── openship.yaml
├── inputs
│   ├── features
│   │   └── <hash>.md
│   ├── specs
│   │   └── <hash>.md
│   └── skills
│       └── <hash>.md
├── nodes
│   └── <nodeId>
│       ├── node.yaml
│       └── artifacts
│           ├── summary
│           │   └── <artifactId>.md
│           ├── docs
│           │   └── <artifactId>.md
│           └── code
│               └── <real source files>
└── edges
    └── edges.yaml
```

Code files live as real files on disk under `nodes/<nodeId>/artifacts/code/`. They can be opened, edited, linted, and compiled directly by any tool (including Claude Code). The `node.yaml` manifest tracks which files belong to which `Code` artifact.

### 10.2 Root Manifest (`openship.yaml`)

```yaml
specVersion: openship/v1
systemNodeId: sys.main
concerns:
  - Features
  - General Specs
  - General Skills
  - Data Model
  - Interfaces
  - Connectivity
  - Security
  - Implementation
  - Deployment
```

### 10.3 Input Document Files

Each shared input document is Markdown with front matter:

```markdown
---
kind: Feature
hash: sha256:8f...
title: API auth contract
language: en
supersedes: sha256:7a...    # optional
---

Document body in Markdown.
```

Rules:

- Location determines store placement (`features`, `specs`, `skills`) and MUST match the `kind` field.
- Filename MUST equal `<hash>.md`.
- Front matter `hash` MUST equal filename hash.

### 10.4 Node Manifest (`nodes/<nodeId>/node.yaml`)

```yaml
id: p.api
kind: Process
name: API
parentId: c.backend
metadata:
  runtime: node
matrix:
  Interfaces:
    featureRefs: [sha256:aaa...]
    specRefs: [sha256:bbb...]
    skillRefs: [sha256:ccc...]
  Security:
    featureRefs: []
    specRefs: [sha256:ddd...]
    skillRefs: []
artifacts:
  Summary:
    - a.p.api.interfaces.summary.v1
  Docs:
    - a.p.api.interfaces.docs.v1
  Code:
    - id: a.p.api.interfaces.code.v1
      concern: Interfaces
      files:
        - src/routes/auth.ts
        - src/middleware/session.ts
```

The `Code` artifact entries in `node.yaml` list the artifact ID, concern, and the file paths relative to `nodes/<nodeId>/artifacts/code/`. The actual source files exist at those paths on disk:

```text
nodes/p.api/
├── node.yaml
└── artifacts/
    ├── summary/
    │   └── a.p.api.interfaces.summary.v1.md
    ├── docs/
    │   └── a.p.api.interfaces.docs.v1.md
    └── code/
        └── src/
            ├── routes/
            │   └── auth.ts
            └── middleware/
                └── session.ts
```

### 10.5 Artifact Files

- `Summary` and `Docs` artifacts are Markdown files with front matter:

```markdown
---
id: a.p.api.interfaces.summary.v1
nodeId: p.api
concern: Interfaces
type: Summary
language: en
---

Summary text.
```

- `Code` artifacts are real source files on disk. There is no wrapper format — the files are stored directly. The artifact-to-file mapping is maintained in `node.yaml` (see Section 10.4).

### 10.6 Edge List (`edges/edges.yaml`)

```yaml
edges:
  - id: e.api.db
    type: Runtime
    fromNodeId: p.api
    toNodeId: p.db
    metadata:
      protocol: pgwire
      speedMbps: 1000
  - id: e.api.authlib
    type: Dependency
    fromNodeId: p.api
    toNodeId: lib.auth
    metadata:
      description: Authentication utility library
```

## 11. JSON-Based Canonical Full-System Representation

### 11.1 Canonical Top-Level Shape

```json
{
  "specVersion": "openship/v1",
  "concerns": [
    "Features",
    "General Specs",
    "General Skills",
    "Data Model",
    "Interfaces",
    "Connectivity",
    "Security",
    "Implementation",
    "Deployment"
  ],
  "documents": {
    "featuresByHash": {},
    "specsByHash": {},
    "skillsByHash": {}
  },
  "nodes": [],
  "edges": [],
  "artifacts": []
}
```

In the JSON representation, `Code` artifacts include file content inline as `filePath` + `fileContent` pairs. When converting from the file-based representation, tooling reads each real file and populates `fileContent`. When converting to the file-based representation, tooling writes each `fileContent` to its corresponding file path on disk.

### 11.2 Canonical Ordering for Deterministic Exports

For deterministic serialization, tools SHOULD sort: concerns by declared order in manifest; nodes, edges, and artifacts by `id`; and map keys lexicographically.

## 12. Hash Identity and Canonicalization Rules

### 12.1 Shared Document Hash Algorithm

Given a shared document, compute its hash from the following canonical payload:

```json
{
  "kind": "Feature|Spec|Skill",
  "language": "en",
  "text": "...",
  "title": "..."
}
```

Procedure:

1. Construct the canonical hash payload containing exactly the four fields: `kind`, `language`, `text`, `title`.
2. Serialize as JSON with lexicographically sorted keys (`kind`, `language`, `text`, `title`).
3. Encode as UTF-8 bytes without BOM.
4. Compute SHA-256 digest.
5. Encode as lowercase hex.
6. Prefix with `sha256:` to form the document hash.

The `supersedes` field, if present, is NOT included in hash computation. This means a document's hash is determined solely by its content identity (`kind` + `title` + `language` + `text`).

Consequence: identical content filed as a `Feature` and as a `Spec` will produce different hashes because `kind` differs. This is intentional — the document's classification is part of its identity.

### 12.2 Artifact Identity

Artifact IDs are not shared references; they are local identifiers. Artifact IDs MUST be unique per system payload. Artifact IDs SHOULD be deterministic when generated from content and `nodeId`.

### 12.3 Canonicalization Goals

Canonicalization ensures: stable hashes across file-based and JSON representations, safe deduplication and cross-node reuse for shared input documents, and deterministic round-trip conversion.

## 13. Extensibility and Versioning

### 13.1 Spec Versioning

- `specVersion` MUST be present.
- OpenShip v1 uses `openship/v1`.

### 13.2 Backward-Compatible Extension Points

- `metadata` objects on nodes and edges are open-ended.
- Additional concerns are allowed via concern registry.
- Unknown metadata keys MUST be preserved by compliant tooling.

### 13.3 Breaking Changes

The following require a new major spec version:

- changing node kind semantics
- changing edge endpoint rules or type definitions
- changing shared document hash algorithm
- changing canonical artifact type meanings (`Summary`, `Code`, `Docs`)

### 13.4 Payload Size Guidance

This specification does not impose hard size limits. However, implementations SHOULD enforce reasonable operational limits. Recommended maximums:

- Individual text field (document text, artifact text): 1 MB.
- Individual code file content: 1 MB.
- Total payload size (JSON representation): 100 MB.
- Individual filename in file-based representation: 255 bytes (to respect common filesystem limits).

Implementations MAY impose stricter limits and SHOULD document them.

## 14. End-to-End Examples

### 14.1 Minimal JSON Example

```json
{
  "specVersion": "openship/v1",
  "concerns": [
    "Features", "General Specs", "General Skills",
    "Data Model", "Interfaces", "Connectivity",
    "Security", "Implementation", "Deployment"
  ],
  "documents": {
    "featuresByHash": {
      "sha256:1111111111111111111111111111111111111111111111111111111111111111": {
        "kind": "Feature",
        "hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
        "title": "User login",
        "language": "en",
        "text": "Users can authenticate via OIDC."
      }
    },
    "specsByHash": {},
    "skillsByHash": {}
  },
  "nodes": [
    {
      "id": "sys.main",
      "kind": "System",
      "name": "Example System",
      "metadata": {},
      "matrix": {
        "Features": {
          "featureRefs": ["sha256:1111111111111111111111111111111111111111111111111111111111111111"],
          "specRefs": [],
          "skillRefs": []
        }
      }
    },
    {
      "id": "h.main",
      "kind": "Host",
      "name": "Main host",
      "metadata": {},
      "matrix": {}
    },
    {
      "id": "p.api",
      "kind": "Process",
      "name": "API",
      "parentId": "h.main",
      "metadata": { "runtime": "node" },
      "matrix": {}
    },
    {
      "id": "p.worker",
      "kind": "Process",
      "name": "Worker",
      "parentId": "h.main",
      "metadata": {},
      "matrix": {}
    },
    {
      "id": "lib.auth",
      "kind": "Library",
      "name": "Auth Library",
      "metadata": {},
      "matrix": {}
    }
  ],
  "edges": [
    {
      "id": "e.api.worker",
      "type": "Runtime",
      "fromNodeId": "p.api",
      "toNodeId": "p.worker",
      "metadata": { "protocol": "grpc" }
    },
    {
      "id": "e.api.authlib",
      "type": "Dependency",
      "fromNodeId": "p.api",
      "toNodeId": "lib.auth",
      "metadata": { "description": "Authentication utility library" }
    }
  ],
  "artifacts": [
    {
      "id": "a.p.api.impl.summary.v1",
      "nodeId": "p.api",
      "concern": "Implementation",
      "type": "Summary",
      "language": "en",
      "text": "Initial API scaffold created."
    },
    {
      "id": "a.p.api.interfaces.code.v1",
      "nodeId": "p.api",
      "concern": "Interfaces",
      "type": "Code",
      "files": [
        {
          "filePath": "src/routes/auth.ts",
          "fileContent": "export const authRoute = ..."
        }
      ]
    }
  ]
}
```

### 14.2 File-Based Example

Given the JSON above, the file-based representation would be:

```text
.
├── openship.yaml
├── inputs
│   └── features
│       └── sha256:1111...1111.md
├── nodes
│   ├── sys.main
│   │   └── node.yaml
│   ├── h.main
│   │   └── node.yaml
│   ├── p.api
│   │   ├── node.yaml
│   │   └── artifacts
│   │       ├── summary
│   │       │   └── a.p.api.impl.summary.v1.md
│   │       └── code
│   │           └── src
│   │               └── routes
│   │                   └── auth.ts        ← real file
│   ├── p.worker
│   │   └── node.yaml
│   └── lib.auth
│       └── node.yaml
└── edges
    └── edges.yaml
```

The `auth.ts` file contains the actual source code, not a YAML wrapper. The `node.yaml` for `p.api` tracks the artifact-to-file mapping.

### 14.3 Reuse and Supersession Example

One `Spec` hash may appear in multiple cells across multiple nodes. When a spec is revised, the new document may reference the old via `supersedes`, enabling tooling to track revision history while all matrix cells continue to reference the appropriate version by hash.

### 14.4 Round-Trip Requirement

A conforming converter MUST preserve semantic content when converting between file-based and JSON canonical representations. Preserved content includes IDs, hashes, concern assignments, matrix references, artifact ownership, edge types, supersession relations, and code file content.

## 15. Conformance Checklist

A producer/consumer is OpenShip v1 conformant when it satisfies all of the following:

- Parses and emits `specVersion: openship/v1`.
- Supports baseline concern names exactly.
- Preserves unknown concerns and metadata fields.
- Validates node containment and edge endpoint rules per edge type.
- Validates DAG constraint for `Dataflow` and `Dependency` edges only (not `Runtime` edges).
- Validates shared document hash references and hash format.
- Validates that `kind` in stored documents matches the store they appear in.
- Supports reusable shared input docs with optional `supersedes`.
- Enforces node-local artifact ownership (no cross-node artifact sharing).
- Stores `Code` artifact files as real files on disk in the file-based representation.
- Reads `Code` artifact file content inline in the JSON representation.
- Supports file-based canonical representation.
- Supports JSON-based canonical representation.
- Preserves semantics across round-trip conversion.
- Rejects IDs not matching the normative regex pattern.

---

*— End of OpenShip v1 Specification —*

