import { templateWebserverPostgresAuth0GoogleVercel } from "./template-webserver-postgres-auth0-google-vercel.js";

export const BLANK_TEMPLATE_ID = "blank" as const;
export type NonBlankTemplateId = "webserver-postgres-auth0-google-vercel";
export type TemplateId = typeof BLANK_TEMPLATE_ID | NonBlankTemplateId;

export type TemplateNodeKind = "Host" | "Container" | "Process" | "Library";
export type TemplateEdgeType = "Runtime" | "Dataflow" | "Dependency";
export type TemplateDocKind = "Feature" | "Spec" | "Skill";
export type TemplateRefType = "Feature" | "Spec" | "Skill";

export interface TemplateNode {
  key: string;
  name: string;
  kind: TemplateNodeKind;
  parentKey?: string;
}

export interface TemplateEdge {
  fromKey: string;
  toKey: string;
  type: TemplateEdgeType;
  protocol?: string;
}

export interface TemplateConcern {
  name: string;
  position: number;
  isBaseline: boolean;
  scope?: string | null;
}

export interface TemplateDocument {
  key: string;
  kind: TemplateDocKind;
  title: string;
  language: string;
  text: string;
}

export interface TemplateMatrixRef {
  nodeKey: string;
  concern: string;
  refType: TemplateRefType;
  documentKey: string;
}

export interface TemplateDefinition {
  id: NonBlankTemplateId;
  label: string;
  description: string;
  nodes: TemplateNode[];
  edges: TemplateEdge[];
  concerns: TemplateConcern[];
  documents: TemplateDocument[];
  matrixRefs: TemplateMatrixRef[];
}

const templateRegistry: Record<NonBlankTemplateId, TemplateDefinition> = {
  [templateWebserverPostgresAuth0GoogleVercel.id]: templateWebserverPostgresAuth0GoogleVercel,
};

export function isKnownTemplateId(templateId: string): templateId is TemplateId {
  return templateId === BLANK_TEMPLATE_ID || templateId in templateRegistry;
}

export function getTemplateById(templateId: TemplateId): TemplateDefinition | null {
  if (templateId === BLANK_TEMPLATE_ID) return null;
  return templateRegistry[templateId];
}
