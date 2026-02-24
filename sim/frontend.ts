import {
  AssistantRun,
  AssistantType,
  encodeEventCursor,
  MatrixSnapshot,
  ProjectRow,
  SimApi,
  SimActorContext,
  StaffXEvent,
  ThreadRow,
  ChatMessage,
} from "./api";

export interface FrontendQueryOptions {
  actor: SimActorContext;
}

export interface EventPollOptions {
  aggregateType?: string;
  aggregateId?: string;
  orgId?: string | null;
  since?: string;
  limit?: number;
}

export interface SimProjectSeed {
  name: string;
  description?: string | null;
  visibility?: "public" | "private";
}

export interface OpenedThreadState {
  thread: ThreadRow;
  matrix: MatrixSnapshot;
  messages: ChatMessage[];
}

export interface RunWaitOptions {
  assistantType: AssistantType;
  prompt?: string;
  model?: string;
  chatMessageId?: string;
  timeoutMs?: number;
  pollMs?: number;
}

export interface RunWaitResult {
  run: AssistantRun;
  events: StaffXEvent[];
}

export interface FullWorkflowInput {
  project: SimProjectSeed;
  threadTitle?: string;
  threadDescription?: string | null;
  matrixPatch?: Array<{ nodeId: string; x: number; y: number }>;
  chatContent?: string;
  run?: {
    assistantType: AssistantType;
    prompt?: string;
    model?: string;
  };
  waitMs?: number;
  eventPollMs?: number;
}

export interface FullWorkflowResult {
  project: ProjectRow & { threadId: string };
  thread: ThreadRow;
  beforeMatrix: MatrixSnapshot;
  afterMatrix: MatrixSnapshot | null;
  run: AssistantRun;
  messages: ChatMessage[];
  events: StaffXEvent[];
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isSimErrorLike(error: unknown): error is { status: number; message: string } {
  return typeof error === "object" && error !== null && "status" in error && "message" in error;
}

export class SimFrontendClient {
  public readonly actor: SimActorContext;

  constructor(
    private readonly api: SimApi,
    actorInput: SimActorContext,
  ) {
    this.actor = actorInput;
  }

  static async bootstrapUser(api: SimApi, handle: string, orgId: string | null = null): Promise<SimActorContext> {
    return api.ensureUser({ handle, orgId });
  }

  async ensureProject(input: SimProjectSeed): Promise<ProjectRow & { threadId: string }> {
    const name = input.name.trim();
    if (!name) {
      throw new Error("project name required");
    }

    const existingList = await this.api.listProjects(this.actor, { name, pageSize: 20 });
    const existing = existingList.items.find((project) => project.name === name);
    if (existing) {
      const threads = await this.api.listThreads(this.actor, { projectId: existing.id, pageSize: 5 });
      const openThread = threads.items.find((thread) => thread.status === "open") ?? threads.items[0];
      const firstThread = openThread;
      if (!firstThread) {
        throw new Error(`project "${name}" exists but has no thread`);
      }
      return { ...existing, threadId: firstThread.id };
    }

    return this.api.createProject(this.actor, {
      name,
      description: input.description,
      visibility: input.visibility,
    });
  }

  async createThread(projectId: string, title?: string, description?: string | null): Promise<ThreadRow> {
    return this.api.createThread(this.actor, { projectId, title, description });
  }

  async openThreadState(threadId: string): Promise<OpenedThreadState> {
    const [thread, matrix, messages] = await Promise.all([
      this.api.getThread(this.actor, threadId),
      this.api.getMatrix(this.actor, threadId),
      this.api.listMessages(this.actor, threadId),
    ]);

    return { thread, matrix, messages };
  }

  async mutateMatrixLayout(threadId: string, payload: Array<{ nodeId: string; x: number; y: number }>): Promise<MatrixSnapshot> {
    if (!payload.length) {
      throw new Error("empty matrix layout");
    }
    return this.api.patchMatrixLayout(this.actor, threadId, payload);
  }

  async sendChat(threadId: string, content: string): Promise<ChatMessage> {
    return this.api.appendChatMessage(this.actor, threadId, { content });
  }

  async startRun(
    threadId: string,
    input: {
      assistantType: AssistantType;
      prompt?: string;
      model?: string;
      chatMessageId?: string;
    },
  ): Promise<AssistantRun> {
    return this.api.startRun(this.actor, threadId, input);
  }

  async queryEvents(input: EventPollOptions): Promise<{ items: StaffXEvent[]; nextCursor: string | null }> {
    return this.api.queryEvents(input);
  }

  async startRunAndWait(threadId: string, options: RunWaitOptions): Promise<RunWaitResult> {
    const sinceMs = Date.now();
    const run = await this.startRun(threadId, {
      assistantType: options.assistantType,
      prompt: options.prompt,
      model: options.model,
      chatMessageId: options.chatMessageId,
    });

    const timeoutMs = Math.max(1_000, options.timeoutMs ?? 20_000);
    const pollMs = Math.max(50, options.pollMs ?? 500);
    const deadline = sinceMs + timeoutMs;
    let nextCursor: string | undefined = undefined;
    const seenEvents = new Set<string>();
    const allEvents: StaffXEvent[] = [];

    while (Date.now() < deadline) {
      const page = await this.api.queryEvents({
        aggregateType: "assistant-run",
        aggregateId: run.runId,
        since: nextCursor,
        limit: 50,
      });

      for (const event of page.items) {
        if (seenEvents.has(event.id)) continue;
        seenEvents.add(event.id);
        allEvents.push(event);
      }
      if (allEvents.length > 0) {
        const latest = allEvents[allEvents.length - 1];
        if (latest) {
          nextCursor = encodeEventCursor(latest);
        }
      }
      if (page.items.length > 0) {
        nextCursor = page.nextCursor ?? nextCursor;
      }

      const currentRun = await this.api.getRun(this.actor, run.runId);
      if (currentRun.status === "success" || currentRun.status === "failed" || currentRun.status === "cancelled") {
        return {
          run: currentRun,
          events: allEvents,
        };
      }

      if (page.nextCursor) {
        continue;
      }

      await sleep(pollMs);
    }

    try {
      const currentRun = await this.api.getRun(this.actor, run.runId);
      return { run: currentRun, events: allEvents };
    } catch (error) {
      if (isSimErrorLike(error)) {
        throw new Error(`run ${run.runId} did not reach terminal state within timeout`);
      }
      throw error;
    }
  }

  async runFullWorkflow(input: FullWorkflowInput): Promise<FullWorkflowResult> {
    const project = await this.ensureProject(input.project);
    const thread = await this.createThread(project.id, input.threadTitle, input.threadDescription);
    const beforeMatrix = await this.api.getMatrix(this.actor, thread.id);
    let afterMatrix: MatrixSnapshot | null = null;
    if (input.matrixPatch && input.matrixPatch.length > 0) {
      afterMatrix = await this.mutateMatrixLayout(thread.id, input.matrixPatch);
    }

    if (input.chatContent) {
      await this.sendChat(thread.id, input.chatContent);
    }

    const { run, events } = await this.startRunAndWait(thread.id, {
      assistantType: input.run?.assistantType ?? "direct",
      prompt: input.run?.prompt,
      model: input.run?.model,
      timeoutMs: input.waitMs,
      pollMs: input.eventPollMs,
    });

    const messages = await this.api.listMessages(this.actor, thread.id);

    return {
      project,
      thread,
      beforeMatrix,
      afterMatrix,
      run,
      messages,
      events,
    };
  }
}
