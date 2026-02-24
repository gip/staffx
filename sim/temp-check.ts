import { SimApi } from './api';
import { SimFrontendClient } from './frontend';
import { SimDesktop } from './desktop';

const api = new SimApi({ databaseUrl: 'pgmem://local' });
const owner = await SimFrontendClient.bootstrapUser(api, 'check-owner', null);
const frontend = new SimFrontendClient(api, owner);

const project = await frontend.ensureProject({
  name: 'staffx_sim_positive',
  description: 'StaffX simulation positive path.',
  visibility: 'private',
});
const threads = await api.listThreads(owner, { projectId: project.id });
console.log('threads from createProject', threads.items.map((t) => ({ id: t.id, seed: t.projectThreadId })), 'projectThreadId for project seed?', project.threadId);

const thread = await api.createThread(owner, { projectId: project.id, title: 'Positive thread' });
console.log('created thread', thread.id, 'seed', thread.projectThreadId);

const threadRows = await api.query<{ id: string; seed_system_id: string; status: string; project_thread_id: number }>(
  `SELECT id, seed_system_id, status, project_thread_id FROM threads WHERE id IN ($1, $2) ORDER BY project_thread_id`,
  [project.threadId, thread.id],
);
console.log('thread rows', threadRows.rows);

const sourceSystem = await api.getThreadSystemId(thread.id);
console.log('resolved source system', sourceSystem);

const desktop = new SimDesktop(api, { actor: owner, pollIntervalMs: 200 });
desktop.start();
await frontend.sendChat(thread.id, 'Run deterministic direct execution.');
await frontend.mutateMatrixLayout(thread.id, [{ nodeId: (await api.getMatrix(owner, thread.id)).topology.nodes[0]?.id ?? 'root', x: 1, y: 2 }]);
const result = await frontend.startRunAndWait(thread.id, {
  assistantType: 'direct',
  timeoutMs: 20000,
  pollMs: 100,
});
console.log('result status', result.run.status, result.run.runError);
await desktop.stop();
await api.close();
