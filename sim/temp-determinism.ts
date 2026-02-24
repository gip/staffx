import { SimApi } from './api';
import { SimFrontendClient } from './frontend';
import { SimDesktop } from './desktop';

async function inspect(api: SimApi, prefix: string) {
  const rows = await api.query<{ id: string; name: string }>(`SELECT id, name FROM projects WHERE name LIKE $1`, [`${prefix}%`]);
  return rows.rows;
}

async function collect(api: SimApi, runId: string) {
  const summary = await api.runArtifactsForRun(runId);
  if (!summary.matrixSystemId) return [] as string[];
  const artifacts = await api.getArtifactsForSystem(summary.matrixSystemId);
  return artifacts.map((artifact) => `${artifact.id}:${artifact.text}`);
}

const api = new SimApi({ databaseUrl: 'pgmem://local' });
const actor = await SimFrontendClient.bootstrapUser(api, 'owner', null);
const frontend = new SimFrontendClient(api, actor);
const desktop = new SimDesktop(api, { actor, pollIntervalMs: 50 });
desktop.start();

const prefix = 'staffx_sim_determinism';
await api.cleanupSimProjects(prefix, `${prefix}%`, true);
console.log('before', await inspect(api, prefix));

const input = {
  project: {
    name: prefix,
    description: 'StaffX determinism check.',
    visibility: 'private' as const,
  },
  threadTitle: 'Determinism thread',
  chatContent: 'Please generate deterministic output.',
  run: {
    assistantType: 'direct' as const,
    prompt: 'Deterministic run prompt.',
  },
};

const first = await frontend.runFullWorkflow(input);
console.log('after first', await inspect(api, prefix));
console.log('first artifacts', await collect(api, first.run.runId));
await api.cleanupSimProjects(prefix, `${prefix}%`, true);
console.log('after cleanup', await inspect(api, prefix));
const second = await frontend.runFullWorkflow(input);
console.log('after second', await inspect(api, prefix));
console.log('second artifacts', await collect(api, second.run.runId));

await desktop.stop();
await api.close();
