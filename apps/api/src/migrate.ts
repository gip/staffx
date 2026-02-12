import { readdir, readFile } from "node:fs/promises";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { query, close } from "./db.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const migrationsDir = join(__dirname, "migrations");

async function migrate() {
  // Drop everything — dev only, no migration tracking
  await query(`
    drop materialized view if exists matrix_view cascade;
    drop view if exists artifact_files_view cascade;
    drop view if exists node_overview cascade;
    drop view if exists user_projects cascade;
    drop view if exists thread_timeline cascade;
    drop view if exists project_summary cascade;

    drop table if exists changes cascade;
    drop table if exists messages cascade;
    drop table if exists actions cascade;
    drop table if exists threads cascade;
    drop table if exists project_thread_counters cascade;
    drop table if exists project_collaborators cascade;
    drop table if exists projects cascade;
    drop table if exists artifact_files cascade;
    drop table if exists file_contents cascade;
    drop table if exists artifacts cascade;
    drop table if exists matrix_refs cascade;
    drop table if exists documents cascade;
    drop table if exists edges cascade;
    drop table if exists nodes cascade;
    drop table if exists concerns cascade;
    drop table if exists systems cascade;
    drop table if exists users cascade;

    drop function if exists set_updated_at cascade;
    drop function if exists upsert_file_content cascade;
    drop function if exists validate_system_root_node cascade;
    drop function if exists fork_system cascade;
    drop function if exists thread_current_system cascade;
    drop function if exists next_project_thread_id cascade;
    drop function if exists assign_project_thread_id cascade;
    drop function if exists create_thread cascade;
    drop function if exists clone_thread cascade;
    drop function if exists begin_action cascade;
    drop function if exists commit_action_empty cascade;
    drop function if exists close_thread cascade;
    drop function if exists diff_artifact_files cascade;
    drop function if exists diff_thread cascade;

    drop type if exists node_kind cascade;
    drop type if exists edge_type cascade;
    drop type if exists doc_kind cascade;
    drop type if exists ref_type cascade;
    drop type if exists artifact_type cascade;
    drop type if exists collaborator_role cascade;
    drop type if exists action_type cascade;
    drop type if exists message_role cascade;
    drop type if exists change_operation cascade;
  `);

  const files = (await readdir(migrationsDir)).filter((f) => f.endsWith(".sql")).sort();

  for (const file of files) {
    const sql = await readFile(join(migrationsDir, file), "utf-8");
    console.log(`Running ${file}…`);
    await query(sql);
  }

  console.log("Schema reset complete.");
  await close();
}

migrate().catch((err) => {
  console.error(err);
  process.exit(1);
});
