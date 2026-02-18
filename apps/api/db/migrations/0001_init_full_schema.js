import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

export async function up(pgm) {
  const sql = await readFile(join(__dirname, "0001_init_full_schema.sql"), "utf-8");
  pgm.sql(sql);
}

export async function down() {
  // No-op. Initial full-schema migration is intentionally not reversible in this phase.
}
