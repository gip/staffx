import { readdir, readFile } from "node:fs/promises";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { query, close } from "./db.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const migrationsDir = join(__dirname, "migrations");

async function migrate() {
  // Drop everything and recreate — dev only, no migrations tracking
  await query("DROP TABLE IF EXISTS users CASCADE");
  await query("DROP TABLE IF EXISTS _migrations CASCADE");

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
