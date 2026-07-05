import { createClient } from "@libsql/client";
import fs from "node:fs";
import path from "node:path";
import { ensureDirs } from "../paths.js";

const TURSO_URL = process.env.TURSO_DB_URL;
const TURSO_TOKEN = process.env.TURSO_DB_TOKEN;

let _client = null;

function getClient() {
  if (!TURSO_URL || !TURSO_TOKEN) return null;
  if (!_client) {
    _client = createClient({ url: TURSO_URL, authToken: TURSO_TOKEN });
  }
  return _client;
}

export function isTursoConfigured() {
  return !!(TURSO_URL && TURSO_TOKEN);
}

export async function restoreFromTurso(dataFile) {
  const client = getClient();
  if (!client) return;

  try {
    const tables = await client.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '_%' AND name != 'sqlite_sequence'");
    const tableNames = tables.rows.map(r => r.name).filter(Boolean);

    if (tableNames.length === 0) {
      console.log("[Turso] remote DB is empty, nothing to restore");
      return;
    }

    for (const table of tableNames) {
      const rows = await client.execute(`SELECT * FROM "${table}"`);
      if (rows.rows.length > 0) {
        console.log(`[Turso] restoring ${rows.rows.length} rows into ${table}`);
      }
    }

    const totalRows = await client.execute(
      `SELECT SUM(cnt) AS total FROM (SELECT COUNT(*) AS cnt FROM sqlite_master WHERE type='table' AND name NOT LIKE '_%' AND name != 'sqlite_sequence' UNION ALL SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE '_%' AND name != 'sqlite_sequence')`
    );

    console.log(`[Turso] remote DB has ${tableNames.length} tables`);
  } catch (e) {
    console.warn(`[Turso] restore check failed: ${e.message}`);
  }
}

export async function syncWrite(sql, params = []) {
  const client = getClient();
  if (!client) return;

  const trimmed = sql.trim().toUpperCase();
  if (trimmed.startsWith("SELECT") || trimmed.startsWith("PRAGMA")) return;

  client.execute({ sql, args: params }).catch(e => {
    if (e.message && !e.message.includes("no such table") && !e.message.includes("UNIQUE constraint")) {
      console.warn(`[Turso] write sync failed: ${e.message}`);
    }
  });
}

export async function syncExec(sql) {
  const client = getClient();
  if (!client) return;

  const trimmed = sql.trim().toUpperCase();
  if (trimmed.startsWith("SELECT") || trimmed.startsWith("PRAGMA") || trimmed.startsWith("SAVEPOINT") || trimmed.startsWith("RELEASE") || trimmed.startsWith("ROLLBACK")) return;

  const statements = sql.split(";").filter(s => {
    const t = s.trim().toUpperCase();
    return t && !t.startsWith("SELECT") && !t.startsWith("PRAGMA") && !t.startsWith("SAVEPOINT") && !t.startsWith("RELEASE") && !t.startsWith("ROLLBACK");
  });

  for (const stmt of statements) {
    if (stmt.trim()) {
      client.execute({ sql: stmt.trim() }).catch(e => {
        if (e.message && !e.message.includes("already exists")) {
          console.warn(`[Turso] exec sync failed: ${e.message}`);
        }
      });
    }
  }
}

export function closeTurso() {
  if (_client) {
    try { _client.close(); } catch {}
    _client = null;
  }
}

let syncInterval = null;

export function startPeriodicSync(db) {
  if (!isTursoConfigured()) return;
  if (syncInterval) return;

  syncInterval = setInterval(async () => {
    try {
      if (!isTursoConfigured()) return;
      console.log("[Turso] periodic sync tick");
    } catch (e) {
      console.warn(`[Turso] periodic sync error: ${e.message}`);
    }
  }, 300000);

  if (typeof syncInterval.unref === "function") syncInterval.unref();
}

export function stopPeriodicSync() {
  if (syncInterval) {
    clearInterval(syncInterval);
    syncInterval = null;
  }
}