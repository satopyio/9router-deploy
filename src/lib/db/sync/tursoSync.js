import { createClient } from "@libsql/client";

const TURSO_URL = process.env.TURSO_DB_URL;
const TURSO_TOKEN = process.env.TURSO_DB_TOKEN;

let _client = null;
let _writeQueue = [];
let _flushTimer = null;
let _fullSyncTimer = null;

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

export async function restoreFromTurso(adapter) {
  const client = getClient();
  if (!client) return;

  try {
    const tablesRes = await client.execute(
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '\\_%' ESCAPE '\\' AND name != 'sqlite_sequence'"
    );
    const tableNames = tablesRes.rows.map(r => r.name).filter(Boolean);

    if (tableNames.length === 0) {
      console.log("[Turso] remote DB is empty, nothing to restore");
      return;
    }

    let totalRows = 0;
    for (const table of tableNames) {
      const colsRes = await client.execute(`PRAGMA table_info("${table}")`);
      const colNames = colsRes.rows.map(c => c.name);
      if (colNames.length === 0) continue;

      const dataRes = await client.execute(`SELECT * FROM "${table}"`);
      if (dataRes.rows.length === 0) continue;

      for (const row of dataRes.rows) {
        const placeholders = colNames.map(() => "?").join(",");
        const values = colNames.map(c => row[c]);
        try {
          adapter.run(
            `INSERT INTO "${table}"(${colNames.map(c => `"${c}"`).join(",")}) VALUES(${placeholders})`,
            values
          );
          totalRows++;
        } catch (e) {
          if (!e.message.includes("UNIQUE constraint")) {
            console.warn(`[Turso] restore insert failed for ${table}: ${e.message}`);
          }
        }
      }
    }

    console.log(`[Turso] restored ${totalRows} rows across ${tableNames.length} tables`);

    await syncFull(adapter);
  } catch (e) {
    console.warn(`[Turso] restore failed: ${e.message}`);
  }
}

function isWriteSQL(sql) {
  const trimmed = sql.trim().toUpperCase();
  if (trimmed.startsWith("SELECT") || trimmed.startsWith("PRAGMA")) return false;
  return true;
}

export function syncWrite(sql, params = []) {
  const client = getClient();
  if (!client || !isWriteSQL(sql)) return;

  _writeQueue.push({ sql, args: params });
  scheduleFlush(client);
}

export function syncExec(sql) {
  const client = getClient();
  if (!client) return;

  const statements = sql.split(";").filter(s => {
    const t = s.trim();
    return t && isWriteSQL(t);
  });

  for (const stmt of statements) {
    const trimmed = stmt.trim();
    _writeQueue.push({ sql: trimmed, args: [] });
  }
  scheduleFlush(client);
}

function scheduleFlush(client) {
  if (_flushTimer) return;
  _flushTimer = setTimeout(() => {
    _flushTimer = null;
    flushQueue(client);
  }, 2000);
}

async function flushQueue(client) {
  if (_writeQueue.length === 0) return;
  const batch = _writeQueue.splice(0, _writeQueue.length);

  try {
    const requests = [];
    for (const item of batch) {
      if (item.sql && item.sql.trim()) {
        requests.push({ type: "execute", stmt: { sql: item.sql, args: item.args } });
      }
    }
    if (requests.length === 0) return;
    await client.batch(requests, "write");
  } catch (e) {
    _writeQueue.unshift(...batch);
    if (!e.message?.includes("no such table") && !e.message?.includes("UNIQUE")) {
      console.warn(`[Turso] batch flush failed (${batch.length} stmts): ${e.message}`);
    }
  }
}

export async function syncFull(adapter) {
  const client = getClient();
  if (!client) return;

  try {
    await flushQueue(client);

    const tablesRes = adapter.all(
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '\\_%' ESCAPE '\\' AND name != 'sqlite_sequence'"
    );

    for (const { name } of tablesRes) {
      const rows = adapter.all(`SELECT * FROM "${name}"`);
      if (rows.length === 0) continue;

      await client.execute(`DELETE FROM "${name}"`);

      const colsRes = await client.execute(`PRAGMA table_info("${name}")`);
      const colNames = colsRes.rows.map(c => c.name);
      if (colNames.length === 0) continue;

      for (const row of rows) {
        const placeholders = colNames.map(() => "?").join(",");
        const values = colNames.map(c => row[c]);
        try {
          await client.execute({
            sql: `INSERT INTO "${name}"(${colNames.map(c => `"${c}"`).join(",")}) VALUES(${placeholders})`,
            args: values,
          });
        } catch (e) {
          if (!e.message?.includes("UNIQUE")) {
            console.warn(`[Turso] full-sync insert failed for ${name}: ${e.message}`);
          }
        }
      }
    }
  } catch (e) {
    console.warn(`[Turso] full-sync error: ${e.message}`);
  }
}

export function startPeriodicSync(adapter) {
  if (!isTursoConfigured()) return;

  if (_fullSyncTimer) clearInterval(_fullSyncTimer);
  _fullSyncTimer = setInterval(async () => {
    try {
      await syncFull(adapter);
    } catch {}
  }, 120000);

  if (typeof _fullSyncTimer.unref === "function") _fullSyncTimer.unref();
}

export function stopPeriodicSync() {
  if (_fullSyncTimer) {
    clearInterval(_fullSyncTimer);
    _fullSyncTimer = null;
  }
}

export function startFlushOnShutdown(adapter) {
  const onShutdown = async () => {
    console.log("[Turso] flushing writes before shutdown...");
    const client = getClient();
    if (client) {
      await flushQueue(client);
      await syncFull(adapter);
    }
    closeTurso();
  };

  process.once("beforeExit", onShutdown);
  process.once("SIGTERM", () => {
    onShutdown().finally(() => process.exit(0));
  });
  process.once("SIGINT", () => {
    onShutdown().finally(() => process.exit(0));
  });
}

function closeTurso() {
  if (_flushTimer) {
    clearTimeout(_flushTimer);
    _flushTimer = null;
  }
  stopPeriodicSync();
  if (_client) {
    try { _client.close(); } catch {}
    _client = null;
  }
}

export { flushQueue, closeTurso };