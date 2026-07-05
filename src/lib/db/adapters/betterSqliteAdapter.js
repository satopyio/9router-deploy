import Database from "better-sqlite3";
import { PRAGMA_SQL } from "../schema.js";
import {
  syncWrite, syncExec, isTursoConfigured,
  startPeriodicSync, startFlushOnShutdown
} from "../sync/tursoSync.js";

// Periodic checkpoint to keep WAL file small (avoid huge -wal/-shm growth)
const CHECKPOINT_INTERVAL_MS = 60 * 1000;

export function createBetterSqliteAdapter(filePath) {
  const db = new Database(filePath);
  db.exec(PRAGMA_SQL);
  // Schema is created/synced by migrate.js after adapter init

  const stmtCache = new Map();

  function prepare(sql) {
    let stmt = stmtCache.get(sql);
    if (!stmt) {
      stmt = db.prepare(sql);
      stmtCache.set(sql, stmt);
    }
    return stmt;
  }

  // Truncate WAL periodically so file stays small for backup/copy
  const checkpointTimer = setInterval(() => {
    try { db.pragma("wal_checkpoint(TRUNCATE)"); } catch {}
  }, CHECKPOINT_INTERVAL_MS);
  if (typeof checkpointTimer.unref === "function") checkpointTimer.unref();

  function gracefulClose() {
    try { db.pragma("wal_checkpoint(TRUNCATE)"); } catch {}
    try { stmtCache.clear(); } catch {}
    try { db.close(); } catch {}
  }

  // Ensure WAL is flushed and -wal/-shm files removed on shutdown
  const onShutdown = () => gracefulClose();
  process.once("beforeExit", onShutdown);
  process.once("SIGINT", () => { onShutdown(); process.exit(0); });
  process.once("SIGTERM", () => { onShutdown(); process.exit(0); });

  const hasTurso = isTursoConfigured();

  if (hasTurso) {
    startFlushOnShutdown(createTursoSyncAdapter(db));
    startPeriodicSync(createTursoSyncAdapter(db));
  }

  function createTursoSyncAdapter(rawDb) {
    return {
      driver: "sync-adapter",
      run(sql, params = []) { return rawDb.prepare(sql).run(params); },
      get(sql, params = []) { return rawDb.prepare(sql).get(params); },
      all(sql, params = []) { return rawDb.prepare(sql).all(params); },
      exec(sql) { return rawDb.exec(sql); },
      transaction(fn) { return rawDb.transaction(fn)(); },
    };
  }

  return {
    driver: hasTurso ? "better-sqlite3 + turso" : "better-sqlite3",
    run(sql, params = []) {
      const result = prepare(sql).run(params);
      if (hasTurso) syncWrite(sql, params);
      return result;
    },
    get(sql, params = []) { return prepare(sql).get(params); },
    all(sql, params = []) { return prepare(sql).all(params); },
    exec(sql) {
      const result = db.exec(sql);
      if (hasTurso) syncExec(sql);
      return result;
    },
    transaction(fn) { return db.transaction(fn)(); },
    checkpoint() { try { db.pragma("wal_checkpoint(TRUNCATE)"); } catch {} },
    close() {
      clearInterval(checkpointTimer);
      gracefulClose();
    },
    raw: db,
  };
}
