import { createClient } from "@libsql/client";

const TURSO_DB_URL = process.env.TURSO_DB_URL;
const TURSO_DB_TOKEN = process.env.TURSO_DB_TOKEN;

export function createTursoAdapter(filePath) {
  const url = TURSO_DB_URL;
  const authToken = TURSO_DB_TOKEN;

  const db = createClient({ url, authToken });

  const stmtCache = new Map();

  function prepare(sql) {
    let stmt = stmtCache.get(sql);
    if (!stmt) {
      stmt = db.prepare(sql);
      stmtCache.set(sql, stmt);
    }
    return stmt;
  }

  return {
    driver: `turso (${url.replace(/https?:\/\//, "").split(".")[0]})`,
    async run(sql, params = []) {
      const stmt = prepare(sql);
      const result = await stmt.run(params);
      return result;
    },
    async get(sql, params = []) {
      const stmt = prepare(sql);
      const result = await stmt.get(params);
      return result;
    },
    async all(sql, params = []) {
      const stmt = prepare(sql);
      const result = await stmt.all(params);
      return result;
    },
    async exec(sql) {
      const stmt = prepare(sql);
      await stmt.run();
    },
    async transaction(fn) {
      const result = await db.transaction(async (tx) => {
        const txAdapter = {
          driver: "turso-tx",
          run: (sql, params) => tx.execute({ sql, args: params }),
          get: (sql, params) => tx.execute({ sql, args: params }).then(r => r.rows[0] || null),
          all: (sql, params) => tx.execute({ sql, args: params }).then(r => r.rows),
          exec: (sql) => tx.execute({ sql }),
        };
        return fn(txAdapter);
      });
      return result;
    },
    async close() {
      stmtCache.clear();
      db.close();
    },
    raw: db,
  };
}