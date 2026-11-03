/**
 * Database access for the dashboard's API routes.
 *
 * The routes talk to MySQL and MongoDB directly for everything they
 * read - cluster status, the key-value store, access control, the event
 * stream - and proxy only *control* actions (write a key through
 * consensus, crash a node, partition the network) to the Python
 * cluster server, because those need the live cluster rather than the
 * database.
 *
 * Pools are cached on globalThis so Next.js's dev-mode hot reload does
 * not open a new pool on every edit.
 */

import mysql from "mysql2/promise";
import { MongoClient } from "mongodb";

const MYSQL_CONFIG = {
  host: process.env.RAFTKV_DB_HOST || "127.0.0.1",
  port: Number(process.env.RAFTKV_DB_PORT || 3306),
  user: process.env.RAFTKV_DB_USER || "root",
  password: process.env.RAFTKV_DB_PASSWORD || "",
  database: process.env.RAFTKV_DB_NAME || "raftkv",
  waitForConnections: true,
  connectionLimit: 10,
  // Needed so CALL on a stored procedure can return its OUT parameters.
  multipleStatements: false,
};

if (process.env.RAFTKV_DB_SOCKET) {
  MYSQL_CONFIG.socketPath = process.env.RAFTKV_DB_SOCKET;
}

export const CLUSTER_API =
  process.env.RAFTKV_CLUSTER_API || "http://127.0.0.1:8080";

const MONGO_URI = process.env.RAFTKV_MONGO_URI || "mongodb://127.0.0.1:27017";
const MONGO_DB = process.env.RAFTKV_MONGO_DB || "raftkv_events";

export function pool() {
  if (!globalThis.__raftkvPool) {
    globalThis.__raftkvPool = mysql.createPool(MYSQL_CONFIG);
  }
  return globalThis.__raftkvPool;
}

/** Run a SELECT and return the rows. */
export async function query(sql, params = []) {
  const [rows] = await pool().execute(sql, params);
  return rows;
}

/** Run a SELECT and return the first row, or null. */
export async function queryOne(sql, params = []) {
  const rows = await query(sql, params);
  return rows.length ? rows[0] : null;
}

/** Run an INSERT, UPDATE or DELETE. */
export async function execute(sql, params = []) {
  const [result] = await pool().execute(sql, params);
  return result;
}

/**
 * Run a block inside a transaction, on one connection.
 * Commits on success, rolls back on any throw.
 */
export async function transaction(fn) {
  const conn = await pool().getConnection();
  try {
    await conn.beginTransaction();
    const result = await fn(conn);
    await conn.commit();
    return result;
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}

/** The MongoDB event collection, connected lazily. */
export async function eventsCollection() {
  if (!globalThis.__raftkvMongo) {
    const client = new MongoClient(MONGO_URI, {
      serverSelectionTimeoutMS: 2000,
    });
    globalThis.__raftkvMongo = client.connect();
  }
  const client = await globalThis.__raftkvMongo;
  return client.db(MONGO_DB).collection("cluster_events");
}

/** Is MongoDB reachable right now? */
export async function mongoAvailable() {
  try {
    const coll = await eventsCollection();
    await coll.estimatedDocumentCount();
    return true;
  } catch {
    return false;
  }
}

/** Call the Python cluster server. */
export async function clusterApi(path, options = {}) {
  const res = await fetch(`${CLUSTER_API}${path}`, {
    ...options,
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    cache: "no-store",
  });
  const text = await res.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { error: text };
  }
  return { ok: res.ok, status: res.status, body };
}
