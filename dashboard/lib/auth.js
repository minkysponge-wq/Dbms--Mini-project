/**
 * Authentication and role-based access control.
 *
 * Two layers, deliberately:
 *
 *   1. The session itself - a signed JWT in an httpOnly cookie, so the
 *      browser cannot read or forge it.
 *   2. The permission check - resolved by the *database*, through the
 *      recursive view v_user_permission, not by a hardcoded list here.
 *      Granting a role a new permission in SQL changes what the app
 *      allows without touching this file.
 *
 * Passwords are bcrypt hashes; the plaintext never leaves the login
 * route.
 */

import { SignJWT, jwtVerify } from "jose";
import bcrypt from "bcryptjs";
import { cookies } from "next/headers";
import { query, queryOne, execute } from "./db";
import crypto from "crypto";

const COOKIE_NAME = "raftkv_session";
const SESSION_HOURS = 8;

function secret() {
  const value =
    process.env.RAFTKV_JWT_SECRET ||
    "dev-only-secret-change-me-in-production-0123456789";
  return new TextEncoder().encode(value);
}

/* ------------------------------------------------------------------ */
/* Login                                                               */
/* ------------------------------------------------------------------ */

/**
 * Verify a username and password against app_user.
 * Returns the user record, or null when the credentials are wrong.
 *
 * The same null is returned for an unknown user and a bad password, so
 * the response cannot be used to enumerate accounts.
 */
export async function verifyCredentials(username, password) {
  const user = await queryOne(
    `SELECT user_id, username, email, password_hash, is_active
       FROM app_user WHERE username = ?`,
    [username]
  );
  if (!user || !user.is_active) return null;

  const ok = await bcrypt.compare(password, user.password_hash);
  if (!ok) return null;

  await execute("UPDATE app_user SET last_login_at = NOW(6) WHERE user_id = ?", [
    user.user_id,
  ]);
  return {
    user_id: user.user_id,
    username: user.username,
    email: user.email,
  };
}

/** Sign a session token and record it in auth_session. */
export async function createSession(user) {
  const expires = new Date(Date.now() + SESSION_HOURS * 3600 * 1000);
  const token = await new SignJWT({
    sub: String(user.user_id),
    username: user.username,
  })
    .setProtectedHeader({ alg: "HS256" })
    .setIssuedAt()
    .setExpirationTime(Math.floor(expires.getTime() / 1000))
    .sign(secret());

  // Only the hash is stored, so a leaked database row cannot be
  // replayed as a session.
  const tokenHash = crypto.createHash("sha256").update(token).digest("hex");
  await execute(
    `INSERT INTO auth_session (token_hash, user_id, expires_at)
     VALUES (?, ?, ?)`,
    [tokenHash, user.user_id, expires]
  );

  cookies().set(COOKIE_NAME, token, {
    httpOnly: true,
    sameSite: "lax",
    path: "/",
    expires,
  });
  return token;
}

export async function destroySession() {
  const token = cookies().get(COOKIE_NAME)?.value;
  if (token) {
    const tokenHash = crypto.createHash("sha256").update(token).digest("hex");
    await execute("UPDATE auth_session SET revoked = TRUE WHERE token_hash = ?", [
      tokenHash,
    ]);
  }
  cookies().delete(COOKIE_NAME);
}

/* ------------------------------------------------------------------ */
/* Reading the current session                                         */
/* ------------------------------------------------------------------ */

/**
 * The signed-in user, with the roles and permissions the database says
 * they have. Returns null when there is no valid session.
 */
export async function currentUser() {
  const token = cookies().get(COOKIE_NAME)?.value;
  if (!token) return null;

  let payload;
  try {
    ({ payload } = await jwtVerify(token, secret()));
  } catch {
    return null;
  }

  // A valid signature is not enough: the session must also still be
  // live in the database, so a logout or an admin revoke takes effect
  // immediately rather than when the token happens to expire.
  const tokenHash = crypto.createHash("sha256").update(token).digest("hex");
  const session = await queryOne(
    `SELECT user_id FROM auth_session
      WHERE token_hash = ? AND revoked = FALSE AND expires_at > NOW(6)`,
    [tokenHash]
  );
  if (!session) return null;

  const userId = Number(payload.sub);

  // Permissions come from the recursive view, so inheritance through
  // the role hierarchy is applied by the database.
  const rows = await query(
    `SELECT role_name, permission_code FROM v_user_permission WHERE user_id = ?`,
    [userId]
  );
  const user = await queryOne(
    "SELECT user_id, username, email FROM app_user WHERE user_id = ?",
    [userId]
  );
  if (!user) return null;

  return {
    ...user,
    roles: [...new Set(rows.map((r) => r.role_name))],
    permissions: [...new Set(rows.map((r) => r.permission_code))],
  };
}

/* ------------------------------------------------------------------ */
/* Authorisation                                                       */
/* ------------------------------------------------------------------ */

export function can(user, permission) {
  return Boolean(user && user.permissions.includes(permission));
}

/**
 * Guard for an API route. Returns { user } when allowed, or
 * { response } carrying the 401 or 403 to return.
 *
 * Every privileged route calls this; none of them check a role name
 * directly, so adding a role in SQL needs no code change.
 */
export async function requirePermission(permission) {
  const user = await currentUser();
  if (!user) {
    return {
      response: Response.json(
        { error: "authentication required" },
        { status: 401 }
      ),
    };
  }
  if (!can(user, permission)) {
    return {
      response: Response.json(
        {
          error: "forbidden",
          detail: `your roles (${user.roles.join(", ")}) do not grant ${permission}`,
        },
        { status: 403 }
      ),
    };
  }
  return { user };
}
