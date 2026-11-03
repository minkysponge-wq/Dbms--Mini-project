import { verifyCredentials, createSession, currentUser } from "@/lib/auth";

export const dynamic = "force-dynamic";

export async function POST(request) {
  const { username, password } = await request.json().catch(() => ({}));
  if (!username || !password) {
    return Response.json(
      { error: "username and password are required" },
      { status: 400 }
    );
  }

  const user = await verifyCredentials(username, password);
  if (!user) {
    // Same message for an unknown user and a wrong password, so the
    // endpoint cannot be used to enumerate accounts.
    return Response.json({ error: "invalid credentials" }, { status: 401 });
  }

  await createSession(user);
  const full = await currentUser();
  return Response.json({ ok: true, user: full });
}
