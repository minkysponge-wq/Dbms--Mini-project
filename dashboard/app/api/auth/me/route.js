import { currentUser } from "@/lib/auth";

export const dynamic = "force-dynamic";

export async function GET() {
  const user = await currentUser();
  if (!user) {
    return Response.json({ error: "not signed in" }, { status: 401 });
  }
  return Response.json({ user });
}
