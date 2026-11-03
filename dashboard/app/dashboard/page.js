import { redirect } from "next/navigation";
import { currentUser } from "@/lib/auth";
import DashboardClient from "./DashboardClient";

export const dynamic = "force-dynamic";

/**
 * Server component: resolves the session before anything renders, so an
 * unauthenticated visitor never receives the dashboard markup at all.
 * The user's permissions come from the database (the recursive
 * v_user_permission view) and are handed to the client component, which
 * uses them only to decide what to *show* - every action is checked
 * again server-side in the API route.
 */
export default async function DashboardPage() {
  const user = await currentUser();
  if (!user) redirect("/login");
  return <DashboardClient user={user} />;
}
