"use client";

import { signOut, useSession } from "next-auth/react";
import Link from "next/link";
import { usePathname } from "next/navigation";

export function Nav() {
  const { data: session } = useSession();
  const path = usePathname();

  return (
    <nav style={{ display: "flex", alignItems: "center", gap: "1rem", padding: "0.75rem 1.5rem", borderBottom: "1px solid #e5e7eb", background: "#fff" }}>
      <span style={{ fontWeight: 700, marginRight: "1rem" }}>nexus-gateway</span>
      <Link href="/dashboard" style={{ fontWeight: path === "/dashboard" ? 700 : 400 }}>Dashboard</Link>
      <Link href="/connectors" style={{ fontWeight: path === "/connectors" ? 700 : 400 }}>Connectors</Link>
      <span style={{ marginLeft: "auto", fontSize: "0.875rem", color: "#6b7280" }}>{session?.user?.email}</span>
      <button onClick={() => signOut()} style={{ cursor: "pointer" }}>Logout</button>
    </nav>
  );
}
