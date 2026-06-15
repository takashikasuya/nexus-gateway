/** Thin wrapper for Admin API calls forwarded from Next.js server routes. */

const BASE = process.env.ADMIN_API_URL ?? "http://localhost:8080";

export type GatewayHealth = {
  UptimeSeconds: number;
  GoRoutines: number;
  MemAllocMB: number;
  DiskUsedMB: number;
  DiskTotalMB: number;
  Connectors: ConnectorHealth[] | null;
};

export type ConnectorHealth = {
  ID: string;
  Image: string;
  Running: boolean;
};

export type ConnectorItem = {
  id: string;
  image: string;
  prev_image?: string;
  container_id?: string;
  running: boolean;
};

export type CatalogEntry = {
  name: string;
  version: string;
  image: string;
  digest: string;
  min_gateway_version: string;
  signature_required: boolean;
  network?: string[];
  mounts?: string[];
};

async function adminFetch(path: string, token: string | undefined, init?: RequestInit) {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (token) headers["Authorization"] = `Bearer ${token}`;
  const res = await fetch(`${BASE}${path}`, { ...init, headers });
  if (!res.ok) throw new Error(`Admin API ${path}: ${res.status} ${res.statusText}`);
  return res;
}

export async function getHealth(token?: string): Promise<GatewayHealth> {
  const res = await adminFetch("/health", token);
  return res.json();
}

export async function listConnectors(token: string): Promise<ConnectorItem[]> {
  const res = await adminFetch("/connectors", token);
  return res.json();
}

export async function listCatalog(token: string): Promise<CatalogEntry[]> {
  const res = await adminFetch("/catalog", token);
  return res.json();
}

export async function connectorAction(
  token: string,
  id: string,
  action: string,
  image?: string
): Promise<void> {
  const url = image
    ? `/connectors/${encodeURIComponent(id)}/${encodeURIComponent(action)}?image=${encodeURIComponent(image)}`
    : `/connectors/${encodeURIComponent(id)}/${encodeURIComponent(action)}`;
  await adminFetch(url, token, { method: "POST" });
}
