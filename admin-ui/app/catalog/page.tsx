"use client";

import { useCallback, useEffect, useState } from "react";
import { useSession } from "next-auth/react";
import type { CatalogEntry, ConnectorItem } from "@/lib/api";

export default function CatalogPage() {
  const { data: session } = useSession();
  const [catalog, setCatalog] = useState<CatalogEntry[]>([]);
  const [installed, setInstalled] = useState<ConnectorItem[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);

  const fetchData = useCallback(async () => {
    try {
      const [catRes, connRes] = await Promise.all([
        fetch("/api/gateway/catalog"),
        fetch("/api/gateway/connectors"),
      ]);
      if (!catRes.ok) throw new Error(`catalog: ${catRes.status}`);
      if (!connRes.ok) throw new Error(`connectors: ${connRes.status}`);
      setCatalog(await catRes.json());
      setInstalled(await connRes.json());
      setError(null);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
    const id = setInterval(fetchData, 15_000);
    return () => clearInterval(id);
  }, [fetchData]);

  const isOperator = session?.realmRoles?.includes("gateway-operator") ?? false;

  const installedMap = new Map(installed.map((c) => [c.id, c]));

  const doInstall = async (name: string) => {
    setBusy(name);
    setActionError(null);
    try {
      const res = await fetch(
        `/api/gateway/connectors/${encodeURIComponent(name)}/install`,
        { method: "POST" }
      );
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `${res.status}`);
      }
      await fetchData();
    } catch (e) {
      setActionError(String(e));
    } finally {
      setBusy(null);
    }
  };

  const doUpdate = async (name: string) => {
    setBusy(`update:${name}`);
    setActionError(null);
    try {
      const res = await fetch(
        `/api/gateway/connectors/${encodeURIComponent(name)}/update`,
        { method: "POST" }
      );
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `${res.status}`);
      }
      await fetchData();
    } catch (e) {
      setActionError(String(e));
    } finally {
      setBusy(null);
    }
  };

  if (loading) return <p>Loading…</p>;

  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "1.25rem" }}>
        <h1 style={{ fontSize: "1.25rem", fontWeight: 700, margin: 0 }}>Connector Catalog</h1>
        {!isOperator && (
          <span style={{ fontSize: "0.8rem", color: "#6b7280", background: "#f3f4f6", padding: "0.2rem 0.6rem", borderRadius: "999px" }}>
            viewer — install disabled
          </span>
        )}
      </div>
      {error && <p style={{ color: "#dc2626" }}>Failed to load: {error}</p>}
      {actionError && <p style={{ color: "#dc2626", marginBottom: "0.5rem" }}>Error: {actionError}</p>}
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.9rem" }}>
        <thead>
          <tr style={{ borderBottom: "2px solid #e5e7eb" }}>
            {["Connector", "Version", "Digest", "Signature", "Status", "Action"].map((h) => (
              <th key={h} style={{ textAlign: "left", padding: "0.5rem 0.75rem", whiteSpace: "nowrap" }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {catalog.length === 0 ? (
            <tr>
              <td colSpan={6} style={{ padding: "1rem", color: "#9ca3af", textAlign: "center" }}>
                Catalog is empty or not configured
              </td>
            </tr>
          ) : catalog.map((entry) => {
            const conn = installedMap.get(entry.name);
            const installedDigest = conn ? digestFromRef(conn.image) : null;
            const catalogDigest = entry.digest;
            const updateAvailable = !!conn && !!installedDigest && installedDigest !== catalogDigest;
            const isBusy = busy === entry.name || busy === `update:${entry.name}`;

            return (
              <tr key={entry.name} style={{ borderBottom: "1px solid #f3f4f6" }}>
                <td style={{ padding: "0.5rem 0.75rem", fontWeight: 600 }}>{entry.name}</td>
                <td style={{ padding: "0.5rem 0.75rem", color: "#374151" }}>{entry.version}</td>
                <td style={{ padding: "0.5rem 0.75rem", fontFamily: "monospace", fontSize: "0.75rem", color: "#6b7280" }}>
                  <span title={catalogDigest}>{catalogDigest.slice(7, 19)}…</span>
                </td>
                <td style={{ padding: "0.5rem 0.75rem" }}>
                  {entry.signature_required ? (
                    <span style={{ color: "#7c3aed", fontWeight: 600, fontSize: "0.8rem" }}>✓ required</span>
                  ) : (
                    <span style={{ color: "#9ca3af", fontSize: "0.8rem" }}>optional</span>
                  )}
                </td>
                <td style={{ padding: "0.5rem 0.75rem" }}>
                  {conn ? (
                    <span>
                      <span style={{ color: conn.running ? "#16a34a" : "#dc2626", fontWeight: 600 }}>
                        {conn.running ? "Running" : "Stopped"}
                      </span>
                      {updateAvailable && (
                        <span style={{ marginLeft: "0.5rem", fontSize: "0.75rem", color: "#d97706", background: "#fef3c7", padding: "0.1rem 0.4rem", borderRadius: "999px" }}>
                          update available
                        </span>
                      )}
                    </span>
                  ) : (
                    <span style={{ color: "#9ca3af" }}>not installed</span>
                  )}
                </td>
                <td style={{ padding: "0.5rem 0.75rem" }}>
                  {isOperator && (
                    <span style={{ display: "flex", gap: "0.4rem" }}>
                      {!conn && (
                        <ActionBtn
                          label={isBusy && busy === entry.name ? "Installing…" : "Install"}
                          disabled={isBusy}
                          onClick={() => doInstall(entry.name)}
                          variant="primary"
                        />
                      )}
                      {conn && updateAvailable && (
                        <ActionBtn
                          label={isBusy ? "Updating…" : "Update"}
                          disabled={isBusy}
                          onClick={() => doUpdate(entry.name)}
                          variant="primary"
                        />
                      )}
                    </span>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function digestFromRef(ref: string): string | null {
  const idx = ref.indexOf("@");
  return idx >= 0 ? ref.slice(idx + 1) : null;
}

function ActionBtn({
  label, disabled, onClick, variant,
}: {
  label: string; disabled: boolean; onClick: () => void; variant?: "primary" | "default";
}) {
  return (
    <button
      disabled={disabled}
      onClick={onClick}
      style={{
        padding: "0.2rem 0.6rem",
        fontSize: "0.8rem",
        cursor: disabled ? "not-allowed" : "pointer",
        opacity: disabled ? 0.5 : 1,
        border: variant === "primary" ? "1px solid #2563eb" : "1px solid #d1d5db",
        borderRadius: "0.25rem",
        background: variant === "primary" ? "#2563eb" : "#fff",
        color: variant === "primary" ? "#fff" : "#111",
      }}
    >
      {label}
    </button>
  );
}
