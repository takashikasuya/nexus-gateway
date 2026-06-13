"use client";

import { useCallback, useMemo, useState } from "react";
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  useReactTable,
} from "@tanstack/react-table";
import type { ConnectorItem } from "@/lib/api";

const col = createColumnHelper<ConnectorItem>();

type Props = {
  data: ConnectorItem[];
  isOperator: boolean;
  onRefresh: () => void;
};

export function ConnectorTable({ data, isOperator, onRefresh }: Props) {
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const doAction = useCallback(
    async (id: string, action: string, image?: string) => {
      setBusy(`${id}:${action}`);
      setError(null);
      try {
        const url = image
          ? `/api/gateway/connectors/${encodeURIComponent(id)}/${action}?image=${encodeURIComponent(image)}`
          : `/api/gateway/connectors/${encodeURIComponent(id)}/${action}`;
        const res = await fetch(url, { method: "POST" });
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
        onRefresh();
      } catch (e) {
        setError(String(e));
      } finally {
        setBusy(null);
      }
    },
    [onRefresh]
  );

  const columns = useMemo(() => [
    col.accessor("id", { header: "ID" }),
    col.accessor("image", {
      header: "Image / Version",
      cell: (info) => {
        const img = info.getValue();
        if (!img) return "—";
        const [, tag] = img.split(":");
        return (
          <span title={img}>
            {img.length > 40 ? `…${img.slice(-37)}` : img}
            {tag && <span style={{ marginLeft: "0.25rem", color: "#6b7280", fontSize: "0.8em" }}>{tag}</span>}
          </span>
        );
      },
    }),
    col.accessor("running", {
      header: "Status",
      cell: (info) => (
        <span style={{ color: info.getValue() ? "#16a34a" : "#dc2626", fontWeight: 600 }}>
          {info.getValue() ? "Running" : "Stopped"}
        </span>
      ),
    }),
    col.display({
      id: "actions",
      header: "Actions",
      cell: (info) => {
        const id = info.row.original.id;
        const running = info.row.original.running;
        const isBusy = busy?.startsWith(`${id}:`);

        if (!isOperator) {
          return <span style={{ color: "#9ca3af", fontSize: "0.875rem" }}>viewer</span>;
        }
        return (
          <span style={{ display: "flex", gap: "0.5rem" }}>
            {running ? (
              <>
                <ActionBtn label="Stop" disabled={!!isBusy} onClick={() => doAction(id, "stop")} />
                <ActionBtn label="Restart" disabled={!!isBusy} onClick={() => doAction(id, "restart")} />
              </>
            ) : (
              <ActionBtn label="Start" disabled={!!isBusy} onClick={() => doAction(id, "start")} />
            )}
            <ActionBtn
              label="Upgrade"
              disabled={!!isBusy}
              onClick={() => {
                const image = window.prompt("New image reference:", info.row.original.image);
                if (image) doAction(id, "upgrade", image);
              }}
            />
          </span>
        );
      },
    }),
  // eslint-disable-next-line react-hooks/exhaustive-deps
  ], [busy, isOperator, doAction]);

  const table = useReactTable({ data, columns, getCoreRowModel: getCoreRowModel() });

  return (
    <div>
      {error && <p style={{ color: "#dc2626", marginBottom: "0.5rem" }}>Error: {error}</p>}
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.9rem" }}>
        <thead>
          {table.getHeaderGroups().map((hg) => (
            <tr key={hg.id} style={{ borderBottom: "2px solid #e5e7eb" }}>
              {hg.headers.map((h) => (
                <th key={h.id} style={{ textAlign: "left", padding: "0.5rem 0.75rem", whiteSpace: "nowrap" }}>
                  {flexRender(h.column.columnDef.header, h.getContext())}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {table.getRowModel().rows.length === 0 ? (
            <tr>
              <td colSpan={columns.length} style={{ padding: "1rem", color: "#9ca3af", textAlign: "center" }}>
                No connectors registered
              </td>
            </tr>
          ) : (
            table.getRowModel().rows.map((row) => (
              <tr key={row.id} style={{ borderBottom: "1px solid #f3f4f6" }}>
                {row.getVisibleCells().map((cell) => (
                  <td key={cell.id} style={{ padding: "0.5rem 0.75rem" }}>
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}

function ActionBtn({ label, disabled, onClick }: { label: string; disabled: boolean; onClick: () => void }) {
  return (
    <button
      disabled={disabled}
      onClick={onClick}
      style={{
        padding: "0.2rem 0.6rem",
        fontSize: "0.8rem",
        cursor: disabled ? "not-allowed" : "pointer",
        opacity: disabled ? 0.5 : 1,
        border: "1px solid #d1d5db",
        borderRadius: "0.25rem",
        background: "#fff",
      }}
    >
      {label}
    </button>
  );
}
