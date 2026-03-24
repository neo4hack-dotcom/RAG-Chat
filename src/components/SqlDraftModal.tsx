import React, { useEffect, useMemo, useRef, useState } from "react";
import { ArrowDownAZ, ArrowUpAZ, Database, Download, GripVertical, Play, X } from "lucide-react";

export type SqlDraftEngine = "clickhouse" | "oracle";

export type SqlDraftPreviewResult = {
  engine: SqlDraftEngine;
  executedSql: string;
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
  shownRows: number;
  rowLimit: number;
};

type SqlDraftModalProps = {
  isOpen: boolean;
  artifactTitle: string;
  sql: string;
  engine: SqlDraftEngine;
  rowLimit: number;
  isLoading: boolean;
  error: string;
  result: SqlDraftPreviewResult | null;
  onClose: () => void;
  onSqlChange: (value: string) => void;
  onEngineChange: (value: SqlDraftEngine) => void;
  onRowLimitChange: (value: number) => void;
  onRun: () => void;
};

type SortState = {
  column: string;
  direction: "asc" | "desc";
} | null;

function compareGridValues(left: unknown, right: unknown): number {
  if (left == null && right == null) return 0;
  if (left == null) return 1;
  if (right == null) return -1;

  if (typeof left === "number" && typeof right === "number") {
    return left - right;
  }

  const leftDate = Date.parse(String(left));
  const rightDate = Date.parse(String(right));
  if (!Number.isNaN(leftDate) && !Number.isNaN(rightDate)) {
    return leftDate - rightDate;
  }

  return String(left).localeCompare(String(right), undefined, { numeric: true, sensitivity: "base" });
}

function escapeCsvCell(value: unknown): string {
  const text = value == null ? "" : String(value);
  if (/[",\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

export function SqlDraftModal({
  isOpen,
  artifactTitle,
  sql,
  engine,
  rowLimit,
  isLoading,
  error,
  result,
  onClose,
  onSqlChange,
  onEngineChange,
  onRowLimitChange,
  onRun,
}: SqlDraftModalProps) {
  const [columnOrder, setColumnOrder] = useState<string[]>([]);
  const [sortState, setSortState] = useState<SortState>(null);
  const draggedColumnRef = useRef<string | null>(null);

  useEffect(() => {
    if (!result) {
      setColumnOrder([]);
      setSortState(null);
      return;
    }
    setColumnOrder((prev) => {
      const filtered = prev.filter((column) => result.columns.includes(column));
      const missing = result.columns.filter((column) => !filtered.includes(column));
      return [...filtered, ...missing];
    });
  }, [result]);

  const orderedColumns = useMemo(() => {
    if (!result) return [];
    const filtered = columnOrder.filter((column) => result.columns.includes(column));
    const missing = result.columns.filter((column) => !filtered.includes(column));
    return [...filtered, ...missing];
  }, [columnOrder, result]);

  const visibleRows = useMemo(() => {
    if (!result) return [];
    const rows = [...result.rows];
    if (!sortState) return rows;
    return rows.sort((left, right) => {
      const compared = compareGridValues(left[sortState.column], right[sortState.column]);
      return sortState.direction === "asc" ? compared : -compared;
    });
  }, [result, sortState]);

  const toggleSort = (column: string) => {
    setSortState((prev) => {
      if (!prev || prev.column !== column) return { column, direction: "asc" };
      if (prev.direction === "asc") return { column, direction: "desc" };
      return null;
    });
  };

  const moveColumn = (from: string, to: string) => {
    if (!result || from === to) return;
    setColumnOrder((prev) => {
      const base = prev.length > 0 ? [...prev] : [...result.columns];
      const fromIndex = base.indexOf(from);
      const toIndex = base.indexOf(to);
      if (fromIndex === -1 || toIndex === -1) return base;
      const next = [...base];
      const [moved] = next.splice(fromIndex, 1);
      next.splice(toIndex, 0, moved);
      return next;
    });
  };

  const exportVisibleRows = () => {
    if (!result || orderedColumns.length === 0) return;
    const lines = [
      orderedColumns.map(escapeCsvCell).join(","),
      ...visibleRows.map((row) => orderedColumns.map((column) => escapeCsvCell(row[column])).join(",")),
    ];
    const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${artifactTitle.toLowerCase().replace(/[^a-z0-9]+/gi, "-").replace(/^-|-$/g, "") || "sql-preview"}.csv`;
    anchor.click();
    URL.revokeObjectURL(url);
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/22 px-6 py-8 backdrop-blur-sm">
      <div className="relative flex h-[min(88vh,920px)] w-[min(95vw,1340px)] flex-col overflow-hidden rounded-[2rem] border border-white/70 bg-white/88 shadow-[0_30px_120px_rgba(15,23,42,0.22)] backdrop-blur-2xl dark:border-white/10 dark:bg-slate-950/88">
        <div className="flex items-start justify-between gap-4 border-b border-slate-200/70 px-6 py-5 dark:border-white/10">
          <div className="min-w-0">
            <div className="text-[11px] font-semibold uppercase tracking-[0.22em] text-slate-500 dark:text-slate-400">
              SQL Draft Preview
            </div>
            <div className="mt-1 flex items-center gap-2 text-xl font-semibold text-slate-900 dark:text-white">
              <Database className="h-5 w-5 text-cyan-500" />
              <span className="truncate">{artifactTitle}</span>
            </div>
            <div className="mt-1 text-sm text-slate-500 dark:text-slate-400">
              Review the SQL, adjust it if needed, then generate an interactive result table.
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="inline-flex h-11 w-11 items-center justify-center rounded-full bg-slate-100 text-slate-500 transition-colors hover:bg-slate-200 hover:text-slate-700 dark:bg-white/10 dark:text-slate-300 dark:hover:bg-white/15 dark:hover:text-white"
            title="Close SQL preview"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="grid min-h-0 flex-1 gap-0 lg:grid-cols-[420px_minmax(0,1fr)]">
          <div className="flex min-h-0 flex-col border-b border-slate-200/70 p-5 dark:border-white/10 lg:border-b-0 lg:border-r">
            <div className="grid gap-3">
              <div>
                <div className="mb-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500 dark:text-slate-400">
                  Data source
                </div>
                <div className="flex flex-wrap gap-2">
                  {(["clickhouse", "oracle"] as SqlDraftEngine[]).map((option) => (
                    <button
                      key={option}
                      type="button"
                      onClick={() => onEngineChange(option)}
                      className={`inline-flex items-center justify-center rounded-full px-4 py-2 text-xs font-semibold transition-colors ${
                        engine === option
                          ? option === "oracle"
                            ? "bg-orange-500 text-white shadow-md shadow-orange-500/25"
                            : "bg-cyan-500 text-white shadow-md shadow-cyan-500/25"
                          : "border border-slate-200/80 bg-white text-slate-600 hover:bg-slate-50 dark:border-white/10 dark:bg-white/5 dark:text-slate-300 dark:hover:bg-white/10"
                      }`}
                    >
                      {option === "oracle" ? "Oracle SQL" : "ClickHouse SQL"}
                    </button>
                  ))}
                </div>
              </div>

              <div>
                <div className="mb-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500 dark:text-slate-400">
                  Fetch size
                </div>
                <div className="flex flex-wrap gap-2">
                  {[1000, 10000].map((value) => (
                    <button
                      key={value}
                      type="button"
                      onClick={() => onRowLimitChange(value)}
                      className={`inline-flex items-center justify-center rounded-full px-4 py-2 text-xs font-semibold transition-colors ${
                        rowLimit === value
                          ? "bg-slate-900 text-white shadow-md shadow-slate-900/20 dark:bg-white dark:text-slate-950"
                          : "border border-slate-200/80 bg-white text-slate-600 hover:bg-slate-50 dark:border-white/10 dark:bg-white/5 dark:text-slate-300 dark:hover:bg-white/10"
                      }`}
                    >
                      {value.toLocaleString()} rows
                    </button>
                  ))}
                </div>
              </div>

              <div>
                <div className="mb-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500 dark:text-slate-400">
                  SQL
                </div>
                <textarea
                  value={sql}
                  onChange={(event) => onSqlChange(event.target.value)}
                  spellCheck={false}
                  className="min-h-[280px] w-full rounded-[1.4rem] border border-slate-200/80 bg-slate-50/90 px-4 py-3 font-mono text-[12px] leading-6 text-slate-800 outline-none transition-colors focus:border-cyan-300 focus:bg-white dark:border-white/10 dark:bg-slate-900/70 dark:text-slate-100 dark:focus:border-cyan-600"
                />
              </div>
            </div>

            <div className="mt-4 flex flex-wrap items-center gap-3">
              <button
                type="button"
                onClick={onRun}
                disabled={isLoading || !sql.trim()}
                className="inline-flex items-center gap-2 rounded-full bg-cyan-500 px-4 py-2.5 text-sm font-semibold text-white shadow-md shadow-cyan-500/25 transition-colors hover:bg-cyan-400 disabled:cursor-not-allowed disabled:bg-cyan-300"
              >
                {isLoading ? <span className="h-4 w-4 animate-spin rounded-full border-2 border-white/70 border-t-transparent" /> : <Play className="h-4 w-4" />}
                {isLoading ? "Running..." : "Generate table"}
              </button>
              <button
                type="button"
                onClick={exportVisibleRows}
                disabled={!result || visibleRows.length === 0}
                className="inline-flex items-center gap-2 rounded-full border border-slate-200/80 bg-white px-4 py-2.5 text-sm font-semibold text-slate-700 transition-colors hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-white/10 dark:bg-white/5 dark:text-slate-200 dark:hover:bg-white/10"
              >
                <Download className="h-4 w-4" />
                Export CSV
              </button>
            </div>

            {error ? (
              <div className="mt-4 rounded-[1.3rem] border border-rose-200 bg-rose-50/90 px-4 py-3 text-sm text-rose-700 dark:border-rose-500/30 dark:bg-rose-950/25 dark:text-rose-200">
                {error}
              </div>
            ) : null}
          </div>

          <div className="flex min-h-0 flex-col p-5">
            <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
              <div>
                <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500 dark:text-slate-400">
                  Result grid
                </div>
                <div className="mt-1 text-sm text-slate-600 dark:text-slate-300">
                  {result
                    ? `Showing ${result.shownRows.toLocaleString()} row(s) with up to ${result.rowLimit.toLocaleString()} fetched rows.`
                    : "Run the SQL to inspect rows, sort columns, and reorder the grid."}
                </div>
              </div>
              {result ? (
                <div className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-600 dark:bg-white/10 dark:text-slate-200">
                  {result.engine === "oracle" ? "Oracle SQL" : "ClickHouse SQL"}
                </div>
              ) : null}
            </div>

            <div className="min-h-0 flex-1 overflow-hidden rounded-[1.5rem] border border-slate-200/80 bg-white/90 shadow-inner dark:border-white/10 dark:bg-slate-950/45">
              {!result ? (
                <div className="flex h-full items-center justify-center px-6 text-center text-sm text-slate-500 dark:text-slate-400">
                  The preview table will appear here after you run the draft.
                </div>
              ) : (
                <div className="h-full overflow-auto">
                  <table className="min-w-full border-separate border-spacing-0 text-sm">
                    <thead className="sticky top-0 z-10 bg-slate-50/95 backdrop-blur dark:bg-slate-950/95">
                      <tr>
                        {orderedColumns.map((column) => {
                          const isSorted = sortState?.column === column;
                          return (
                            <th
                              key={column}
                              draggable
                              onDragStart={() => {
                                draggedColumnRef.current = column;
                              }}
                              onDragOver={(event) => {
                                event.preventDefault();
                              }}
                              onDrop={() => {
                                if (draggedColumnRef.current) {
                                  moveColumn(draggedColumnRef.current, column);
                                }
                                draggedColumnRef.current = null;
                              }}
                              className="border-b border-slate-200/80 px-4 py-3 text-left text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500 dark:border-white/10 dark:text-slate-400"
                            >
                              <button
                                type="button"
                                onClick={() => toggleSort(column)}
                                className="flex items-center gap-2 text-left transition-colors hover:text-slate-800 dark:hover:text-white"
                              >
                                <GripVertical className="h-3.5 w-3.5 text-slate-300 dark:text-slate-600" />
                                <span>{column}</span>
                                {isSorted ? (
                                  sortState?.direction === "asc" ? <ArrowDownAZ className="h-3.5 w-3.5" /> : <ArrowUpAZ className="h-3.5 w-3.5" />
                                ) : null}
                              </button>
                            </th>
                          );
                        })}
                      </tr>
                    </thead>
                    <tbody>
                      {visibleRows.map((row, rowIndex) => (
                        <tr key={rowIndex} className="odd:bg-white even:bg-slate-50/55 dark:odd:bg-transparent dark:even:bg-white/[0.03]">
                          {orderedColumns.map((column) => (
                            <td
                              key={`${rowIndex}-${column}`}
                              className="max-w-[20rem] border-b border-slate-100 px-4 py-2.5 align-top text-slate-700 dark:border-white/5 dark:text-slate-200"
                            >
                              <div className="whitespace-pre-wrap break-words text-[13px] leading-5">
                                {row[column] == null ? <span className="text-slate-300 dark:text-slate-600">—</span> : String(row[column])}
                              </div>
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
