import React, { useEffect, useMemo, useState } from "react";
import { BarChart3, CheckCircle2, Database, Filter, Layers3, RefreshCw, Search, X } from "lucide-react";
import { DataQualitySchemaColumn, cn } from "../lib/utils";

type DataQualityModalProps = {
  isOpen: boolean;
  isBusy: boolean;
  isLoadingMetadata: boolean;
  error: string | null;
  tables: string[];
  schema: DataQualitySchemaColumn[];
  selectedTable: string;
  selectedColumns: string[];
  sampleSize: number;
  rowFilter: string;
  timeColumn: string | null;
  onClose: () => void;
  onRefreshMetadata: () => Promise<void>;
  onTableChange: (table: string) => void;
  onColumnsChange: (columns: string[]) => void;
  onSampleSizeChange: (sampleSize: number) => void;
  onRowFilterChange: (rowFilter: string) => void;
  onTimeColumnChange: (timeColumn: string | null) => void;
  onSubmit: () => Promise<void>;
};

const SAMPLE_PRESETS = [
  {
    label: "50k",
    description: "Fast baseline profiling",
    value: 50_000,
  },
  {
    label: "100k",
    description: "More robust sample",
    value: 100_000,
  },
  {
    label: "500k",
    description: "Large quality scan",
    value: 500_000,
  },
  {
    label: "Full scan",
    description: "Capped to 2,000,000 rows",
    value: 0,
  },
] as const;

function categoryTone(category: DataQualitySchemaColumn["category"]) {
  if (category === "numeric") return "bg-cyan-50 text-cyan-700 border-cyan-200";
  if (category === "string") return "bg-emerald-50 text-emerald-700 border-emerald-200";
  if (category === "date") return "bg-amber-50 text-amber-700 border-amber-200";
  return "bg-gray-50 text-gray-600 border-gray-200";
}

function categoryLabel(category: DataQualitySchemaColumn["category"]) {
  if (category === "numeric") return "Numeric";
  if (category === "string") return "Text";
  if (category === "date") return "Date";
  return "Other";
}

export function DataQualityModal({
  isOpen,
  isBusy,
  isLoadingMetadata,
  error,
  tables,
  schema,
  selectedTable,
  selectedColumns,
  sampleSize,
  rowFilter,
  timeColumn,
  onClose,
  onRefreshMetadata,
  onTableChange,
  onColumnsChange,
  onSampleSizeChange,
  onRowFilterChange,
  onTimeColumnChange,
  onSubmit,
}: DataQualityModalProps) {
  const [search, setSearch] = useState("");

  useEffect(() => {
    if (!isOpen) {
      setSearch("");
    }
  }, [isOpen]);

  const filteredSchema = useMemo(() => {
    const normalizedSearch = search.trim().toLowerCase();
    if (!normalizedSearch) return schema;
    return schema.filter((column) => {
      const haystack = `${column.name} ${column.type} ${column.category}`.toLowerCase();
      return haystack.includes(normalizedSearch);
    });
  }, [schema, search]);

  const dateColumns = schema.filter((column) => column.category === "date");
  const selectedPreset = SAMPLE_PRESETS.find((preset) => preset.value === sampleSize)?.value ?? "custom";
  const launchDisabled = isBusy || isLoadingMetadata || !selectedTable || selectedColumns.length === 0;
  const sampleSizeLabel = sampleSize === 0 ? "Full scan (capped to 2,000,000 rows)" : `${sampleSize.toLocaleString()} rows`;

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-[90] bg-black/45 backdrop-blur-sm flex items-center justify-center p-4">
      <div className="w-full max-w-[1180px] max-h-[92vh] overflow-hidden rounded-[2rem] border border-white/20 bg-[#f8f8f6] dark:bg-[#101115] shadow-2xl shadow-black/30">
        <div className="flex items-center justify-between gap-4 px-6 py-5 border-b border-gray-200/70 dark:border-gray-800/80 bg-white/80 dark:bg-black/20">
          <div>
            <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.2em] text-gray-500 dark:text-gray-400">
              <BarChart3 className="w-3.5 h-3.5" />
              Data quality - Tables
            </div>
            <h2 className="mt-1 text-xl font-semibold text-gray-900 dark:text-gray-100">
              Configure a table profiling run
            </h2>
            <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
              Select a table, choose the columns to profile, and launch the report directly from this form.
            </p>
          </div>

          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => void onRefreshMetadata()}
              disabled={isBusy || isLoadingMetadata}
              className="inline-flex items-center gap-2 px-3 py-2 rounded-xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors disabled:opacity-60"
            >
              <RefreshCw className={cn("w-4 h-4", isLoadingMetadata && "animate-spin")} />
              Refresh
            </button>
            <button
              type="button"
              onClick={onClose}
              className="inline-flex items-center justify-center w-10 h-10 rounded-xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
            >
              <X className="w-5 h-5" />
            </button>
          </div>
        </div>

        <div className="overflow-y-auto max-h-[calc(92vh-88px)]">
          <div className="space-y-6 p-6">
            <div className="grid lg:grid-cols-[1.05fr,1.15fr] gap-6">
            <div className="space-y-6">
              {error && (
                <div className="rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
                  {error}
                </div>
              )}

              <div className="rounded-[1.75rem] border border-gray-200/80 dark:border-gray-800/70 bg-white/85 dark:bg-gray-900/60 p-5 shadow-sm space-y-5">
                <div>
                  <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">
                    Scope
                  </h3>
                  <p className="text-sm text-gray-500 dark:text-gray-400">
                    Define the target table, sample size, filter, and optional volumetric analysis.
                  </p>
                </div>

                <label className="block space-y-2">
                  <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
                    Table
                  </span>
                  <select
                    value={selectedTable}
                    onChange={(e) => onTableChange(e.target.value)}
                    className="w-full rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-fuchsia-400"
                  >
                    <option value="">Select a table</option>
                    {tables.map((table) => (
                      <option key={table} value={table}>
                        {table}
                      </option>
                    ))}
                  </select>
                </label>

                <div className="space-y-3">
                  <div className="flex items-center gap-2 text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
                    <Layers3 className="w-3.5 h-3.5" />
                    Sample Size
                  </div>
                  <div className="grid sm:grid-cols-2 gap-3">
                    {SAMPLE_PRESETS.map((preset) => (
                      <button
                        key={preset.label}
                        type="button"
                        onClick={() => onSampleSizeChange(preset.value)}
                        className={cn(
                          "text-left rounded-[1.5rem] border px-4 py-3 transition-colors",
                          selectedPreset === preset.value
                            ? "border-fuchsia-400 bg-fuchsia-50 text-fuchsia-900 dark:bg-fuchsia-900/20 dark:border-fuchsia-500 dark:text-fuchsia-100"
                            : "border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 text-gray-800 dark:text-gray-200 hover:border-fuchsia-300"
                        )}
                      >
                        <div className="font-semibold text-sm">{preset.label}</div>
                        <div className="text-xs text-gray-500 dark:text-gray-400 mt-1">{preset.description}</div>
                      </button>
                    ))}
                    <label
                      className={cn(
                        "rounded-[1.5rem] border px-4 py-3 transition-colors",
                        selectedPreset === "custom"
                          ? "border-fuchsia-400 bg-fuchsia-50 dark:bg-fuchsia-900/20 dark:border-fuchsia-500"
                          : "border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950"
                      )}
                    >
                      <div className="font-semibold text-sm text-gray-900 dark:text-gray-100">Custom</div>
                      <input
                        type="number"
                        min={0}
                        step={1000}
                        value={selectedPreset === "custom" ? sampleSize : ""}
                        onFocus={() => {
                          if (selectedPreset !== "custom") onSampleSizeChange(50_000);
                        }}
                        onChange={(e) => onSampleSizeChange(Math.max(0, Number(e.target.value) || 0))}
                        placeholder="Rows"
                        className="mt-2 w-full rounded-xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 px-3 py-2 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-fuchsia-400"
                      />
                    </label>
                  </div>
                </div>

                <label className="block space-y-2">
                  <div className="flex items-center gap-2 text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
                    <Filter className="w-3.5 h-3.5" />
                    Row Filter
                  </div>
                  <textarea
                    value={rowFilter}
                    onChange={(e) => onRowFilterChange(e.target.value)}
                    rows={4}
                    placeholder="Optional. Example: region = 'FR'"
                    className="w-full rounded-[1.5rem] border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-fuchsia-400 resize-y"
                  />
                  <p className="text-xs text-gray-500 dark:text-gray-400">
                    Leave empty to scan all rows in the chosen sample.
                  </p>
                </label>

                <label className="block space-y-2">
                  <div className="flex items-center gap-2 text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
                    <Database className="w-3.5 h-3.5" />
                    Volumetric Analysis
                  </div>
                  <select
                    value={timeColumn ?? ""}
                    onChange={(e) => onTimeColumnChange(e.target.value || null)}
                    disabled={dateColumns.length === 0}
                    className="w-full rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-fuchsia-400 disabled:opacity-60"
                  >
                    <option value="">No volumetric analysis</option>
                    {dateColumns.map((column) => (
                      <option key={column.name} value={column.name}>
                        {column.name}
                      </option>
                    ))}
                  </select>
                  <p className="text-xs text-gray-500 dark:text-gray-400">
                    Choose a date-like column only if you want volume trend and anomaly checks.
                  </p>
                </label>
              </div>

              <div className="rounded-[1.75rem] border border-fuchsia-200/70 dark:border-fuchsia-700/40 bg-fuchsia-50/70 dark:bg-fuchsia-900/10 p-5">
                <div className="flex items-start gap-3">
                  <CheckCircle2 className="w-5 h-5 text-fuchsia-600 dark:text-fuchsia-300 mt-0.5" />
                  <div className="space-y-1 text-sm text-fuchsia-950 dark:text-fuchsia-100">
                    <p className="font-medium">What the run will produce</p>
                    <p className="text-fuchsia-900/80 dark:text-fuchsia-200/85">
                      A Markdown report with an executive summary, per-column findings, prioritized recommendations, and optional volumetric analysis.
                    </p>
                  </div>
                </div>
              </div>
            </div>

            <div className="space-y-6">
              <div className="rounded-[1.75rem] border border-gray-200/80 dark:border-gray-800/70 bg-white/85 dark:bg-gray-900/60 p-5 shadow-sm">
                <div className="flex items-center justify-between gap-3 mb-4">
                  <div>
                    <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">
                      Columns to Profile
                    </h3>
                    <p className="text-sm text-gray-500 dark:text-gray-400">
                      Select the exact fields that should be scored in the report.
                    </p>
                  </div>
                  <div className="text-xs text-gray-500 dark:text-gray-400">
                    {selectedColumns.length} selected
                  </div>
                </div>

                <div className="flex flex-wrap gap-2 mb-4">
                  <button
                    type="button"
                    onClick={() => onColumnsChange(schema.map((column) => column.name))}
                    className="px-3 py-1.5 rounded-full text-xs font-medium border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 text-gray-700 dark:text-gray-200 hover:border-fuchsia-300"
                  >
                    All
                  </button>
                  <button
                    type="button"
                    onClick={() => onColumnsChange(schema.filter((column) => column.category === "numeric").map((column) => column.name))}
                    className="px-3 py-1.5 rounded-full text-xs font-medium border border-cyan-200 bg-cyan-50 text-cyan-700 hover:border-cyan-300"
                  >
                    Numeric
                  </button>
                  <button
                    type="button"
                    onClick={() => onColumnsChange(schema.filter((column) => column.category === "string").map((column) => column.name))}
                    className="px-3 py-1.5 rounded-full text-xs font-medium border border-emerald-200 bg-emerald-50 text-emerald-700 hover:border-emerald-300"
                  >
                    Text
                  </button>
                  <button
                    type="button"
                    onClick={() => onColumnsChange(schema.filter((column) => column.category === "date").map((column) => column.name))}
                    className="px-3 py-1.5 rounded-full text-xs font-medium border border-amber-200 bg-amber-50 text-amber-700 hover:border-amber-300"
                  >
                    Date
                  </button>
                  <button
                    type="button"
                    onClick={() => onColumnsChange([])}
                    className="px-3 py-1.5 rounded-full text-xs font-medium border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 text-gray-700 dark:text-gray-200 hover:border-red-300"
                  >
                    Clear
                  </button>
                </div>

                <label className="flex items-center gap-2 rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 mb-4">
                  <Search className="w-4 h-4 text-gray-400" />
                  <input
                    type="text"
                    value={search}
                    onChange={(e) => setSearch(e.target.value)}
                    placeholder="Search columns..."
                    className="w-full bg-transparent text-sm text-gray-900 dark:text-gray-100 outline-none"
                  />
                </label>

                <div className="rounded-[1.5rem] border border-gray-200 dark:border-gray-800 overflow-hidden">
                  <div className="max-h-[420px] overflow-y-auto divide-y divide-gray-100 dark:divide-gray-800 bg-white dark:bg-gray-950">
                    {filteredSchema.length === 0 ? (
                      <div className="px-4 py-6 text-sm text-gray-500 dark:text-gray-400">
                        {selectedTable
                          ? "No columns match the current search."
                          : "Select a table to load its schema."}
                      </div>
                    ) : (
                      filteredSchema.map((column) => {
                        const checked = selectedColumns.includes(column.name);
                        return (
                          <label
                            key={column.name}
                            className="flex items-start gap-3 px-4 py-3 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-900/60"
                          >
                            <input
                              type="checkbox"
                              checked={checked}
                              onChange={(e) => {
                                if (e.target.checked) {
                                  onColumnsChange([...selectedColumns, column.name]);
                                } else {
                                  onColumnsChange(selectedColumns.filter((item) => item !== column.name));
                                }
                              }}
                              className="mt-1 rounded border-gray-300 text-fuchsia-600 focus:ring-fuchsia-500"
                            />
                            <div className="min-w-0 flex-1">
                              <div className="flex flex-wrap items-center gap-2">
                                <span className="font-medium text-sm text-gray-900 dark:text-gray-100">
                                  {column.name}
                                </span>
                                <span className={cn("px-2 py-0.5 rounded-full border text-[11px] font-medium", categoryTone(column.category))}>
                                  {categoryLabel(column.category)}
                                </span>
                              </div>
                              <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                                {column.type}
                              </div>
                            </div>
                          </label>
                        );
                      })
                    )}
                  </div>
                </div>
              </div>
            </div>
            </div>

            <div className="rounded-[1.75rem] border border-black/10 dark:border-white/10 bg-white/85 dark:bg-gray-900/60 p-5 shadow-sm">
              <div className="flex flex-col gap-5 lg:flex-row lg:items-center lg:justify-between">
                <div className="space-y-3">
                  <div>
                    <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">
                      Ready to launch
                    </h3>
                    <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                      Review the current scope, then launch the agent directly from the form. The final report will still be posted back into the chat.
                    </p>
                  </div>

                  <div className="flex flex-wrap gap-2 text-xs">
                    <span className="rounded-full border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-950 px-3 py-1.5 text-gray-700 dark:text-gray-200">
                      Table: <strong>{selectedTable || "Not selected"}</strong>
                    </span>
                    <span className="rounded-full border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-950 px-3 py-1.5 text-gray-700 dark:text-gray-200">
                      Columns: <strong>{selectedColumns.length}</strong>
                    </span>
                    <span className="rounded-full border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-950 px-3 py-1.5 text-gray-700 dark:text-gray-200">
                      Sample: <strong>{sampleSizeLabel}</strong>
                    </span>
                    <span className="rounded-full border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-950 px-3 py-1.5 text-gray-700 dark:text-gray-200">
                      Filter: <strong>{rowFilter.trim() ? "Enabled" : "None"}</strong>
                    </span>
                    <span className="rounded-full border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-950 px-3 py-1.5 text-gray-700 dark:text-gray-200">
                      Time column: <strong>{timeColumn || "None"}</strong>
                    </span>
                  </div>
                </div>

                <div className="flex flex-col items-stretch gap-2 lg:min-w-[220px]">
                  <button
                    type="button"
                    onClick={() => void onSubmit()}
                    disabled={launchDisabled}
                    className="inline-flex items-center justify-center gap-2 rounded-[1.35rem] bg-black px-5 py-3 text-sm font-medium text-white transition-colors hover:bg-gray-800 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    <BarChart3 className="w-4 h-4" />
                    {isBusy ? "Launching..." : "Launch"}
                  </button>
                  <p className="text-xs text-gray-500 dark:text-gray-400">
                    Select a table and at least one column to enable the launch action.
                  </p>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className="flex items-center justify-end gap-3 px-6 py-5 border-t border-gray-200/70 dark:border-gray-800/80 bg-white/70 dark:bg-black/10">
          <div className="mr-auto text-xs text-gray-500 dark:text-gray-400">
            The report is generated from the form configuration and posted back into the chat.
          </div>
          <button
            type="button"
            onClick={onClose}
            className="px-4 py-2 rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm font-medium text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}
