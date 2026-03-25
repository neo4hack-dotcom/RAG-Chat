import React from "react";
import { createPortal } from "react-dom";
import { BrainCircuit, Database, Loader2, Play, RefreshCw, Sparkles, Target, X } from "lucide-react";
import { cn } from "../lib/utils";

export type GuideSchemaColumn = {
  name: string;
  type: string;
  category: "numeric" | "string" | "date" | "other";
};

type AgentGuideMode = "feature_engineer" | "auto_ml";

type AgentGuideModalProps = {
  isOpen: boolean;
  mode: AgentGuideMode;
  isBusy: boolean;
  isLoadingMetadata: boolean;
  error: string | null;
  tables: string[];
  schema: GuideSchemaColumn[];
  selectedTable: string;
  targetColumn?: string;
  targetCandidates?: string[];
  goalText: string;
  notesText: string;
  onClose: () => void;
  onRefreshMetadata: () => void;
  onTableChange: (table: string) => void;
  onTargetColumnChange?: (value: string) => void;
  onGoalTextChange: (value: string) => void;
  onNotesTextChange: (value: string) => void;
  onSubmit: () => void;
  onStop: () => void;
};

const FEATURE_GOAL_PRESETS = [
  "Predict sales more accurately",
  "Detect churn risk earlier",
  "Rank high-value customers",
  "Forecast operational incidents",
];

const AUTOML_GOAL_PRESETS = [
  "Find the strongest baseline model",
  "Predict a business KPI",
  "Score a binary target quickly",
  "Benchmark models before deeper feature work",
];

function categoryTone(category: GuideSchemaColumn["category"]) {
  if (category === "numeric") return "border-cyan-200 bg-cyan-50 text-cyan-700 dark:border-cyan-800/70 dark:bg-cyan-950/30 dark:text-cyan-200";
  if (category === "date") return "border-violet-200 bg-violet-50 text-violet-700 dark:border-violet-800/70 dark:bg-violet-950/30 dark:text-violet-200";
  if (category === "string") return "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-800/70 dark:bg-emerald-950/30 dark:text-emerald-200";
  return "border-slate-200 bg-slate-50 text-slate-700 dark:border-slate-700 dark:bg-slate-900/40 dark:text-slate-200";
}

export function AgentGuideModal({
  isOpen,
  mode,
  isBusy,
  isLoadingMetadata,
  error,
  tables,
  schema,
  selectedTable,
  targetColumn = "",
  targetCandidates = [],
  goalText,
  notesText,
  onClose,
  onRefreshMetadata,
  onTableChange,
  onTargetColumnChange,
  onGoalTextChange,
  onNotesTextChange,
  onSubmit,
  onStop,
}: AgentGuideModalProps) {
  if (!isOpen || typeof document === "undefined") return null;

  const isFeatureGuide = mode === "feature_engineer";
  const title = isFeatureGuide ? "Feature Engineer guide" : "Auto-ML guide";
  const subtitle = isFeatureGuide
    ? "Pick a table, frame the business objective, and launch a more focused feature-design pass."
    : "Pick the training table, choose the prediction target, and benchmark models with the right business framing.";
  const presets = isFeatureGuide ? FEATURE_GOAL_PRESETS : AUTOML_GOAL_PRESETS;
  const canLaunch = isFeatureGuide ? Boolean(selectedTable) : Boolean(selectedTable && targetColumn);

  return createPortal(
    <div className="fixed inset-0 z-[130] flex items-center justify-center bg-black/20 px-4 py-6 backdrop-blur-sm">
      <div
        className="relative flex max-h-[92vh] w-full max-w-6xl flex-col overflow-hidden rounded-[2rem] border border-white/70 bg-white/88 shadow-[0_28px_90px_rgba(15,23,42,0.22)] backdrop-blur-2xl dark:border-white/10 dark:bg-[#0f1117]/90"
        role="dialog"
        aria-modal="true"
        aria-label={title}
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-4 border-b border-black/5 px-6 py-5 dark:border-white/10">
          <div className="min-w-0">
            <div className="text-[10px] font-semibold uppercase tracking-[0.22em] text-gray-500 dark:text-gray-400">
              Guided setup
            </div>
            <div className="mt-2 flex items-center gap-3">
              <div
                className={cn(
                  "inline-flex h-11 w-11 items-center justify-center rounded-2xl border",
                  isFeatureGuide
                    ? "border-lime-200 bg-lime-50 text-lime-700 dark:border-lime-800/70 dark:bg-lime-950/30 dark:text-lime-200"
                    : "border-rose-200 bg-rose-50 text-rose-700 dark:border-rose-800/70 dark:bg-rose-950/30 dark:text-rose-200"
                )}
              >
                {isFeatureGuide ? <Sparkles className="h-5 w-5" /> : <BrainCircuit className="h-5 w-5" />}
              </div>
              <div className="min-w-0">
                <h2 className="truncate text-xl font-semibold text-gray-950 dark:text-white">{title}</h2>
                <p className="mt-1 max-w-3xl text-sm text-gray-600 dark:text-gray-300">{subtitle}</p>
              </div>
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="inline-flex h-10 w-10 items-center justify-center rounded-full bg-black/5 text-gray-600 transition-colors hover:bg-black/10 dark:bg-white/10 dark:text-gray-300 dark:hover:bg-white/15"
            title="Close guide"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="grid min-h-0 flex-1 grid-cols-1 gap-0 lg:grid-cols-[1.5fr_1fr]">
          <div className="min-h-0 space-y-5 overflow-y-auto px-6 py-5">
            <section className="rounded-[1.7rem] border border-white/70 bg-white/82 p-4 shadow-sm dark:border-white/10 dark:bg-white/5">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">Step 1</div>
                  <h3 className="mt-1 text-sm font-semibold text-gray-950 dark:text-white">Choose the source table</h3>
                  <p className="mt-1 text-xs text-gray-500 dark:text-gray-400">Start with the dataset that best matches the business problem you want to solve.</p>
                </div>
                <button
                  type="button"
                  onClick={onRefreshMetadata}
                  disabled={isLoadingMetadata}
                  className="inline-flex items-center gap-2 rounded-full border border-gray-200 bg-white px-3 py-2 text-xs font-medium text-gray-700 transition-colors hover:bg-gray-50 disabled:opacity-60 dark:border-white/10 dark:bg-white/5 dark:text-gray-200 dark:hover:bg-white/10"
                >
                  {isLoadingMetadata ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
                  Refresh
                </button>
              </div>

              <div className="mt-4 flex flex-wrap gap-2.5">
                {tables.map((table) => (
                  <button
                    key={table}
                    type="button"
                    onClick={() => onTableChange(table)}
                    className={cn(
                      "inline-flex items-center gap-2 rounded-2xl border px-3.5 py-2.5 text-sm font-medium transition-all",
                      selectedTable === table
                        ? "border-cyan-300 bg-cyan-500 text-white shadow-md shadow-cyan-500/20"
                        : "border-cyan-200/80 bg-cyan-50/80 text-cyan-800 hover:bg-cyan-100/80 dark:border-cyan-800/70 dark:bg-cyan-950/20 dark:text-cyan-200 dark:hover:bg-cyan-950/35"
                    )}
                  >
                    <Database className="h-4 w-4" />
                    {table}
                  </button>
                ))}
              </div>
            </section>

            {!isFeatureGuide && (
              <section className="rounded-[1.7rem] border border-white/70 bg-white/82 p-4 shadow-sm dark:border-white/10 dark:bg-white/5">
                <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">Step 2</div>
                <h3 className="mt-1 text-sm font-semibold text-gray-950 dark:text-white">Choose the prediction target</h3>
                <p className="mt-1 text-xs text-gray-500 dark:text-gray-400">Pick the column you ultimately want the model to predict. The benchmark will treat the rest as candidate features.</p>
                <div className="mt-4 flex flex-wrap gap-2.5">
                  {targetCandidates.map((candidate) => (
                    <button
                      key={candidate}
                      type="button"
                      onClick={() => onTargetColumnChange?.(candidate)}
                      className={cn(
                        "inline-flex items-center gap-2 rounded-2xl border px-3.5 py-2.5 text-sm font-medium transition-all",
                        targetColumn === candidate
                          ? "border-rose-300 bg-rose-500 text-white shadow-md shadow-rose-500/20"
                          : "border-rose-200/80 bg-rose-50/80 text-rose-800 hover:bg-rose-100/80 dark:border-rose-800/70 dark:bg-rose-950/20 dark:text-rose-200 dark:hover:bg-rose-950/35"
                      )}
                    >
                      <Target className="h-4 w-4" />
                      {candidate}
                    </button>
                  ))}
                </div>
                {targetCandidates.length === 0 && (
                  <div className="mt-4 rounded-2xl border border-dashed border-gray-200 px-4 py-3 text-xs text-gray-500 dark:border-white/10 dark:text-gray-400">
                    Choose a table first to load candidate target columns.
                  </div>
                )}
              </section>
            )}

            <section className="rounded-[1.7rem] border border-white/70 bg-white/82 p-4 shadow-sm dark:border-white/10 dark:bg-white/5">
              <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">
                {isFeatureGuide ? "Step 2" : "Step 3"}
              </div>
              <h3 className="mt-1 text-sm font-semibold text-gray-950 dark:text-white">
                {isFeatureGuide ? "Frame the feature objective" : "Frame the benchmark objective"}
              </h3>
              <p className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                The more concrete the objective, the better the agent can focus the analysis and produce a usable recommendation.
              </p>
              <div className="mt-4 flex flex-wrap gap-2">
                {presets.map((preset) => (
                  <button
                    key={preset}
                    type="button"
                    onClick={() => onGoalTextChange(preset)}
                    className="inline-flex items-center rounded-full border border-gray-200 bg-white px-3 py-1.5 text-xs font-medium text-gray-700 transition-colors hover:bg-gray-50 dark:border-white/10 dark:bg-white/5 dark:text-gray-200 dark:hover:bg-white/10"
                  >
                    {preset}
                  </button>
                ))}
              </div>
              <div className="mt-4 grid gap-3">
                <textarea
                  value={goalText}
                  onChange={(event) => onGoalTextChange(event.target.value)}
                  rows={3}
                  className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition focus:border-blue-400 focus:ring-4 focus:ring-blue-100 dark:border-white/10 dark:bg-white/5 dark:text-gray-100 dark:focus:border-blue-500 dark:focus:ring-blue-900/30"
                  placeholder={isFeatureGuide ? "Example: help forecast weekly sales and explain seasonality drivers." : "Example: benchmark models to predict customer churn with a strong baseline F1-score."}
                />
                <textarea
                  value={notesText}
                  onChange={(event) => onNotesTextChange(event.target.value)}
                  rows={3}
                  className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition focus:border-blue-400 focus:ring-4 focus:ring-blue-100 dark:border-white/10 dark:bg-white/5 dark:text-gray-100 dark:focus:border-blue-500 dark:focus:ring-blue-900/30"
                  placeholder={isFeatureGuide ? "Optional notes: known leakage risks, preferred business angle, time granularity..." : "Optional notes: preferred business metric, class imbalance concern, baseline expectations..."}
                />
              </div>
            </section>

            <section className="rounded-[1.7rem] border border-white/70 bg-gradient-to-br from-white via-white to-slate-50 p-4 shadow-sm dark:border-white/10 dark:bg-gradient-to-br dark:from-white/5 dark:via-white/[0.03] dark:to-slate-950/40">
              <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">
                {isFeatureGuide ? "Step 3" : "Step 4"}
              </div>
              <h3 className="mt-1 text-sm font-semibold text-gray-950 dark:text-white">Review and launch</h3>
              <div className="mt-4 grid gap-3 sm:grid-cols-2">
                <div className="rounded-2xl border border-white/70 bg-white/85 px-4 py-3 dark:border-white/10 dark:bg-white/5">
                  <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-gray-500 dark:text-gray-400">Table</div>
                  <div className="mt-1 text-sm font-semibold text-gray-950 dark:text-white">{selectedTable || "Not selected yet"}</div>
                </div>
                {!isFeatureGuide && (
                  <div className="rounded-2xl border border-white/70 bg-white/85 px-4 py-3 dark:border-white/10 dark:bg-white/5">
                    <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-gray-500 dark:text-gray-400">Target</div>
                    <div className="mt-1 text-sm font-semibold text-gray-950 dark:text-white">{targetColumn || "Not selected yet"}</div>
                  </div>
                )}
              </div>
              {error && (
                <div className="mt-4 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700 dark:border-rose-800/70 dark:bg-rose-950/20 dark:text-rose-200">
                  {error}
                </div>
              )}
              <div className="mt-4 flex flex-wrap items-center gap-3">
                <button
                  type="button"
                  onClick={isBusy ? onStop : onSubmit}
                  disabled={!canLaunch}
                  className={cn(
                    "inline-flex items-center gap-2 rounded-full px-4 py-2.5 text-sm font-semibold text-white transition-colors disabled:cursor-not-allowed disabled:opacity-50",
                    isBusy
                      ? "bg-rose-500 hover:bg-rose-400"
                      : isFeatureGuide
                        ? "bg-lime-500 hover:bg-lime-400"
                        : "bg-rose-500 hover:bg-rose-400"
                  )}
                >
                  {isBusy ? <X className="h-4 w-4" /> : <Play className="h-4 w-4" />}
                  {isBusy ? "Stop" : "Launch"}
                </button>
                <div className="text-xs text-gray-500 dark:text-gray-400">
                  {isFeatureGuide
                    ? "The result will still appear in the main chat with suggested engineered features and reusable SQL."
                    : "The benchmark result will appear in the main chat with a comparison table and a recommended baseline model."}
                </div>
              </div>
            </section>
          </div>

          <aside className="min-h-0 border-t border-black/5 bg-white/65 px-6 py-5 dark:border-white/10 dark:bg-white/[0.03] lg:border-l lg:border-t-0">
            <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">Schema preview</div>
            <div className="mt-2 text-sm font-semibold text-gray-950 dark:text-white">
              {selectedTable ? `${selectedTable} columns` : "Select a table to inspect its columns"}
            </div>
            <p className="mt-1 text-xs text-gray-500 dark:text-gray-400">
              This live preview helps the user choose the right table and, for Auto-ML, the right prediction target.
            </p>

            <div className="mt-4 max-h-[52vh] space-y-2 overflow-y-auto pr-1">
              {schema.length === 0 ? (
                <div className="rounded-2xl border border-dashed border-gray-200 px-4 py-4 text-sm text-gray-500 dark:border-white/10 dark:text-gray-400">
                  Choose a table to load the schema and unlock the guided setup.
                </div>
              ) : (
                schema.map((column) => (
                  <div
                    key={column.name}
                    className={cn("rounded-2xl border px-3.5 py-3", categoryTone(column.category))}
                  >
                    <div className="text-sm font-semibold">{column.name}</div>
                    <div className="mt-1 text-xs opacity-80">{column.type}</div>
                  </div>
                ))
              )}
            </div>
          </aside>
        </div>
      </div>
    </div>,
    document.body
  );
}
