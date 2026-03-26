import React from "react";
import { createPortal } from "react-dom";
import { Activity, CalendarClock, CheckCircle2, RefreshCw, X } from "lucide-react";
import { CrewPlan, CrewPlanRun, PlanningBackendState, cn } from "../lib/utils";

interface McpPlanningMonitorModalProps {
  isOpen: boolean;
  onClose: () => void;
  planningState: PlanningBackendState;
  isBusy: boolean;
  onRefresh: () => Promise<void>;
}

function formatDateLabel(value: string | null | undefined) {
  if (!value) return "—";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function statusTone(status: CrewPlan["status"] | CrewPlan["lastStatus"] | CrewPlanRun["status"] | null | undefined) {
  if (status === "active" || status === "success") return "text-emerald-700 bg-emerald-50 border-emerald-200";
  if (status === "paused") return "text-amber-700 bg-amber-50 border-amber-200";
  if (status === "running") return "text-sky-700 bg-sky-50 border-sky-200";
  if (status === "error") return "text-red-700 bg-red-50 border-red-200";
  return "text-gray-600 bg-gray-50 border-gray-200";
}

export function McpPlanningMonitorModal({
  isOpen,
  onClose,
  planningState,
  isBusy,
  onRefresh,
}: McpPlanningMonitorModalProps) {
  if (!isOpen || typeof document === "undefined") return null;

  const activePlans = planningState.plans.filter((plan) => plan.status === "active");
  const recentRuns = planningState.runs.slice(0, 5);

  return createPortal(
    <>
      <div className="fixed inset-0 z-[112] bg-black/50 backdrop-blur-sm" onClick={onClose} />
      <div className="pointer-events-none fixed inset-0 z-[113] overflow-y-auto p-4">
        <div className="flex min-h-full items-center justify-center py-4">
          <div
            role="dialog"
            aria-modal="true"
            className="pointer-events-auto w-full max-w-[1080px] overflow-hidden rounded-[2rem] border border-white/20 bg-[#f8f8f6] shadow-2xl shadow-black/30 dark:bg-[#101115]"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="flex items-center justify-between gap-4 border-b border-gray-200/70 bg-white/80 px-6 py-5 dark:border-gray-800/80 dark:bg-black/20">
              <div>
                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.2em] text-gray-500 dark:text-gray-400">
                  <Activity className="h-3.5 w-3.5" />
                  MCP Scheduling Monitor
                </div>
                <h2 className="mt-1 text-xl font-semibold text-gray-900 dark:text-gray-100">
                  Active MCP automations and recent execution results
                </h2>
              </div>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => void onRefresh()}
                  disabled={isBusy}
                  className="inline-flex items-center gap-2 rounded-xl border border-gray-200 bg-white px-3 py-2 text-sm text-gray-700 transition-colors hover:bg-gray-50 disabled:opacity-60 dark:border-gray-700 dark:bg-gray-900 dark:text-gray-200 dark:hover:bg-gray-800"
                >
                  <RefreshCw className={cn("h-4 w-4", isBusy && "animate-spin")} />
                  Refresh
                </button>
                <button
                  type="button"
                  onClick={onClose}
                  className="inline-flex h-10 w-10 items-center justify-center rounded-xl border border-gray-200 bg-white text-gray-600 transition-colors hover:bg-gray-50 dark:border-gray-700 dark:bg-gray-900 dark:text-gray-300 dark:hover:bg-gray-800"
                >
                  <X className="h-5 w-5" />
                </button>
              </div>
            </div>

            <div className="grid gap-6 p-6 lg:grid-cols-[0.92fr,1.08fr]">
              <div className="space-y-4">
                <div className="rounded-[1.75rem] border border-gray-200/80 bg-white/85 p-5 shadow-sm dark:border-gray-800/70 dark:bg-gray-900/60">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">
                        Active plans
                      </div>
                      <div className="mt-2 text-3xl font-semibold text-gray-900 dark:text-gray-100">
                        {activePlans.length}
                      </div>
                    </div>
                    <CheckCircle2 className="h-9 w-9 text-emerald-500" />
                  </div>
                  <div className="mt-3 text-sm text-gray-500 dark:text-gray-400">
                    {planningState.plans.length} total MCP plan(s) configured
                  </div>
                </div>

                <div className="rounded-[1.75rem] border border-gray-200/80 bg-white/85 p-5 shadow-sm dark:border-gray-800/70 dark:bg-gray-900/60">
                  <div className="flex items-center gap-2">
                    <CalendarClock className="h-4 w-4 text-indigo-500" />
                    <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">Configured plans</h3>
                  </div>
                  <div className="mt-4 space-y-3">
                    {planningState.plans.length === 0 && (
                      <div className="rounded-2xl border border-dashed border-gray-200 px-4 py-6 text-sm text-gray-500 dark:border-gray-700 dark:text-gray-400">
                        No MCP plan saved yet.
                      </div>
                    )}
                    {planningState.plans.map((plan) => (
                      <div key={plan.id} className="rounded-2xl border border-gray-200 bg-white p-4 dark:border-gray-700 dark:bg-gray-950">
                        <div className="flex items-start justify-between gap-3">
                          <div className="min-w-0">
                            <div className="text-sm font-semibold text-gray-900 dark:text-gray-100">{plan.name || "Untitled MCP plan"}</div>
                            <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                              Next run: {formatDateLabel(plan.nextRunAt)}
                            </div>
                          </div>
                          <span className={cn("inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-medium", statusTone(plan.status))}>
                            {plan.status}
                          </span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>

              <div className="rounded-[1.75rem] border border-gray-200/80 bg-white/85 p-5 shadow-sm dark:border-gray-800/70 dark:bg-gray-900/60">
                <div className="flex items-center gap-2">
                  <Activity className="h-4 w-4 text-sky-500" />
                  <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">Last 5 executions</h3>
                </div>
                <div className="mt-4 space-y-3">
                  {recentRuns.length === 0 && (
                    <div className="rounded-2xl border border-dashed border-gray-200 px-4 py-6 text-sm text-gray-500 dark:border-gray-700 dark:text-gray-400">
                      No execution recorded yet.
                    </div>
                  )}
                  {recentRuns.map((run) => (
                    <div key={run.id} className="rounded-2xl border border-gray-200 bg-white p-4 dark:border-gray-700 dark:bg-gray-950">
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="text-sm font-semibold text-gray-900 dark:text-gray-100">{run.planName}</div>
                          <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                            {formatDateLabel(run.startedAt)} · {run.triggerLabel}
                          </div>
                        </div>
                        <span className={cn("inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-medium", statusTone(run.status))}>
                          {run.status}
                        </span>
                      </div>
                      <div className="mt-3 text-sm text-gray-600 dark:text-gray-300">
                        {run.summary || "No summary captured yet."}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </>,
    document.body
  );
}
