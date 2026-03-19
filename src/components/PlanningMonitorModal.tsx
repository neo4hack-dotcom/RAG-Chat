import React from "react";
import { createPortal } from "react-dom";
import {
  Activity,
  AlertTriangle,
  CalendarClock,
  CheckCircle2,
  Clock3,
  PlayCircle,
  RefreshCw,
  TimerReset,
  X,
} from "lucide-react";
import { CrewPlan, CrewPlanRun, PlanningBackendState, cn } from "../lib/utils";

interface PlanningMonitorModalProps {
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

export function PlanningMonitorModal({
  isOpen,
  onClose,
  planningState,
  isBusy,
  onRefresh,
}: PlanningMonitorModalProps) {
  if (!isOpen || typeof document === "undefined") return null;

  const activePlans = planningState.plans.filter((plan) => plan.status === "active");
  const pausedPlans = planningState.plans.filter((plan) => plan.status === "paused");
  const runningRuns = planningState.runs.filter((run) => run.status === "running");
  const successfulRuns = planningState.runs.filter((run) => run.status === "success");
  const failedRuns = planningState.runs.filter((run) => run.status === "error");
  const watchPlans = planningState.plans.filter((plan) => (
    plan.trigger.kind === "clickhouse_watch" || plan.trigger.kind === "file_watch"
  ));
  const fixedSchedulePlans = planningState.plans.filter((plan) => (
    plan.trigger.kind !== "clickhouse_watch" && plan.trigger.kind !== "file_watch"
  ));
  const successRate = planningState.runs.length > 0
    ? Math.round((successfulRuns.length / planningState.runs.length) * 100)
    : 0;
  const upcomingPlans = [...planningState.plans]
    .filter((plan) => Boolean(plan.nextRunAt))
    .sort((left, right) => {
      const leftTime = Date.parse(left.nextRunAt || "");
      const rightTime = Date.parse(right.nextRunAt || "");
      return leftTime - rightTime;
    })
    .slice(0, 8);

  return createPortal(
    <>
      <div
        className="fixed inset-0 z-[82] bg-black/50 backdrop-blur-sm"
        onClick={onClose}
      />
      <div className="fixed inset-0 z-[83] overflow-y-auto p-4">
        <div className="flex min-h-full items-center justify-center">
          <div
            className="flex h-full w-full max-w-[1440px] flex-col overflow-hidden rounded-[2rem] border border-white/20 bg-[#f8f8f6] shadow-2xl shadow-black/30 dark:bg-[#101115]"
            onClick={(event) => event.stopPropagation()}
          >
          <div className="flex items-center justify-between gap-4 border-b border-gray-200/70 bg-white/80 px-6 py-5 dark:border-gray-800/80 dark:bg-black/20">
            <div>
              <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.2em] text-gray-500 dark:text-gray-400">
                <Activity className="h-3.5 w-3.5" />
                CrewAI - Planning Monitor
              </div>
              <h2 className="mt-1 text-xl font-semibold text-gray-900 dark:text-gray-100">
                Execution tracking, scheduling health, and recent agent activity
              </h2>
              <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                Monitor active plans, live runs, recent outcomes, and the next scheduled executions.
              </p>
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

          <div className="flex-1 overflow-y-auto">
            <div className="space-y-6 p-6">
              <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                <div className="rounded-[1.75rem] border border-gray-200/80 bg-white/85 p-5 shadow-sm dark:border-gray-800/70 dark:bg-gray-900/60">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">
                        Saved plans
                      </div>
                      <div className="mt-2 text-3xl font-semibold text-gray-900 dark:text-gray-100">
                        {planningState.plans.length}
                      </div>
                    </div>
                    <CalendarClock className="h-9 w-9 text-indigo-500" />
                  </div>
                  <div className="mt-3 text-sm text-gray-500 dark:text-gray-400">
                    {activePlans.length} active · {pausedPlans.length} paused
                  </div>
                </div>

                <div className="rounded-[1.75rem] border border-gray-200/80 bg-white/85 p-5 shadow-sm dark:border-gray-800/70 dark:bg-gray-900/60">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">
                        Execution health
                      </div>
                      <div className="mt-2 text-3xl font-semibold text-gray-900 dark:text-gray-100">
                        {successRate}%
                      </div>
                    </div>
                    <CheckCircle2 className="h-9 w-9 text-emerald-500" />
                  </div>
                  <div className="mt-3 text-sm text-gray-500 dark:text-gray-400">
                    {successfulRuns.length} success · {failedRuns.length} error
                  </div>
                </div>

                <div className="rounded-[1.75rem] border border-gray-200/80 bg-white/85 p-5 shadow-sm dark:border-gray-800/70 dark:bg-gray-900/60">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">
                        Live activity
                      </div>
                      <div className="mt-2 text-3xl font-semibold text-gray-900 dark:text-gray-100">
                        {runningRuns.length}
                      </div>
                    </div>
                    <PlayCircle className="h-9 w-9 text-sky-500" />
                  </div>
                  <div className="mt-3 text-sm text-gray-500 dark:text-gray-400">
                    Run(s) currently in progress
                  </div>
                </div>

                <div className="rounded-[1.75rem] border border-gray-200/80 bg-white/85 p-5 shadow-sm dark:border-gray-800/70 dark:bg-gray-900/60">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">
                        Trigger mix
                      </div>
                      <div className="mt-2 text-3xl font-semibold text-gray-900 dark:text-gray-100">
                        {watchPlans.length}
                      </div>
                    </div>
                    <TimerReset className="h-9 w-9 text-fuchsia-500" />
                  </div>
                  <div className="mt-3 text-sm text-gray-500 dark:text-gray-400">
                    Watch plans · {fixedSchedulePlans.length} fixed schedule
                  </div>
                </div>
              </div>

              <div className="grid gap-6 xl:grid-cols-[0.95fr,1.05fr]">
                <div className="rounded-[1.75rem] border border-gray-200/80 bg-white/85 p-5 shadow-sm dark:border-gray-800/70 dark:bg-gray-900/60">
                  <div className="flex items-center gap-2">
                    <Clock3 className="h-4 w-4 text-indigo-500" />
                    <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">Upcoming schedule</h3>
                  </div>
                  <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                    Next known executions across all active and paused plans.
                  </p>

                  <div className="mt-4 space-y-3">
                    {upcomingPlans.length === 0 && (
                      <div className="rounded-2xl border border-dashed border-gray-200 px-4 py-6 text-sm text-gray-500 dark:border-gray-700 dark:text-gray-400">
                        No upcoming execution is registered yet.
                      </div>
                    )}

                    {upcomingPlans.map((plan) => (
                      <div key={plan.id} className="rounded-2xl border border-gray-200 bg-white p-4 dark:border-gray-700 dark:bg-gray-950">
                        <div className="flex items-start justify-between gap-3">
                          <div className="min-w-0">
                            <div className="text-sm font-semibold text-gray-900 dark:text-gray-100">
                              {plan.name || "Untitled plan"}
                            </div>
                            <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                              {plan.trigger.kind} · {plan.agents.join(", ") || "No agents"}
                            </div>
                          </div>
                          <span className={cn("inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-medium", statusTone(plan.status))}>
                            {plan.status}
                          </span>
                        </div>
                        <div className="mt-3 text-sm text-gray-600 dark:text-gray-300">
                          Next run: <strong>{formatDateLabel(plan.nextRunAt)}</strong>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                <div className="rounded-[1.75rem] border border-gray-200/80 bg-white/85 p-5 shadow-sm dark:border-gray-800/70 dark:bg-gray-900/60">
                  <div className="flex items-center gap-2">
                    <Activity className="h-4 w-4 text-sky-500" />
                    <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">Execution log</h3>
                  </div>
                  <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                    Live and recent runs, with their agent outputs and latest summaries.
                  </p>

                  <div className="mt-4 space-y-3 max-h-[58vh] overflow-y-auto pr-1">
                    {planningState.runs.length === 0 && (
                      <div className="rounded-2xl border border-dashed border-gray-200 px-4 py-6 text-sm text-gray-500 dark:border-gray-700 dark:text-gray-400">
                        No execution log yet.
                      </div>
                    )}

                    {planningState.runs.map((run) => (
                      <div key={run.id} className="rounded-2xl border border-gray-200 bg-white p-4 dark:border-gray-700 dark:bg-gray-950">
                        <div className="flex flex-wrap items-start justify-between gap-3">
                          <div className="min-w-0">
                            <div className="text-sm font-semibold text-gray-900 dark:text-gray-100">
                              {run.planName}
                            </div>
                            <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                              {run.triggerLabel || run.triggerKind}
                            </div>
                          </div>
                          <span className={cn("inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-medium", statusTone(run.status))}>
                            {run.status}
                          </span>
                        </div>

                        <div className="mt-3 grid gap-2 text-xs text-gray-500 dark:text-gray-400 md:grid-cols-2">
                          <div>Started: {formatDateLabel(run.startedAt)}</div>
                          <div>Finished: {formatDateLabel(run.finishedAt)}</div>
                        </div>

                        {run.outputs.length > 0 && (
                          <div className="mt-3 flex flex-wrap gap-2">
                            {run.outputs.map((output, index) => (
                              <span
                                key={`${run.id}-${output.agent}-${index}`}
                                className={cn(
                                  "inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-medium",
                                  output.status === "success"
                                    ? "border-emerald-200 bg-emerald-50 text-emerald-700"
                                    : "border-red-200 bg-red-50 text-red-700"
                                )}
                              >
                                {output.agent} · {output.status}
                              </span>
                            ))}
                          </div>
                        )}

                        {run.summary && (
                          <p className="mt-3 text-sm leading-relaxed text-gray-600 dark:text-gray-300">
                            {run.summary.replace(/^##\s+/gm, "").slice(0, 420)}
                          </p>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              </div>

              <div className="rounded-[1.75rem] border border-gray-200/80 bg-white/85 p-5 shadow-sm dark:border-gray-800/70 dark:bg-gray-900/60">
                <div className="flex items-center gap-2">
                  <AlertTriangle className="h-4 w-4 text-amber-500" />
                  <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">Plan health</h3>
                </div>
                <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                  Track the latest status, last run, and upcoming schedule for each saved automation.
                </p>

                <div className="mt-4 grid gap-3 lg:grid-cols-2">
                  {planningState.plans.length === 0 && (
                    <div className="rounded-2xl border border-dashed border-gray-200 px-4 py-6 text-sm text-gray-500 dark:border-gray-700 dark:text-gray-400">
                      No plan has been configured yet.
                    </div>
                  )}

                  {planningState.plans.map((plan) => (
                    <div key={plan.id} className="rounded-2xl border border-gray-200 bg-white p-4 dark:border-gray-700 dark:bg-gray-950">
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="text-sm font-semibold text-gray-900 dark:text-gray-100">
                            {plan.name || "Untitled plan"}
                          </div>
                          <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                            {plan.agents.join(", ") || "No agents"}
                          </div>
                        </div>
                        <div className="flex flex-col items-end gap-2">
                          <span className={cn("inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-medium", statusTone(plan.status))}>
                            {plan.status}
                          </span>
                          {plan.lastStatus && (
                            <span className={cn("inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-medium", statusTone(plan.lastStatus))}>
                              last {plan.lastStatus}
                            </span>
                          )}
                        </div>
                      </div>

                      <div className="mt-3 grid gap-2 text-xs text-gray-500 dark:text-gray-400 md:grid-cols-2">
                        <div>Next run: {formatDateLabel(plan.nextRunAt)}</div>
                        <div>Last run: {formatDateLabel(plan.lastRunAt)}</div>
                      </div>

                      {plan.lastSummary && (
                        <p className="mt-3 text-sm leading-relaxed text-gray-600 dark:text-gray-300">
                          {plan.lastSummary.replace(/^##\s+/gm, "").slice(0, 240)}
                        </p>
                      )}
                    </div>
                  ))}
                </div>
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
