import React, { useEffect } from "react";
import { createPortal } from "react-dom";
import {
  CalendarClock,
  Database,
  Mail,
  Network,
  Pause,
  Play,
  RefreshCw,
  Sheet,
  Trash2,
  Workflow,
  X,
} from "lucide-react";
import { CrewPlan, CrewPlanDraft, MCP_ORCHESTRATOR_ID, McpTool, PlanningBackendState, PlanningWeekday, cn } from "../lib/utils";

interface McpPlanningModalProps {
  isOpen: boolean;
  onClose: () => void;
  draft: CrewPlanDraft;
  editingPlanId: string | null;
  planningState: PlanningBackendState;
  mcpTools: McpTool[];
  isBusy: boolean;
  error: string | null;
  onDraftChange: (draft: CrewPlanDraft) => void;
  onStartNewDraft: () => void;
  onSavePlan: (draft: CrewPlanDraft, editingPlanId: string | null) => Promise<void>;
  onEditPlan: (plan: CrewPlan) => void;
  onTogglePlanStatus: (plan: CrewPlan) => Promise<void>;
  onDeletePlan: (planId: string) => Promise<void>;
  onRunPlan: (planId: string) => Promise<void>;
  onRefresh: () => Promise<void>;
}

const TRIGGER_OPTIONS = [
  { kind: "once", title: "One time", description: "Run once on a specific date and time." },
  { kind: "daily", title: "Daily", description: "Run every day at a fixed time." },
  { kind: "weekly", title: "Weekly", description: "Run on selected weekdays." },
  { kind: "interval", title: "Interval", description: "Run every N minutes." },
  { kind: "clickhouse_watch", title: "ClickHouse watch", description: "Run when a watch SQL changes or returns rows." },
  { kind: "file_watch", title: "File watch", description: "Run when files appear in a directory." },
] as const;

const WEEKDAYS: PlanningWeekday[] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];

function formatDateLabel(value: string | null | undefined) {
  if (!value) return "—";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function statusTone(status: CrewPlan["status"] | CrewPlan["lastStatus"] | null | undefined) {
  if (status === "active" || status === "success") return "text-emerald-700 bg-emerald-50 border-emerald-200";
  if (status === "paused") return "text-amber-700 bg-amber-50 border-amber-200";
  if (status === "running") return "text-sky-700 bg-sky-50 border-sky-200";
  if (status === "error") return "text-red-700 bg-red-50 border-red-200";
  return "text-gray-600 bg-gray-50 border-gray-200";
}

function triggerSummary(trigger: CrewPlanDraft["trigger"]) {
  if (trigger.kind === "once") return trigger.oneTimeAt || "Pick a date and time";
  if (trigger.kind === "daily") return `Every day at ${trigger.timeOfDay || "09:00"}`;
  if (trigger.kind === "weekly") {
    const days = trigger.weekdays.length > 0 ? trigger.weekdays.map((day) => day.toUpperCase()).join(", ") : "No weekday selected";
    return `${days} at ${trigger.timeOfDay || "09:00"}`;
  }
  if (trigger.kind === "interval") return `Every ${trigger.intervalMinutes || 60} minute(s)`;
  if (trigger.kind === "clickhouse_watch") return trigger.watchSql.trim() ? "ClickHouse watch query configured" : "Add a watch SQL query";
  return trigger.directory.trim() ? `Watching ${trigger.directory}` : "Set a directory to watch";
}

export function McpPlanningModal({
  isOpen,
  onClose,
  draft,
  editingPlanId,
  planningState,
  mcpTools,
  isBusy,
  error,
  onDraftChange,
  onStartNewDraft,
  onSavePlan,
  onEditPlan,
  onTogglePlanStatus,
  onDeletePlan,
  onRunPlan,
  onRefresh,
}: McpPlanningModalProps) {
  useEffect(() => {
    if (!isOpen) return undefined;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [isOpen, onClose]);

  if (!isOpen || typeof document === "undefined") return null;

  const updateDraft = (next: Partial<CrewPlanDraft>) => {
    onDraftChange({
      ...draft,
      ...next,
      agents: [],
    });
  };

  const updateTrigger = (next: Partial<CrewPlanDraft["trigger"]>) => {
    onDraftChange({
      ...draft,
      agents: [],
      trigger: {
        ...draft.trigger,
        ...next,
      },
    });
  };

  const toggleMcpTool = (toolId: string) => {
    const hasTool = draft.mcpToolIds.includes(toolId);
    updateDraft({
      mcpToolIds: hasTool
        ? draft.mcpToolIds.filter((item) => item !== toolId)
        : [...draft.mcpToolIds, toolId],
    });
  };

  const toggleMcpOrchestrator = () => {
    updateDraft({
      useMcpOrchestrator: !draft.useMcpOrchestrator,
    });
  };

  const toggleWeekday = (day: PlanningWeekday) => {
    const hasDay = draft.trigger.weekdays.includes(day);
    updateTrigger({
      weekdays: hasDay
        ? draft.trigger.weekdays.filter((item) => item !== day)
        : [...draft.trigger.weekdays, day],
    });
  };

  const handleSave = async () => {
    await onSavePlan({ ...draft, agents: [] }, editingPlanId);
  };

  const selectedMcpTools = mcpTools.filter((tool) => draft.mcpToolIds.includes(tool.id));
  const selectedExecutorCount = selectedMcpTools.length + (draft.useMcpOrchestrator ? 1 : 0);
  const activeTrigger = TRIGGER_OPTIONS.find((option) => option.kind === draft.trigger.kind) ?? TRIGGER_OPTIONS[0];
  const mcpPlans = planningState.plans;
  const recentRuns = planningState.runs.slice(0, 5);
  const updateExportPostAction = (next: Partial<CrewPlanDraft["postActions"]["exportFile"]>) => {
    updateDraft({
      postActions: {
        ...draft.postActions,
        exportFile: {
          ...draft.postActions.exportFile,
          ...next,
        },
      },
    });
  };
  const updateEmailPostAction = (next: Partial<CrewPlanDraft["postActions"]["sendEmail"]>) => {
    updateDraft({
      postActions: {
        ...draft.postActions,
        sendEmail: {
          ...draft.postActions.sendEmail,
          ...next,
        },
      },
    });
  };

  return createPortal(
    <>
      <div className="fixed inset-0 z-[110] bg-black/45 backdrop-blur-sm" onClick={onClose} />
      <div className="pointer-events-none fixed inset-0 z-[111] overflow-y-auto p-4">
        <div className="flex min-h-full items-center justify-center py-4">
          <div
            role="dialog"
            aria-modal="true"
            aria-label="MCP scheduling"
            className="pointer-events-auto relative flex h-[min(94vh,960px)] w-full max-w-[1320px] flex-col overflow-hidden rounded-[2.1rem] border border-white/30 bg-[#f7f7f4] shadow-[0_30px_80px_rgba(15,23,42,0.35)] dark:border-white/10 dark:bg-[#101115]"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="flex items-start justify-between gap-4 border-b border-gray-200/80 bg-white/75 px-6 py-5 backdrop-blur-xl dark:border-gray-800/80 dark:bg-black/20">
              <div>
                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.22em] text-sky-600 dark:text-sky-300">
                  <Workflow className="h-3.5 w-3.5" />
                  MCP Scheduling
                </div>
                <h2 className="mt-1 text-xl font-semibold text-gray-900 dark:text-gray-100">
                  Schedule MCP connectors and the MCP Orchestrator
                </h2>
                <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                  Lightweight LangGraph setup focused only on MCP executions and recurring tool automation.
                </p>
              </div>
              <button
                type="button"
                onClick={onClose}
                className="inline-flex h-10 w-10 items-center justify-center rounded-xl border border-gray-200 bg-white text-gray-600 transition-colors hover:bg-gray-50 dark:border-gray-700 dark:bg-gray-900 dark:text-gray-300 dark:hover:bg-gray-800"
              >
                <X className="h-5 w-5" />
              </button>
            </div>

            <div className="flex-1 overflow-y-auto">
              <div className="grid gap-6 p-6 xl:grid-cols-[0.95fr,1.05fr]">
                <div className="space-y-5">
                  <div className="rounded-[1.75rem] border border-gray-200/80 bg-white/85 p-5 shadow-sm dark:border-gray-800/70 dark:bg-gray-900/60">
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">
                          Plan identity
                        </div>
                        <div className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                          Define the MCP automation objective and its status.
                        </div>
                      </div>
                      <button
                        type="button"
                        onClick={onStartNewDraft}
                        className="rounded-full border border-gray-200 bg-white px-3 py-1.5 text-[11px] font-medium text-gray-700 transition-colors hover:bg-gray-50 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-200 dark:hover:bg-gray-900"
                      >
                        New draft
                      </button>
                    </div>

                    <div className="mt-4 space-y-4">
                      <label className="block">
                        <span className="mb-1.5 block text-xs font-medium text-gray-600 dark:text-gray-300">Plan name</span>
                        <input
                          type="text"
                          value={draft.name}
                          onChange={(event) => updateDraft({ name: event.target.value })}
                          placeholder="Nightly MCP sync"
                          className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                        />
                      </label>

                      <label className="block">
                        <span className="mb-1.5 block text-xs font-medium text-gray-600 dark:text-gray-300">Prompt</span>
                        <textarea
                          value={draft.prompt}
                          onChange={(event) => updateDraft({ prompt: event.target.value })}
                          placeholder="Describe what the MCP execution should do when it runs."
                          className="min-h-[140px] w-full rounded-[1.4rem] border border-gray-200 bg-white px-4 py-3 text-sm leading-6 text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                        />
                      </label>

                      <label className="block">
                        <span className="mb-1.5 block text-xs font-medium text-gray-600 dark:text-gray-300">Status</span>
                        <select
                          value={draft.status}
                          onChange={(event) => updateDraft({ status: event.target.value as CrewPlanDraft["status"] })}
                          className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                        >
                          <option value="active">Active</option>
                          <option value="paused">Paused</option>
                        </select>
                      </label>
                    </div>
                  </div>

                  <div className="rounded-[1.75rem] border border-gray-200/80 bg-white/85 p-5 shadow-sm dark:border-gray-800/70 dark:bg-gray-900/60">
                    <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">
                      Executors
                    </div>
                    <div className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                      Select one or more MCP connectors, or let the MCP Orchestrator plan across the full catalog.
                    </div>

                    <div className="mt-4 space-y-3">
                      <button
                        type="button"
                        onClick={toggleMcpOrchestrator}
                        className={cn(
                          "flex w-full items-start gap-3 rounded-[1.35rem] border px-4 py-3 text-left transition-colors",
                          draft.useMcpOrchestrator
                            ? "border-teal-300 bg-teal-50 text-teal-900 dark:border-teal-700 dark:bg-teal-950/35 dark:text-teal-100"
                            : "border-gray-200 bg-white text-gray-700 hover:bg-gray-50 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-200 dark:hover:bg-gray-900"
                        )}
                      >
                        <Network className="mt-0.5 h-4 w-4 flex-shrink-0" />
                        <div>
                          <div className="text-sm font-semibold">MCP Orchestrator</div>
                          <div className="mt-1 text-xs opacity-80">Discovers tools first, builds a plan, then coordinates one or more MCPs.</div>
                        </div>
                      </button>

                      <div className="flex flex-wrap gap-2">
                        {mcpTools.map((tool) => {
                          const checked = draft.mcpToolIds.includes(tool.id);
                          return (
                            <button
                              key={tool.id}
                              type="button"
                              onClick={() => toggleMcpTool(tool.id)}
                              className={cn(
                                "inline-flex items-center gap-2 rounded-full border px-3.5 py-2 text-xs font-medium transition-colors",
                                checked
                                  ? "border-sky-300 bg-sky-50 text-sky-800 dark:border-sky-700 dark:bg-sky-950/35 dark:text-sky-100"
                                  : "border-gray-200 bg-white text-gray-700 hover:bg-gray-50 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-200 dark:hover:bg-gray-900"
                              )}
                            >
                              <Database className="h-3.5 w-3.5" />
                              {tool.label}
                            </button>
                          );
                        })}
                      </div>
                    </div>
                  </div>

                  <div className="rounded-[1.75rem] border border-gray-200/80 bg-white/85 p-5 shadow-sm dark:border-gray-800/70 dark:bg-gray-900/60">
                    <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">
                      Trigger
                    </div>
                    <div className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                      Same trigger model as LangGraph Planning, simplified for MCP workflows.
                    </div>

                    <div className="mt-4 grid gap-2 md:grid-cols-2 xl:grid-cols-3">
                      {TRIGGER_OPTIONS.map((option) => (
                        <button
                          key={option.kind}
                          type="button"
                          onClick={() => updateTrigger({ kind: option.kind })}
                          className={cn(
                            "rounded-[1.25rem] border px-4 py-3 text-left transition-colors",
                            draft.trigger.kind === option.kind
                              ? "border-sky-300 bg-sky-50 text-sky-900 dark:border-sky-700 dark:bg-sky-950/35 dark:text-sky-100"
                              : "border-gray-200 bg-white text-gray-700 hover:bg-gray-50 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-200 dark:hover:bg-gray-900"
                          )}
                        >
                          <div className="text-sm font-semibold">{option.title}</div>
                          <div className="mt-1 text-xs opacity-75">{option.description}</div>
                        </button>
                      ))}
                    </div>

                    <div className="mt-4 grid gap-4 md:grid-cols-2">
                      {(activeTrigger.kind === "once") && (
                        <label className="block">
                          <span className="mb-1.5 block text-xs font-medium text-gray-600 dark:text-gray-300">Run at</span>
                          <input
                            type="datetime-local"
                            value={draft.trigger.oneTimeAt}
                            onChange={(event) => updateTrigger({ oneTimeAt: event.target.value })}
                            className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                          />
                        </label>
                      )}

                      {(activeTrigger.kind === "daily" || activeTrigger.kind === "weekly") && (
                        <label className="block">
                          <span className="mb-1.5 block text-xs font-medium text-gray-600 dark:text-gray-300">Time of day</span>
                          <input
                            type="time"
                            value={draft.trigger.timeOfDay}
                            onChange={(event) => updateTrigger({ timeOfDay: event.target.value })}
                            className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                          />
                        </label>
                      )}

                      {activeTrigger.kind === "weekly" && (
                        <div className="md:col-span-2">
                          <span className="mb-1.5 block text-xs font-medium text-gray-600 dark:text-gray-300">Weekdays</span>
                          <div className="flex flex-wrap gap-2">
                            {WEEKDAYS.map((day) => {
                              const checked = draft.trigger.weekdays.includes(day);
                              return (
                                <button
                                  key={day}
                                  type="button"
                                  onClick={() => toggleWeekday(day)}
                                  className={cn(
                                    "rounded-full border px-3 py-1.5 text-xs font-medium uppercase transition-colors",
                                    checked
                                      ? "border-sky-300 bg-sky-50 text-sky-800 dark:border-sky-700 dark:bg-sky-950/35 dark:text-sky-100"
                                      : "border-gray-200 bg-white text-gray-700 hover:bg-gray-50 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-200 dark:hover:bg-gray-900"
                                  )}
                                >
                                  {day}
                                </button>
                              );
                            })}
                          </div>
                        </div>
                      )}

                      {activeTrigger.kind === "interval" && (
                        <label className="block">
                          <span className="mb-1.5 block text-xs font-medium text-gray-600 dark:text-gray-300">Interval (minutes)</span>
                          <input
                            type="number"
                            min={1}
                            value={draft.trigger.intervalMinutes}
                            onChange={(event) => updateTrigger({ intervalMinutes: parseInt(event.target.value, 10) || 1 })}
                            className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                          />
                        </label>
                      )}

                      {activeTrigger.kind === "clickhouse_watch" && (
                        <>
                          <label className="block">
                            <span className="mb-1.5 block text-xs font-medium text-gray-600 dark:text-gray-300">Poll every (minutes)</span>
                            <input
                              type="number"
                              min={1}
                              value={draft.trigger.pollMinutes}
                              onChange={(event) => updateTrigger({ pollMinutes: parseInt(event.target.value, 10) || 1 })}
                              className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                            />
                          </label>
                          <label className="block">
                            <span className="mb-1.5 block text-xs font-medium text-gray-600 dark:text-gray-300">Watch mode</span>
                            <select
                              value={draft.trigger.watchMode}
                              onChange={(event) => updateTrigger({ watchMode: event.target.value as CrewPlanDraft["trigger"]["watchMode"] })}
                              className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                            >
                              <option value="result_changes">Result changes</option>
                              <option value="has_rows">Has rows</option>
                            </select>
                          </label>
                          <label className="block md:col-span-2">
                            <span className="mb-1.5 block text-xs font-medium text-gray-600 dark:text-gray-300">Watch SQL</span>
                            <textarea
                              value={draft.trigger.watchSql}
                              onChange={(event) => updateTrigger({ watchSql: event.target.value })}
                              className="min-h-[120px] w-full rounded-[1.4rem] border border-gray-200 bg-white px-4 py-3 text-sm leading-6 text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                            />
                          </label>
                        </>
                      )}

                      {activeTrigger.kind === "file_watch" && (
                        <>
                          <label className="block">
                            <span className="mb-1.5 block text-xs font-medium text-gray-600 dark:text-gray-300">Directory</span>
                            <input
                              type="text"
                              value={draft.trigger.directory}
                              onChange={(event) => updateTrigger({ directory: event.target.value })}
                              className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                            />
                          </label>
                          <label className="block">
                            <span className="mb-1.5 block text-xs font-medium text-gray-600 dark:text-gray-300">Pattern</span>
                            <input
                              type="text"
                              value={draft.trigger.pattern}
                              onChange={(event) => updateTrigger({ pattern: event.target.value })}
                              className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                            />
                          </label>
                        </>
                      )}
                    </div>
                  </div>

                  <div className="rounded-[1.75rem] border border-gray-200/80 bg-white/85 p-5 shadow-sm dark:border-gray-800/70 dark:bg-gray-900/60">
                    <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">
                      Post-actions
                    </div>
                    <div className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                      After the MCP run completes, optionally publish the result into the app chat, generate a file export, and/or send the result by email.
                    </div>

                    <div className="mt-4 space-y-4">
                      <div className="rounded-[1.35rem] border border-gray-200 bg-white p-4 dark:border-gray-700 dark:bg-gray-950">
                        <div className="flex items-start justify-between gap-3">
                          <div className="flex items-start gap-3">
                            <div className="mt-0.5 rounded-full bg-violet-50 p-2 text-violet-600 dark:bg-violet-950/35 dark:text-violet-300">
                              <Workflow className="h-4 w-4" />
                            </div>
                            <div>
                              <div className="text-sm font-semibold text-gray-900 dark:text-gray-100">Publish into the app chat</div>
                              <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                                Reuse a dedicated automation conversation for this schedule and append the final formatted answer after each run.
                              </div>
                            </div>
                          </div>
                          <label className="inline-flex cursor-pointer items-center gap-2 text-xs font-medium text-gray-600 dark:text-gray-300">
                            <input
                              type="checkbox"
                              checked={draft.postActions.publishToChat.enabled}
                              onChange={(event) =>
                                updateDraft({
                                  postActions: {
                                    ...draft.postActions,
                                    publishToChat: { enabled: event.target.checked },
                                  },
                                })
                              }
                            />
                            Enabled
                          </label>
                        </div>
                        {draft.postActions.publishToChat.enabled && (
                          <div className="mt-3 rounded-2xl border border-violet-100 bg-violet-50/70 px-4 py-3 text-xs leading-6 text-violet-900 dark:border-violet-900/40 dark:bg-violet-950/20 dark:text-violet-100">
                            The scheduler will reuse the same automation conversation for this plan, append each new result without clearing the history, and automatically keep only the latest 20 injected runs.
                          </div>
                        )}
                      </div>

                      <div className="rounded-[1.35rem] border border-gray-200 bg-white p-4 dark:border-gray-700 dark:bg-gray-950">
                        <div className="flex items-start justify-between gap-3">
                          <div className="flex items-start gap-3">
                            <div className="mt-0.5 rounded-full bg-sky-50 p-2 text-sky-600 dark:bg-sky-950/35 dark:text-sky-300">
                              <Sheet className="h-4 w-4" />
                            </div>
                            <div>
                              <div className="text-sm font-semibold text-gray-900 dark:text-gray-100">Generate a file</div>
                              <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                                Export the MCP run outputs as a reusable CSV, TSV, or Excel file.
                              </div>
                            </div>
                          </div>
                          <label className="inline-flex cursor-pointer items-center gap-2 text-xs font-medium text-gray-600 dark:text-gray-300">
                            <input
                              type="checkbox"
                              checked={draft.postActions.exportFile.enabled}
                              onChange={(event) => updateExportPostAction({ enabled: event.target.checked })}
                            />
                            Enabled
                          </label>
                        </div>

                        {draft.postActions.exportFile.enabled && (
                          <div className="mt-4 grid gap-4 md:grid-cols-2">
                            <label className="block">
                              <span className="mb-1.5 block text-xs font-medium text-gray-600 dark:text-gray-300">Format</span>
                              <select
                                value={draft.postActions.exportFile.format}
                                onChange={(event) => updateExportPostAction({ format: event.target.value as "csv" | "tsv" | "xlsx" })}
                                className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                              >
                                <option value="csv">CSV</option>
                                <option value="tsv">TSV</option>
                                <option value="xlsx">Excel (.xlsx)</option>
                              </select>
                            </label>
                            <label className="block">
                              <span className="mb-1.5 block text-xs font-medium text-gray-600 dark:text-gray-300">Path</span>
                              <input
                                type="text"
                                value={draft.postActions.exportFile.path}
                                onChange={(event) => updateExportPostAction({ path: event.target.value })}
                                placeholder="exports/mcp-results.csv"
                                className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                              />
                            </label>
                          </div>
                        )}
                      </div>

                      <div className="rounded-[1.35rem] border border-gray-200 bg-white p-4 dark:border-gray-700 dark:bg-gray-950">
                        <div className="flex items-start justify-between gap-3">
                          <div className="flex items-start gap-3">
                            <div className="mt-0.5 rounded-full bg-emerald-50 p-2 text-emerald-600 dark:bg-emerald-950/35 dark:text-emerald-300">
                              <Mail className="h-4 w-4" />
                            </div>
                            <div>
                              <div className="text-sm font-semibold text-gray-900 dark:text-gray-100">Send an email</div>
                              <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                                Deliver the run summary automatically, with optional attachment of the generated export file.
                              </div>
                            </div>
                          </div>
                          <label className="inline-flex cursor-pointer items-center gap-2 text-xs font-medium text-gray-600 dark:text-gray-300">
                            <input
                              type="checkbox"
                              checked={draft.postActions.sendEmail.enabled}
                              onChange={(event) => updateEmailPostAction({ enabled: event.target.checked })}
                            />
                            Enabled
                          </label>
                        </div>

                        {draft.postActions.sendEmail.enabled && (
                          <div className="mt-4 space-y-4">
                            <div className="grid gap-4 md:grid-cols-3">
                              <label className="block">
                                <span className="mb-1.5 block text-xs font-medium text-gray-600 dark:text-gray-300">To</span>
                                <input
                                  type="text"
                                  value={draft.postActions.sendEmail.to.join(", ")}
                                  onChange={(event) => updateEmailPostAction({ to: event.target.value.split(",").map((item) => item.trim()).filter(Boolean) })}
                                  placeholder="alice@company.com, bob@company.com"
                                  className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                                />
                              </label>
                              <label className="block">
                                <span className="mb-1.5 block text-xs font-medium text-gray-600 dark:text-gray-300">Cc</span>
                                <input
                                  type="text"
                                  value={draft.postActions.sendEmail.cc.join(", ")}
                                  onChange={(event) => updateEmailPostAction({ cc: event.target.value.split(",").map((item) => item.trim()).filter(Boolean) })}
                                  placeholder="optional"
                                  className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                                />
                              </label>
                              <label className="block">
                                <span className="mb-1.5 block text-xs font-medium text-gray-600 dark:text-gray-300">Bcc</span>
                                <input
                                  type="text"
                                  value={draft.postActions.sendEmail.bcc.join(", ")}
                                  onChange={(event) => updateEmailPostAction({ bcc: event.target.value.split(",").map((item) => item.trim()).filter(Boolean) })}
                                  placeholder="optional"
                                  className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                                />
                              </label>
                            </div>

                            <label className="block">
                              <span className="mb-1.5 block text-xs font-medium text-gray-600 dark:text-gray-300">Subject</span>
                              <input
                                type="text"
                                value={draft.postActions.sendEmail.subject}
                                onChange={(event) => updateEmailPostAction({ subject: event.target.value })}
                                placeholder="RAGnarok MCP automation result"
                                className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                              />
                            </label>

                            <label className="block">
                              <span className="mb-1.5 block text-xs font-medium text-gray-600 dark:text-gray-300">Email body template</span>
                              <textarea
                                value={draft.postActions.sendEmail.bodyTemplate}
                                onChange={(event) => updateEmailPostAction({ bodyTemplate: event.target.value })}
                                className="min-h-[150px] w-full rounded-[1.4rem] border border-gray-200 bg-white px-4 py-3 text-sm leading-6 text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                              />
                              <div className="mt-2 text-[11px] text-gray-500 dark:text-gray-400">
                                Available variables: <code>{"{plan_name}"}</code>, <code>{"{trigger_label}"}</code>, <code>{"{summary}"}</code>, <code>{"{outputs_markdown}"}</code>
                              </div>
                            </label>

                            <label className="inline-flex items-center gap-2 text-xs font-medium text-gray-600 dark:text-gray-300">
                              <input
                                type="checkbox"
                                checked={draft.postActions.sendEmail.attachExportedFile}
                                onChange={(event) => updateEmailPostAction({ attachExportedFile: event.target.checked })}
                              />
                              Attach the generated export file when available
                            </label>
                          </div>
                        )}
                      </div>
                    </div>
                  </div>

                  {error && (
                    <div className="rounded-[1.4rem] border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700 dark:border-red-800/70 dark:bg-red-950/30 dark:text-red-200">
                      {error}
                    </div>
                  )}
                </div>

                <div className="space-y-5">
                  <div className="rounded-[1.75rem] border border-gray-200/80 bg-white/85 p-5 shadow-sm dark:border-gray-800/70 dark:bg-gray-900/60">
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">
                          Launch summary
                        </div>
                        <div className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                          Quick review before saving the MCP automation.
                        </div>
                      </div>
                      <div className="rounded-full border border-sky-200 bg-sky-50 px-3 py-1 text-[11px] font-medium text-sky-700 dark:border-sky-800/70 dark:bg-sky-950/35 dark:text-sky-200">
                        {selectedExecutorCount} executor{selectedExecutorCount === 1 ? "" : "s"}
                      </div>
                    </div>

                    <div className="mt-4 space-y-3 text-sm text-gray-700 dark:text-gray-200">
                      <div className="rounded-[1.2rem] border border-gray-200/80 bg-white p-3 dark:border-gray-700 dark:bg-gray-950">
                        <div className="text-[11px] uppercase tracking-[0.16em] text-gray-500 dark:text-gray-400">Objective</div>
                        <div className="mt-1">{draft.prompt.trim() || "No execution objective yet."}</div>
                      </div>
                      <div className="rounded-[1.2rem] border border-gray-200/80 bg-white p-3 dark:border-gray-700 dark:bg-gray-950">
                        <div className="text-[11px] uppercase tracking-[0.16em] text-gray-500 dark:text-gray-400">Executors</div>
                        <div className="mt-1">
                          {draft.useMcpOrchestrator && <div>MCP Orchestrator</div>}
                          {selectedMcpTools.map((tool) => (
                            <div key={tool.id}>{tool.label}</div>
                          ))}
                          {!draft.useMcpOrchestrator && selectedMcpTools.length === 0 && "No executor selected yet."}
                        </div>
                      </div>
                      <div className="rounded-[1.2rem] border border-gray-200/80 bg-white p-3 dark:border-gray-700 dark:bg-gray-950">
                        <div className="text-[11px] uppercase tracking-[0.16em] text-gray-500 dark:text-gray-400">Trigger</div>
                        <div className="mt-1">{triggerSummary(draft.trigger)}</div>
                      </div>
                    </div>

                    <div className="mt-4 flex flex-wrap gap-2">
                      <button
                        type="button"
                        onClick={() => void handleSave()}
                        disabled={isBusy}
                        className="inline-flex items-center gap-2 rounded-full bg-black px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-gray-800 disabled:opacity-60"
                      >
                        <Workflow className="h-4 w-4" />
                        {editingPlanId ? "Save changes" : "Save MCP plan"}
                      </button>
                      <button
                        type="button"
                        onClick={() => void onRefresh()}
                        disabled={isBusy}
                        className="inline-flex items-center gap-2 rounded-full border border-gray-200 bg-white px-4 py-2 text-sm font-medium text-gray-700 transition-colors hover:bg-gray-50 disabled:opacity-60 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-200 dark:hover:bg-gray-900"
                      >
                        <RefreshCw className="h-4 w-4" />
                        Refresh
                      </button>
                    </div>
                  </div>

                  <div className="rounded-[1.75rem] border border-gray-200/80 bg-white/85 p-5 shadow-sm dark:border-gray-800/70 dark:bg-gray-900/60">
                    <div className="flex items-center gap-2">
                      <Database className="h-4 w-4 text-sky-500" />
                      <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">Saved MCP plans</h3>
                    </div>
                    <div className="mt-4 space-y-3">
                      {mcpPlans.length === 0 && (
                        <div className="rounded-2xl border border-dashed border-gray-200 px-4 py-6 text-sm text-gray-500 dark:border-gray-700 dark:text-gray-400">
                          No MCP plan saved yet.
                        </div>
                      )}

                      {mcpPlans.map((plan) => (
                        <div key={plan.id} className="rounded-2xl border border-gray-200 bg-white p-4 dark:border-gray-700 dark:bg-gray-950">
                          <div className="flex items-start justify-between gap-3">
                            <div className="min-w-0">
                              <div className="text-sm font-semibold text-gray-900 dark:text-gray-100">{plan.name || "Untitled MCP plan"}</div>
                              <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                                {(plan.useMcpOrchestrator ? ["MCP Orchestrator"] : [])
                                  .concat((plan.mcpToolIds || []).map((toolId) => mcpTools.find((tool) => tool.id === toolId)?.label || toolId))
                                  .join(" · ") || "No executor"}
                              </div>
                            </div>
                            <span className={cn("rounded-full border px-2.5 py-1 text-[11px] font-medium", statusTone(plan.status))}>
                              {plan.status}
                            </span>
                          </div>
                          <div className="mt-3 text-xs text-gray-500 dark:text-gray-400">
                            <div>Next run: {formatDateLabel(plan.nextRunAt)}</div>
                            <div>Last run: {formatDateLabel(plan.lastRunAt)}</div>
                          </div>
                          <div className="mt-3 flex flex-wrap gap-2">
                            <button
                              type="button"
                              onClick={() => onEditPlan(plan)}
                              className="rounded-full border border-gray-200 bg-white px-3 py-1.5 text-[11px] font-medium text-gray-700 transition-colors hover:bg-gray-50 dark:border-gray-700 dark:bg-gray-900 dark:text-gray-200 dark:hover:bg-gray-800"
                            >
                              Edit
                            </button>
                            <button
                              type="button"
                              onClick={() => void onRunPlan(plan.id)}
                              className="inline-flex items-center gap-1 rounded-full border border-sky-200 bg-sky-50 px-3 py-1.5 text-[11px] font-medium text-sky-700 transition-colors hover:bg-sky-100 dark:border-sky-800/70 dark:bg-sky-950/35 dark:text-sky-200 dark:hover:bg-sky-950/45"
                            >
                              <Play className="h-3.5 w-3.5" />
                              Run now
                            </button>
                            <button
                              type="button"
                              onClick={() => void onTogglePlanStatus(plan)}
                              className="inline-flex items-center gap-1 rounded-full border border-amber-200 bg-amber-50 px-3 py-1.5 text-[11px] font-medium text-amber-700 transition-colors hover:bg-amber-100 dark:border-amber-800/70 dark:bg-amber-950/35 dark:text-amber-200 dark:hover:bg-amber-950/45"
                            >
                              {plan.status === "active" ? <Pause className="h-3.5 w-3.5" /> : <Play className="h-3.5 w-3.5" />}
                              {plan.status === "active" ? "Pause" : "Resume"}
                            </button>
                            <button
                              type="button"
                              onClick={() => void onDeletePlan(plan.id)}
                              className="inline-flex items-center gap-1 rounded-full border border-red-200 bg-red-50 px-3 py-1.5 text-[11px] font-medium text-red-700 transition-colors hover:bg-red-100 dark:border-red-800/70 dark:bg-red-950/35 dark:text-red-200 dark:hover:bg-red-950/45"
                            >
                              <Trash2 className="h-3.5 w-3.5" />
                              Delete
                            </button>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>

                  <div className="rounded-[1.75rem] border border-gray-200/80 bg-white/85 p-5 shadow-sm dark:border-gray-800/70 dark:bg-gray-900/60">
                    <div className="flex items-center gap-2">
                      <CalendarClock className="h-4 w-4 text-emerald-500" />
                      <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">Last 5 MCP runs</h3>
                    </div>
                    <div className="mt-4 space-y-3">
                      {recentRuns.length === 0 && (
                        <div className="rounded-2xl border border-dashed border-gray-200 px-4 py-6 text-sm text-gray-500 dark:border-gray-700 dark:text-gray-400">
                          No MCP execution yet.
                        </div>
                      )}
                      {recentRuns.map((run) => (
                        <div key={run.id} className="rounded-2xl border border-gray-200 bg-white p-4 dark:border-gray-700 dark:bg-gray-950">
                          <div className="flex items-start justify-between gap-3">
                            <div className="min-w-0">
                              <div className="text-sm font-semibold text-gray-900 dark:text-gray-100">{run.planName}</div>
                              <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">{formatDateLabel(run.startedAt)}</div>
                            </div>
                            <span className={cn("rounded-full border px-2.5 py-1 text-[11px] font-medium", statusTone(run.status))}>
                              {run.status}
                            </span>
                          </div>
                          <div className="mt-2 text-sm text-gray-600 dark:text-gray-300">
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
        </div>
      </div>
    </>,
    document.body
  );
}
