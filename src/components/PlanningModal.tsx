import React from "react";
import { createPortal } from "react-dom";
import {
  X,
  RefreshCw,
  CalendarDays,
  Clock3,
  FolderOpen,
  Database,
  Play,
  Pause,
  Trash2,
  Bot,
  CheckCircle2,
} from "lucide-react";
import {
  AgentRole,
  CrewPlan,
  CrewPlanDraft,
  PlanningBackendState,
  PlanningWeekday,
  cn,
} from "../lib/utils";

interface PlanningModalProps {
  isOpen: boolean;
  onClose: () => void;
  draft: CrewPlanDraft;
  editingPlanId: string | null;
  planningState: PlanningBackendState;
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

const AGENT_OPTIONS: Array<{
  role: AgentRole;
  label: string;
  description: string;
  icon: React.ComponentType<{ className?: string }>;
  accent: string;
}> = [
  {
    role: "manager",
    label: "Manager",
    description: "Operational synthesis and prioritization.",
    icon: Bot,
    accent: "from-amber-500 to-orange-500",
  },
  {
    role: "clickhouse_query",
    label: "ClickHouse Query",
    description: "Safe SQL generation and execution against ClickHouse.",
    icon: Database,
    accent: "from-cyan-500 to-teal-500",
  },
  {
    role: "file_management",
    label: "File management",
    description: "Filesystem actions with previews, confirmations, and sandbox support.",
    icon: FolderOpen,
    accent: "from-emerald-500 to-lime-500",
  },
];

const WEEKDAYS: PlanningWeekday[] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];

const TRIGGER_OPTIONS = [
  {
    kind: "once",
    title: "One Time",
    description: "Run once on a precise date and time.",
    icon: CalendarDays,
  },
  {
    kind: "daily",
    title: "Daily",
    description: "Run every day at a fixed time.",
    icon: Clock3,
  },
  {
    kind: "weekly",
    title: "Weekly",
    description: "Run on selected weekdays.",
    icon: CalendarDays,
  },
  {
    kind: "interval",
    title: "Interval",
    description: "Run every N minutes.",
    icon: RefreshCw,
  },
  {
    kind: "clickhouse_watch",
    title: "ClickHouse Watch",
    description: "Trigger from a SQL result change or new rows.",
    icon: Database,
  },
  {
    kind: "file_watch",
    title: "File Watch",
    description: "Trigger when new files arrive in a directory.",
    icon: FolderOpen,
  },
] as const;

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

export function PlanningModal({
  isOpen,
  onClose,
  draft,
  editingPlanId,
  planningState,
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
}: PlanningModalProps) {
  if (!isOpen || typeof document === "undefined") return null;

  const updateDraft = (next: Partial<CrewPlanDraft>) => {
    onDraftChange({
      ...draft,
      ...next,
    });
  };

  const updateTrigger = (next: Partial<CrewPlanDraft["trigger"]>) => {
    onDraftChange({
      ...draft,
      trigger: {
        ...draft.trigger,
        ...next,
      },
    });
  };

  const toggleAgent = (role: AgentRole) => {
    const hasRole = draft.agents.includes(role);
    onDraftChange({
      ...draft,
      agents: hasRole
        ? draft.agents.filter((item) => item !== role)
        : [...draft.agents, role],
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

  const loadPlanIntoForm = (plan: CrewPlan) => {
    onEditPlan(plan);
  };

  const handleSave = async () => {
    await onSavePlan(draft, editingPlanId);
  };

  const planCount = planningState.plans.length;
  const runCount = planningState.runs.length;

  return createPortal(
    <>
      <div
        className="fixed inset-0 z-[100] bg-black/45 backdrop-blur-sm"
        onClick={onClose}
      />
      <div className="pointer-events-none fixed inset-0 z-[101] overflow-y-auto p-4">
        <div className="flex min-h-full items-center justify-center py-4">
          <div
            role="dialog"
            aria-modal="true"
            className="pointer-events-auto w-full max-w-[1280px] max-h-[calc(100vh-2rem)] overflow-hidden rounded-[2rem] border border-white/20 bg-[#f8f8f6] shadow-2xl shadow-black/30 dark:bg-[#101115]"
            onClick={(event) => event.stopPropagation()}
          >
        <div className="flex items-center justify-between gap-4 px-6 py-5 border-b border-gray-200/70 dark:border-gray-800/80 bg-white/80 dark:bg-black/20">
          <div>
            <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.2em] text-gray-500 dark:text-gray-400">
              <CheckCircle2 className="w-3.5 h-3.5" />
              CrewAI - Planning
            </div>
            <h2 className="mt-1 text-xl font-semibold text-gray-900 dark:text-gray-100">
              Schedule existing agents with fixed time or event triggers
            </h2>
            <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
              {planCount} saved plan(s) · {runCount} recent run(s)
            </p>
          </div>

          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => void onRefresh()}
              disabled={isBusy}
              className="inline-flex items-center gap-2 px-3 py-2 rounded-xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors disabled:opacity-60"
            >
              <RefreshCw className={cn("w-4 h-4", isBusy && "animate-spin")} />
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
          <div className="grid lg:grid-cols-[1.3fr,0.9fr] gap-6 p-6">
            <div className="space-y-6">
              {error && (
                <div className="rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
                  {error}
                </div>
              )}

              <div className="rounded-[1.75rem] border border-gray-200/80 dark:border-gray-800/70 bg-white/85 dark:bg-gray-900/60 p-5 shadow-sm">
                <div className="flex items-center justify-between gap-3 mb-4">
                  <div>
                    <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">
                      {editingPlanId ? "Edit Planning Job" : "New Planning Job"}
                    </h3>
                    <p className="text-sm text-gray-500 dark:text-gray-400">
                      Build a reusable automation for one or more existing agents.
                    </p>
                  </div>
                  <button
                    type="button"
                    onClick={onStartNewDraft}
                    className="inline-flex items-center gap-2 px-3 py-2 rounded-xl border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800 text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
                  >
                    Start fresh
                  </button>
                </div>

                <div className="grid md:grid-cols-2 gap-4">
                  <label className="space-y-2 md:col-span-2">
                    <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">Plan Name</span>
                    <input
                      type="text"
                      value={draft.name}
                      onChange={(e) => updateDraft({ name: e.target.value })}
                      placeholder="Morning anomaly monitor"
                      className="w-full rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-blue-400"
                    />
                  </label>

                  <label className="space-y-2 md:col-span-2">
                    <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">Objective / Prompt</span>
                    <textarea
                      value={draft.prompt}
                      onChange={(e) => updateDraft({ prompt: e.target.value })}
                      placeholder="Describe exactly what the scheduled agents should do when the trigger fires."
                      rows={4}
                      className="w-full rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-blue-400 resize-y"
                    />
                  </label>

                  <label className="space-y-2">
                    <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">Status</span>
                    <select
                      value={draft.status}
                      onChange={(e) => updateDraft({ status: e.target.value as CrewPlanDraft["status"] })}
                      className="w-full rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-blue-400"
                    >
                      <option value="active">Active</option>
                      <option value="paused">Paused</option>
                    </select>
                  </label>

                  <label className="space-y-2">
                    <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">Time Zone</span>
                    <input
                      type="text"
                      value={draft.trigger.timezone}
                      onChange={(e) => updateTrigger({ timezone: e.target.value })}
                      placeholder="Europe/Paris"
                      className="w-full rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-blue-400"
                    />
                  </label>
                </div>
              </div>

              <div className="rounded-[1.75rem] border border-gray-200/80 dark:border-gray-800/70 bg-white/85 dark:bg-gray-900/60 p-5 shadow-sm">
                <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">Existing Agents</h3>
                <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                  Select one or more existing agents that should run when the plan is triggered.
                </p>
                <div className="grid md:grid-cols-2 gap-3 mt-4">
                  {AGENT_OPTIONS.map((agent) => {
                    const Icon = agent.icon;
                    const isSelected = draft.agents.includes(agent.role);
                    return (
                      <button
                        key={agent.role}
                        type="button"
                        onClick={() => toggleAgent(agent.role)}
                        className={cn(
                          "rounded-2xl border px-4 py-4 text-left transition-all",
                          isSelected
                            ? "border-transparent bg-gray-950 text-white shadow-lg"
                            : "border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 text-gray-900 dark:text-gray-100 hover:border-gray-300 dark:hover:border-gray-600"
                        )}
                      >
                        <div className="flex items-center gap-3">
                          <div className={cn("flex h-11 w-11 items-center justify-center rounded-2xl bg-gradient-to-br text-white", agent.accent)}>
                            <Icon className="w-5 h-5" />
                          </div>
                          <div className="min-w-0">
                            <div className="text-sm font-semibold">{agent.label}</div>
                            <div className={cn("text-xs mt-1", isSelected ? "text-white/70" : "text-gray-500 dark:text-gray-400")}>
                              {agent.description}
                            </div>
                          </div>
                        </div>
                      </button>
                    );
                  })}
                </div>
              </div>

              <div className="rounded-[1.75rem] border border-gray-200/80 dark:border-gray-800/70 bg-white/85 dark:bg-gray-900/60 p-5 shadow-sm">
                <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">Trigger Type</h3>
                <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                  Choose between a fixed schedule or an event-driven automation.
                </p>

                <div className="grid md:grid-cols-2 xl:grid-cols-3 gap-3 mt-4">
                  {TRIGGER_OPTIONS.map((option) => {
                    const Icon = option.icon;
                    const isSelected = draft.trigger.kind === option.kind;
                    return (
                      <button
                        key={option.kind}
                        type="button"
                        onClick={() => updateTrigger({ kind: option.kind })}
                        className={cn(
                          "rounded-2xl border px-4 py-4 text-left transition-all",
                          isSelected
                            ? "border-blue-400 bg-blue-50 dark:bg-blue-950/40 shadow-sm"
                            : "border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 hover:border-gray-300 dark:hover:border-gray-600"
                        )}
                      >
                        <div className="flex items-center gap-3">
                          <div className={cn(
                            "flex h-10 w-10 items-center justify-center rounded-2xl",
                            isSelected
                              ? "bg-blue-600 text-white"
                              : "bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-300"
                          )}>
                            <Icon className="w-5 h-5" />
                          </div>
                          <div>
                            <div className="text-sm font-semibold text-gray-900 dark:text-gray-100">{option.title}</div>
                            <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">{option.description}</div>
                          </div>
                        </div>
                      </button>
                    );
                  })}
                </div>

                <div className="mt-5 grid md:grid-cols-2 gap-4">
                  {draft.trigger.kind === "once" && (
                    <label className="space-y-2 md:col-span-2">
                      <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">Run At</span>
                      <input
                        type="datetime-local"
                        value={draft.trigger.oneTimeAt}
                        onChange={(e) => updateTrigger({ oneTimeAt: e.target.value })}
                        className="w-full rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-blue-400"
                      />
                    </label>
                  )}

                  {draft.trigger.kind === "daily" && (
                    <label className="space-y-2">
                      <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">Time Of Day</span>
                      <input
                        type="time"
                        value={draft.trigger.timeOfDay}
                        onChange={(e) => updateTrigger({ timeOfDay: e.target.value })}
                        className="w-full rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-blue-400"
                      />
                    </label>
                  )}

                  {draft.trigger.kind === "weekly" && (
                    <>
                      <label className="space-y-2">
                        <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">Time Of Day</span>
                        <input
                          type="time"
                          value={draft.trigger.timeOfDay}
                          onChange={(e) => updateTrigger({ timeOfDay: e.target.value })}
                          className="w-full rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-blue-400"
                        />
                      </label>
                      <div className="space-y-2 md:col-span-2">
                        <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">Weekdays</span>
                        <div className="flex flex-wrap gap-2">
                          {WEEKDAYS.map((day) => (
                            <button
                              key={day}
                              type="button"
                              onClick={() => toggleWeekday(day)}
                              className={cn(
                                "px-3 py-2 rounded-xl border text-sm font-medium transition-colors",
                                draft.trigger.weekdays.includes(day)
                                  ? "border-blue-400 bg-blue-50 dark:bg-blue-950/40 text-blue-700 dark:text-blue-200"
                                  : "border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 text-gray-600 dark:text-gray-300"
                              )}
                            >
                              {day.toUpperCase()}
                            </button>
                          ))}
                        </div>
                      </div>
                    </>
                  )}

                  {draft.trigger.kind === "interval" && (
                    <label className="space-y-2">
                      <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">Interval (minutes)</span>
                      <input
                        type="number"
                        min={1}
                        value={draft.trigger.intervalMinutes}
                        onChange={(e) => updateTrigger({ intervalMinutes: parseInt(e.target.value, 10) || 1 })}
                        className="w-full rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-blue-400"
                      />
                    </label>
                  )}

                  {draft.trigger.kind === "clickhouse_watch" && (
                    <>
                      <label className="space-y-2 md:col-span-2">
                        <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">Watch SQL</span>
                        <textarea
                          value={draft.trigger.watchSql}
                          onChange={(e) => updateTrigger({ watchSql: e.target.value })}
                          rows={5}
                          placeholder="SELECT * FROM events WHERE created_at > now() - INTERVAL 5 MINUTE"
                          className="w-full rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm font-mono text-gray-900 dark:text-gray-100 outline-none focus:border-blue-400 resize-y"
                        />
                      </label>
                      <label className="space-y-2">
                        <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">Watch Mode</span>
                        <select
                          value={draft.trigger.watchMode}
                          onChange={(e) => updateTrigger({ watchMode: e.target.value as CrewPlanDraft["trigger"]["watchMode"] })}
                          className="w-full rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-blue-400"
                        >
                          <option value="result_changes">Trigger when the result changes</option>
                          <option value="returns_rows">Trigger when new rows appear</option>
                          <option value="count_increases">Trigger when the numeric count increases</option>
                        </select>
                      </label>
                      <label className="space-y-2">
                        <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">Polling (minutes)</span>
                        <input
                          type="number"
                          min={1}
                          value={draft.trigger.pollMinutes}
                          onChange={(e) => updateTrigger({ pollMinutes: parseInt(e.target.value, 10) || 1 })}
                          className="w-full rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-blue-400"
                        />
                      </label>
                    </>
                  )}

                  {draft.trigger.kind === "file_watch" && (
                    <>
                      <label className="space-y-2 md:col-span-2">
                        <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">Directory</span>
                        <input
                          type="text"
                          value={draft.trigger.directory}
                          onChange={(e) => updateTrigger({ directory: e.target.value })}
                          placeholder="/Users/mathieumasson/inbox"
                          className="w-full rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-blue-400"
                        />
                      </label>
                      <label className="space-y-2">
                        <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">Pattern</span>
                        <input
                          type="text"
                          value={draft.trigger.pattern}
                          onChange={(e) => updateTrigger({ pattern: e.target.value || "*" })}
                          placeholder="*.csv"
                          className="w-full rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-blue-400"
                        />
                      </label>
                      <label className="space-y-2">
                        <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">Polling (minutes)</span>
                        <input
                          type="number"
                          min={1}
                          value={draft.trigger.pollMinutes}
                          onChange={(e) => updateTrigger({ pollMinutes: parseInt(e.target.value, 10) || 1 })}
                          className="w-full rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-blue-400"
                        />
                      </label>
                      <label className="inline-flex items-center gap-3 text-sm text-gray-700 dark:text-gray-200 md:col-span-2">
                        <input
                          type="checkbox"
                          checked={draft.trigger.recursive}
                          onChange={(e) => updateTrigger({ recursive: e.target.checked })}
                          className="w-4 h-4 rounded border-gray-300 dark:border-gray-600 text-blue-600"
                        />
                        Watch subdirectories too
                      </label>
                    </>
                  )}
                </div>
              </div>

              <div className="flex flex-wrap items-center justify-between gap-3">
                <div className="text-sm text-gray-500 dark:text-gray-400">
                  The planner always uses the local LLM configured in RAGnarok.
                </div>
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    onClick={onClose}
                    className="px-4 py-3 rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
                  >
                    Close
                  </button>
                  <button
                    type="button"
                    onClick={() => void handleSave()}
                    disabled={isBusy}
                    className="px-5 py-3 rounded-2xl bg-black text-white text-sm font-medium hover:bg-gray-800 transition-colors disabled:opacity-60"
                  >
                    {editingPlanId ? "Save changes" : "Save planning job"}
                  </button>
                </div>
              </div>
            </div>

            <div className="space-y-6">
              <div className="rounded-[1.75rem] border border-gray-200/80 dark:border-gray-800/70 bg-white/85 dark:bg-gray-900/60 p-5 shadow-sm">
                <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">Saved Plans</h3>
                <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                  Edit, pause, run, or delete existing planning jobs.
                </p>
                <div className="mt-4 space-y-3 max-h-[430px] overflow-y-auto pr-1">
                  {planningState.plans.length === 0 && (
                    <div className="rounded-2xl border border-dashed border-gray-200 dark:border-gray-700 px-4 py-6 text-sm text-gray-500 dark:text-gray-400">
                      No saved plan yet.
                    </div>
                  )}

                  {planningState.plans.map((plan) => (
                    <div key={plan.id} className="rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 p-4">
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="text-sm font-semibold text-gray-900 dark:text-gray-100">
                            {plan.name || "Untitled plan"}
                          </div>
                          <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                            {plan.agents.join(", ") || "No agents"}
                          </div>
                        </div>
                        <span className={cn("inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-medium", statusTone(plan.status))}>
                          {plan.status}
                        </span>
                      </div>

                      <div className="mt-3 space-y-1 text-xs text-gray-500 dark:text-gray-400">
                        <div>Trigger: {(plan.trigger || {}).kind}</div>
                        <div>Next run: {formatDateLabel(plan.nextRunAt)}</div>
                        <div>Last run: {formatDateLabel(plan.lastRunAt)}</div>
                      </div>

                      {plan.lastSummary && (
                        <p className="mt-3 text-xs leading-relaxed text-gray-600 dark:text-gray-300 line-clamp-4">
                          {plan.lastSummary.replace(/^##\s+/gm, "").slice(0, 240)}
                        </p>
                      )}

                      <div className="mt-4 flex flex-wrap gap-2">
                        <button
                          type="button"
                          onClick={() => loadPlanIntoForm(plan)}
                          className="px-3 py-2 rounded-xl border border-gray-200 dark:border-gray-700 text-xs font-medium text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-900 transition-colors"
                        >
                          Edit
                        </button>
                        <button
                          type="button"
                          onClick={() => void onTogglePlanStatus(plan)}
                          className="inline-flex items-center gap-1.5 px-3 py-2 rounded-xl border border-gray-200 dark:border-gray-700 text-xs font-medium text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-900 transition-colors"
                        >
                          {plan.status === "active" ? <Pause className="w-3.5 h-3.5" /> : <Play className="w-3.5 h-3.5" />}
                          {plan.status === "active" ? "Pause" : "Resume"}
                        </button>
                        <button
                          type="button"
                          onClick={() => void onRunPlan(plan.id)}
                          className="inline-flex items-center gap-1.5 px-3 py-2 rounded-xl border border-gray-200 dark:border-gray-700 text-xs font-medium text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-900 transition-colors"
                        >
                          <Play className="w-3.5 h-3.5" />
                          Run now
                        </button>
                        <button
                          type="button"
                          onClick={() => void onDeletePlan(plan.id)}
                          className="inline-flex items-center gap-1.5 px-3 py-2 rounded-xl border border-red-200 text-xs font-medium text-red-700 hover:bg-red-50 transition-colors"
                        >
                          <Trash2 className="w-3.5 h-3.5" />
                          Delete
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="rounded-[1.75rem] border border-gray-200/80 dark:border-gray-800/70 bg-white/85 dark:bg-gray-900/60 p-5 shadow-sm">
                <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">Recent Runs</h3>
                <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                  Review the last executions performed by the planner.
                </p>
                <div className="mt-4 space-y-3 max-h-[360px] overflow-y-auto pr-1">
                  {planningState.runs.length === 0 && (
                    <div className="rounded-2xl border border-dashed border-gray-200 dark:border-gray-700 px-4 py-6 text-sm text-gray-500 dark:text-gray-400">
                      No execution yet.
                    </div>
                  )}
                  {planningState.runs.map((run) => (
                    <div key={run.id} className="rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 p-4">
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="text-sm font-semibold text-gray-900 dark:text-gray-100">{run.planName}</div>
                          <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                            {run.triggerLabel || run.triggerKind}
                          </div>
                        </div>
                        <span className={cn("inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-medium", statusTone(run.status))}>
                          {run.status}
                        </span>
                      </div>
                      <div className="mt-3 text-xs text-gray-500 dark:text-gray-400">
                        Started: {formatDateLabel(run.startedAt)}
                      </div>
                      {run.summary && (
                        <p className="mt-3 text-xs leading-relaxed text-gray-600 dark:text-gray-300 line-clamp-5">
                          {run.summary.replace(/^##\s+/gm, "").slice(0, 320)}
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
    </div>
    </>,
    document.body
  );
}
