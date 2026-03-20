import React, { useEffect } from "react";
import { createPortal } from "react-dom";
import {
  Bot,
  CalendarClock,
  Database,
  FolderOpen,
  Pause,
  Play,
  RefreshCw,
  ShieldCheck,
  Sparkles,
  Trash2,
  Workflow,
  X,
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
}> = [
  {
    role: "manager",
    label: "Agent Manager",
    description: "Operational synthesis and orchestration.",
    icon: Bot,
  },
  {
    role: "clickhouse_query",
    label: "Clickhouse SQL",
    description: "Safe SQL generation and execution on ClickHouse.",
    icon: Database,
  },
  {
    role: "file_management",
    label: "File management",
    description: "Filesystem actions with confirmations and previews.",
    icon: FolderOpen,
  },
];

const TRIGGER_OPTIONS = [
  {
    kind: "once",
    title: "One time",
    description: "Run once on a specific date and time.",
    icon: CalendarClock,
  },
  {
    kind: "daily",
    title: "Daily",
    description: "Run every day at a fixed time.",
    icon: CalendarClock,
  },
  {
    kind: "weekly",
    title: "Weekly",
    description: "Run on selected weekdays.",
    icon: CalendarClock,
  },
  {
    kind: "interval",
    title: "Interval",
    description: "Run every N minutes.",
    icon: RefreshCw,
  },
  {
    kind: "clickhouse_watch",
    title: "ClickHouse watch",
    description: "Run when a ClickHouse result changes or returns rows.",
    icon: Database,
  },
  {
    kind: "file_watch",
    title: "File watch",
    description: "Run when new files appear in a directory.",
    icon: FolderOpen,
  },
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

function agentLabel(role: AgentRole) {
  return AGENT_OPTIONS.find((agent) => agent.role === role)?.label || role;
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
  useEffect(() => {
    if (!isOpen) return undefined;

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onClose();
      }
    };

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [isOpen, onClose]);

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
    updateDraft({
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

  const handleSave = async () => {
    await onSavePlan(draft, editingPlanId);
  };

  const selectedAgents = AGENT_OPTIONS.filter((agent) => draft.agents.includes(agent.role));
  const activeTrigger = TRIGGER_OPTIONS.find((option) => option.kind === draft.trigger.kind) ?? TRIGGER_OPTIONS[0];
  const readinessChecks = [
    { label: "Objective", ok: draft.prompt.trim().length > 0 },
    { label: "Agents", ok: draft.agents.length > 0 },
    { label: "Trigger", ok: Boolean(triggerSummary(draft.trigger)) },
  ];

  return createPortal(
    <>
      <div
        className="fixed inset-0 z-[110] bg-black/45 backdrop-blur-sm"
        onClick={onClose}
      />

      <div className="pointer-events-none fixed inset-0 z-[111] overflow-y-auto p-4">
        <div className="flex min-h-full items-center justify-center py-4">
          <div
            role="dialog"
            aria-modal="true"
            aria-label="LangGraph planning"
            className="pointer-events-auto relative flex h-[min(94vh,980px)] w-full max-w-[1420px] flex-col overflow-hidden rounded-[2.1rem] border border-white/30 bg-[#f7f7f4] shadow-[0_30px_80px_rgba(15,23,42,0.35)] dark:border-white/10 dark:bg-[#101115]"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="flex items-start justify-between gap-4 border-b border-gray-200/80 bg-white/75 px-6 py-5 backdrop-blur-xl dark:border-gray-800/80 dark:bg-black/20">
            <div>
              <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.22em] text-emerald-600 dark:text-emerald-300">
                <Workflow className="h-3.5 w-3.5" />
                LangGraph Planning
              </div>
              <h2 className="mt-1 text-xl font-semibold text-gray-900 dark:text-gray-100">
                Schedule existing agents with fixed time or event triggers
              </h2>
              <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                Native controls for Windows reliability, plus LangGraph orchestration for draft parsing and scheduled execution.
              </p>
              <div className="mt-3 flex flex-wrap items-center gap-2 text-[11px] text-gray-600 dark:text-gray-300">
                <span className="rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1 dark:border-emerald-800/70 dark:bg-emerald-900/30">
                  {selectedAgents.length} agent{selectedAgents.length === 1 ? "" : "s"} selected
                </span>
                <span className="rounded-full border border-sky-200 bg-sky-50 px-3 py-1 dark:border-sky-800/70 dark:bg-sky-900/30">
                  Trigger: {activeTrigger.title}
                </span>
              </div>
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

          <div className="grid min-h-0 flex-1 gap-0 xl:grid-cols-[1.08fr,0.92fr]">
            <form
              className="min-h-0 overflow-y-auto border-r border-gray-200/80 px-6 py-6 dark:border-gray-800/80"
              onSubmit={(event) => {
                event.preventDefault();
                void handleSave();
              }}
            >
              <div className="space-y-6">
                {error && (
                  <div className="rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
                    {error}
                  </div>
                )}

                <section className="rounded-[1.6rem] border border-white/70 bg-white/80 p-5 shadow-sm dark:border-gray-800/80 dark:bg-gray-900/65">
                  <div className="mb-4 flex items-center justify-between gap-3">
                    <div>
                      <div className="mb-2 inline-flex items-center gap-2 rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1 text-[11px] font-medium uppercase tracking-[0.18em] text-emerald-700 dark:border-emerald-800/60 dark:bg-emerald-900/20 dark:text-emerald-200">
                        <Sparkles className="h-3.5 w-3.5" />
                        Step 1
                      </div>
                      <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">
                        Planning job
                      </h3>
                      <p className="text-sm text-gray-500 dark:text-gray-400">
                        Name the automation and describe the recurring objective in plain English.
                      </p>
                    </div>
                    <button
                      type="button"
                      onClick={onStartNewDraft}
                      className="rounded-xl border border-gray-200 bg-gray-50 px-3 py-2 text-sm text-gray-700 transition-colors hover:bg-gray-100 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200 dark:hover:bg-gray-700"
                    >
                      Start fresh
                    </button>
                  </div>

                  <div className="grid gap-4 md:grid-cols-2">
                    <label className="block md:col-span-2">
                      <span className="mb-2 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
                        Plan name
                      </span>
                      <input
                        type="text"
                        value={draft.name}
                        onChange={(event) => updateDraft({ name: event.target.value })}
                        placeholder="Morning anomaly monitor"
                        className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-emerald-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                      />
                    </label>

                    <label className="block md:col-span-2">
                      <span className="mb-2 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
                        Objective / prompt
                      </span>
                      <textarea
                        value={draft.prompt}
                        onChange={(event) => updateDraft({ prompt: event.target.value })}
                        rows={5}
                        placeholder="Describe the recurring workflow in plain English."
                        className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-emerald-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                      />
                    </label>

                    <label className="block">
                      <span className="mb-2 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
                        Status
                      </span>
                      <select
                        value={draft.status}
                        onChange={(event) => updateDraft({ status: event.target.value as CrewPlanDraft["status"] })}
                        className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-emerald-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                      >
                        <option value="active">Active</option>
                        <option value="paused">Paused</option>
                      </select>
                    </label>

                    <label className="block">
                      <span className="mb-2 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
                        Time zone
                      </span>
                      <input
                        type="text"
                        value={draft.trigger.timezone}
                        onChange={(event) => updateTrigger({ timezone: event.target.value })}
                        placeholder="Europe/Paris"
                        className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-emerald-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                      />
                    </label>
                  </div>
                </section>

                <section className="rounded-[1.6rem] border border-white/70 bg-white/80 p-5 shadow-sm dark:border-gray-800/80 dark:bg-gray-900/65">
                  <div className="mb-3 inline-flex items-center gap-2 rounded-full border border-violet-200 bg-violet-50 px-3 py-1 text-[11px] font-medium uppercase tracking-[0.18em] text-violet-700 dark:border-violet-800/60 dark:bg-violet-900/20 dark:text-violet-200">
                    <Bot className="h-3.5 w-3.5" />
                    Step 2
                  </div>
                  <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">
                    Agents to run
                  </h3>
                  <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                    Select one or more existing agents. The scheduler keeps their current capabilities unchanged.
                  </p>

                  <div className="mt-4 grid gap-3 md:grid-cols-2">
                    {AGENT_OPTIONS.map((agent) => {
                      const Icon = agent.icon;
                      const checked = draft.agents.includes(agent.role);
                      return (
                        <label
                          key={agent.role}
                          className={cn(
                            "flex cursor-pointer items-start gap-3 rounded-2xl border px-4 py-4 transition-colors",
                            checked
                              ? "border-emerald-300 bg-emerald-50 dark:border-emerald-600 dark:bg-emerald-900/20"
                              : "border-gray-200 bg-white dark:border-gray-700 dark:bg-gray-950"
                          )}
                        >
                          <input
                            type="checkbox"
                            checked={checked}
                            onChange={() => toggleAgent(agent.role)}
                            className="mt-1 h-4 w-4 rounded border-gray-300 text-emerald-600 focus:ring-emerald-500"
                          />
                          <div className="min-w-0 flex-1">
                            <div className="flex items-center gap-2 text-sm font-semibold text-gray-900 dark:text-gray-100">
                              <Icon className="h-4 w-4 text-emerald-600 dark:text-emerald-300" />
                              {agent.label}
                            </div>
                            <p className="mt-1 text-xs leading-relaxed text-gray-500 dark:text-gray-400">
                              {agent.description}
                            </p>
                          </div>
                        </label>
                      );
                    })}
                  </div>
                </section>

                <section className="rounded-[1.6rem] border border-white/70 bg-white/80 p-5 shadow-sm dark:border-gray-800/80 dark:bg-gray-900/65">
                  <div className="mb-3 inline-flex items-center gap-2 rounded-full border border-sky-200 bg-sky-50 px-3 py-1 text-[11px] font-medium uppercase tracking-[0.18em] text-sky-700 dark:border-sky-800/60 dark:bg-sky-900/20 dark:text-sky-200">
                    <CalendarClock className="h-3.5 w-3.5" />
                    Step 3
                  </div>
                  <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">
                    Trigger configuration
                  </h3>
                  <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                    Choose how the automation starts, then fill the matching parameters using native browser inputs only.
                  </p>

                  <div className="mt-4 grid gap-3 md:grid-cols-2">
                    {TRIGGER_OPTIONS.map((option) => {
                      const Icon = option.icon;
                      return (
                      <label
                        key={option.kind}
                        className={cn(
                          "flex cursor-pointer items-start gap-3 rounded-2xl border px-4 py-4 transition-colors",
                          draft.trigger.kind === option.kind
                            ? "border-sky-300 bg-sky-50 dark:border-sky-600 dark:bg-sky-900/20"
                            : "border-gray-200 bg-white dark:border-gray-700 dark:bg-gray-950"
                        )}
                      >
                        <input
                          type="radio"
                          name="planning-trigger-kind"
                          checked={draft.trigger.kind === option.kind}
                          onChange={() => updateTrigger({ kind: option.kind })}
                          className="mt-1 h-4 w-4 border-gray-300 text-sky-600 focus:ring-sky-500"
                        />
                        <div>
                          <div className="flex items-center gap-2 text-sm font-semibold text-gray-900 dark:text-gray-100">
                            <Icon className="h-4 w-4 text-sky-600 dark:text-sky-300" />
                            {option.title}
                          </div>
                          <p className="mt-1 text-xs leading-relaxed text-gray-500 dark:text-gray-400">
                            {option.description}
                          </p>
                        </div>
                      </label>
                    )})}
                  </div>

                  <div className="mt-5 grid gap-4 md:grid-cols-2">
                    {draft.trigger.kind === "once" && (
                      <label className="block md:col-span-2">
                        <span className="mb-2 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
                          Run at
                        </span>
                        <input
                          type="datetime-local"
                          value={draft.trigger.oneTimeAt}
                          onChange={(event) => updateTrigger({ oneTimeAt: event.target.value })}
                          className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                        />
                      </label>
                    )}

                    {draft.trigger.kind === "daily" && (
                      <label className="block">
                        <span className="mb-2 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
                          Time of day
                        </span>
                        <input
                          type="time"
                          value={draft.trigger.timeOfDay}
                          onChange={(event) => updateTrigger({ timeOfDay: event.target.value })}
                          className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                        />
                      </label>
                    )}

                    {draft.trigger.kind === "weekly" && (
                      <>
                        <label className="block">
                          <span className="mb-2 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
                            Time of day
                          </span>
                          <input
                            type="time"
                            value={draft.trigger.timeOfDay}
                            onChange={(event) => updateTrigger({ timeOfDay: event.target.value })}
                            className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                          />
                        </label>

                        <fieldset className="md:col-span-2">
                          <legend className="mb-2 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
                            Weekdays
                          </legend>
                          <div className="grid grid-cols-2 gap-2 sm:grid-cols-4 lg:grid-cols-7">
                            {WEEKDAYS.map((day) => {
                              const checked = draft.trigger.weekdays.includes(day);
                              return (
                                <label
                                  key={day}
                                  className={cn(
                                    "flex cursor-pointer items-center gap-2 rounded-xl border px-3 py-2 text-sm transition-colors",
                                    checked
                                      ? "border-sky-300 bg-sky-50 text-sky-800 dark:border-sky-600 dark:bg-sky-900/20 dark:text-sky-100"
                                      : "border-gray-200 bg-white text-gray-700 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-200"
                                  )}
                                >
                                  <input
                                    type="checkbox"
                                    checked={checked}
                                    onChange={() => toggleWeekday(day)}
                                    className="h-4 w-4 rounded border-gray-300 text-sky-600 focus:ring-sky-500"
                                  />
                                  {day.toUpperCase()}
                                </label>
                              );
                            })}
                          </div>
                        </fieldset>
                      </>
                    )}

                    {draft.trigger.kind === "interval" && (
                      <label className="block">
                        <span className="mb-2 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
                          Interval (minutes)
                        </span>
                        <input
                          type="number"
                          min={1}
                          value={draft.trigger.intervalMinutes}
                          onChange={(event) => updateTrigger({ intervalMinutes: parseInt(event.target.value, 10) || 1 })}
                          className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                        />
                      </label>
                    )}

                    {draft.trigger.kind === "clickhouse_watch" && (
                      <>
                        <label className="block md:col-span-2">
                          <span className="mb-2 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
                            Watch SQL
                          </span>
                          <textarea
                            value={draft.trigger.watchSql}
                            onChange={(event) => updateTrigger({ watchSql: event.target.value })}
                            rows={5}
                            placeholder="SELECT count(*) AS new_rows FROM events WHERE created_at >= now() - INTERVAL 5 MINUTE"
                            className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 font-mono text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                          />
                        </label>

                        <label className="block">
                          <span className="mb-2 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
                            Watch mode
                          </span>
                          <select
                            value={draft.trigger.watchMode}
                            onChange={(event) => updateTrigger({ watchMode: event.target.value as CrewPlanDraft["trigger"]["watchMode"] })}
                            className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                          >
                            <option value="result_changes">Trigger when the result changes</option>
                            <option value="returns_rows">Trigger when rows are returned</option>
                            <option value="count_increases">Trigger when the count increases</option>
                          </select>
                        </label>

                        <label className="block">
                          <span className="mb-2 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
                            Polling (minutes)
                          </span>
                          <input
                            type="number"
                            min={1}
                            value={draft.trigger.pollMinutes}
                            onChange={(event) => updateTrigger({ pollMinutes: parseInt(event.target.value, 10) || 1 })}
                            className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                          />
                        </label>
                      </>
                    )}

                    {draft.trigger.kind === "file_watch" && (
                      <>
                        <label className="block md:col-span-2">
                          <span className="mb-2 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
                            Directory
                          </span>
                          <input
                            type="text"
                            value={draft.trigger.directory}
                            onChange={(event) => updateTrigger({ directory: event.target.value })}
                            placeholder="/Users/mathieumasson/inbox"
                            className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                          />
                        </label>

                        <label className="block">
                          <span className="mb-2 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
                            Pattern
                          </span>
                          <input
                            type="text"
                            value={draft.trigger.pattern}
                            onChange={(event) => updateTrigger({ pattern: event.target.value || "*" })}
                            placeholder="*.csv"
                            className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                          />
                        </label>

                        <label className="block">
                          <span className="mb-2 block text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
                            Polling (minutes)
                          </span>
                          <input
                            type="number"
                            min={1}
                            value={draft.trigger.pollMinutes}
                            onChange={(event) => updateTrigger({ pollMinutes: parseInt(event.target.value, 10) || 1 })}
                            className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition-colors focus:border-sky-400 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-100"
                          />
                        </label>

                        <label className="flex cursor-pointer items-center gap-3 rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-700 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-200 md:col-span-2">
                          <input
                            type="checkbox"
                            checked={draft.trigger.recursive}
                            onChange={(event) => updateTrigger({ recursive: event.target.checked })}
                            className="h-4 w-4 rounded border-gray-300 text-sky-600 focus:ring-sky-500"
                          />
                          Watch subdirectories too
                        </label>
                      </>
                    )}
                  </div>
                </section>
              </div>
            </form>

            <aside className="min-h-0 overflow-y-auto bg-[linear-gradient(180deg,rgba(255,255,255,0.62),rgba(246,248,250,0.90))] px-6 py-6 dark:bg-[linear-gradient(180deg,rgba(18,20,26,0.85),rgba(10,12,17,0.92))]">
              <div className="space-y-6">
                <section className="rounded-[1.6rem] border border-white/70 bg-white/80 p-5 shadow-sm dark:border-gray-800/80 dark:bg-gray-900/65">
                  <div className="mb-3 inline-flex items-center gap-2 rounded-full border border-amber-200 bg-amber-50 px-3 py-1 text-[11px] font-medium uppercase tracking-[0.18em] text-amber-700 dark:border-amber-800/60 dark:bg-amber-900/20 dark:text-amber-200">
                    <ShieldCheck className="h-3.5 w-3.5" />
                    Review
                  </div>
                  <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">
                    Launch summary
                  </h3>
                  <div className="mt-4 space-y-3 text-sm">
                    <div>
                      <div className="text-[11px] uppercase tracking-wide text-gray-500 dark:text-gray-400">Selected agents</div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {selectedAgents.length > 0 ? selectedAgents.map((agent) => (
                          <span
                            key={agent.role}
                            className="rounded-full border border-violet-200 bg-violet-50 px-3 py-1 text-xs text-violet-800 dark:border-violet-800/70 dark:bg-violet-900/25 dark:text-violet-200"
                          >
                            {agent.label}
                          </span>
                        )) : (
                          <span className="text-gray-500 dark:text-gray-400">No agent selected yet</span>
                        )}
                      </div>
                    </div>
                    <div>
                      <div className="text-[11px] uppercase tracking-wide text-gray-500 dark:text-gray-400">Trigger</div>
                      <div className="mt-1 text-gray-900 dark:text-gray-100">
                        {activeTrigger.title} · {triggerSummary(draft.trigger)}
                      </div>
                    </div>
                    <div>
                      <div className="text-[11px] uppercase tracking-wide text-gray-500 dark:text-gray-400">Status</div>
                      <div className="mt-1 text-gray-900 dark:text-gray-100">{draft.status}</div>
                    </div>
                    <div>
                      <div className="text-[11px] uppercase tracking-wide text-gray-500 dark:text-gray-400">Readiness</div>
                      <div className="mt-2 grid gap-2 sm:grid-cols-3">
                        {readinessChecks.map((item) => (
                          <div
                            key={item.label}
                            className={cn(
                              "rounded-2xl border px-3 py-3 text-xs font-medium",
                              item.ok
                                ? "border-emerald-200 bg-emerald-50 text-emerald-800 dark:border-emerald-800/60 dark:bg-emerald-900/20 dark:text-emerald-200"
                                : "border-amber-200 bg-amber-50 text-amber-800 dark:border-amber-800/60 dark:bg-amber-900/20 dark:text-amber-200"
                            )}
                          >
                            {item.label}: {item.ok ? "ready" : "missing"}
                          </div>
                        ))}
                      </div>
                    </div>
                    <div className="rounded-2xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-800 dark:border-emerald-700/50 dark:bg-emerald-900/20 dark:text-emerald-100">
                      LangGraph handles draft understanding and execution orchestration, while this form stays fully native for better Windows compatibility.
                    </div>
                  </div>
                </section>

                <section className="rounded-[1.6rem] border border-white/70 bg-white/80 p-5 shadow-sm dark:border-gray-800/80 dark:bg-gray-900/65">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">
                        Saved plans
                      </h3>
                      <p className="text-sm text-gray-500 dark:text-gray-400">
                        Edit, pause, run, or delete an existing plan.
                      </p>
                    </div>
                    <span className="rounded-full border border-gray-200 bg-gray-50 px-2.5 py-1 text-[11px] font-medium text-gray-600 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-300">
                      {planningState.plans.length}
                    </span>
                  </div>

                  <div className="mt-4 space-y-3">
                    {planningState.plans.length === 0 && (
                      <div className="rounded-2xl border border-dashed border-gray-200 px-4 py-5 text-sm text-gray-500 dark:border-gray-700 dark:text-gray-400">
                        No saved plan yet.
                      </div>
                    )}

                    {planningState.plans.map((plan) => (
                      <div
                        key={plan.id}
                        className="rounded-2xl border border-gray-200 bg-white p-4 dark:border-gray-700 dark:bg-gray-950"
                      >
                        <div className="flex items-start justify-between gap-3">
                          <div className="min-w-0">
                            <div className="text-sm font-semibold text-gray-900 dark:text-gray-100">
                              {plan.name || "Untitled plan"}
                            </div>
                          <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                              {plan.agents.map(agentLabel).join(", ") || "No agents"} · {plan.trigger.kind}
                          </div>
                        </div>
                          <span className={cn("rounded-full border px-2.5 py-1 text-[11px] font-medium", statusTone(plan.status))}>
                            {plan.status}
                          </span>
                        </div>

                        <div className="mt-3 space-y-1 text-xs text-gray-500 dark:text-gray-400">
                          <div>Next run: {formatDateLabel(plan.nextRunAt)}</div>
                          <div>Last run: {formatDateLabel(plan.lastRunAt)}</div>
                        </div>

                        <div className="mt-4 flex flex-wrap gap-2">
                          <button
                            type="button"
                            onClick={() => onEditPlan(plan)}
                            className="rounded-xl border border-gray-200 bg-white px-3 py-2 text-xs font-medium text-gray-700 transition-colors hover:bg-gray-50 dark:border-gray-700 dark:bg-gray-900 dark:text-gray-200 dark:hover:bg-gray-800"
                          >
                            Edit
                          </button>
                          <button
                            type="button"
                            onClick={() => void onTogglePlanStatus(plan)}
                            className="inline-flex items-center gap-1.5 rounded-xl border border-gray-200 bg-white px-3 py-2 text-xs font-medium text-gray-700 transition-colors hover:bg-gray-50 dark:border-gray-700 dark:bg-gray-900 dark:text-gray-200 dark:hover:bg-gray-800"
                          >
                            {plan.status === "active" ? <Pause className="h-3.5 w-3.5" /> : <Play className="h-3.5 w-3.5" />}
                            {plan.status === "active" ? "Pause" : "Resume"}
                          </button>
                          <button
                            type="button"
                            onClick={() => void onRunPlan(plan.id)}
                            className="inline-flex items-center gap-1.5 rounded-xl border border-gray-200 bg-white px-3 py-2 text-xs font-medium text-gray-700 transition-colors hover:bg-gray-50 dark:border-gray-700 dark:bg-gray-900 dark:text-gray-200 dark:hover:bg-gray-800"
                          >
                            <Play className="h-3.5 w-3.5" />
                            Run now
                          </button>
                          <button
                            type="button"
                            onClick={() => void onDeletePlan(plan.id)}
                            className="inline-flex items-center gap-1.5 rounded-xl border border-red-200 bg-red-50 px-3 py-2 text-xs font-medium text-red-700 transition-colors hover:bg-red-100"
                          >
                            <Trash2 className="h-3.5 w-3.5" />
                            Delete
                          </button>
                        </div>
                      </div>
                    ))}
                  </div>
                </section>

                <section className="rounded-[1.6rem] border border-white/70 bg-white/80 p-5 shadow-sm dark:border-gray-800/80 dark:bg-gray-900/65">
                  <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100">
                    Recent runs
                  </h3>
                  <p className="text-sm text-gray-500 dark:text-gray-400">
                    Latest execution snapshots.
                  </p>

                  <div className="mt-4 space-y-3">
                    {planningState.runs.length === 0 && (
                      <div className="rounded-2xl border border-dashed border-gray-200 px-4 py-5 text-sm text-gray-500 dark:border-gray-700 dark:text-gray-400">
                        No execution yet.
                      </div>
                    )}

                    {planningState.runs.slice(0, 8).map((run) => (
                      <div
                        key={run.id}
                        className="rounded-2xl border border-gray-200 bg-white p-4 dark:border-gray-700 dark:bg-gray-950"
                      >
                        <div className="flex items-start justify-between gap-3">
                          <div className="min-w-0">
                            <div className="text-sm font-semibold text-gray-900 dark:text-gray-100">
                              {run.planName}
                            </div>
                            <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                              {run.triggerLabel || run.triggerKind}
                            </div>
                          </div>
                          <span className={cn("rounded-full border px-2.5 py-1 text-[11px] font-medium", statusTone(run.status))}>
                            {run.status}
                          </span>
                        </div>
                        <div className="mt-3 text-xs text-gray-500 dark:text-gray-400">
                          Started: {formatDateLabel(run.startedAt)}
                        </div>
                        {run.summary && (
                          <p className="mt-3 line-clamp-4 text-xs leading-relaxed text-gray-600 dark:text-gray-300">
                            {run.summary.replace(/^##\s+/gm, "").slice(0, 260)}
                          </p>
                        )}
                      </div>
                    ))}
                  </div>
                </section>
              </div>
            </aside>
          </div>

          <div className="flex items-center justify-between gap-3 border-t border-gray-200/80 bg-white/75 px-6 py-5 backdrop-blur-xl dark:border-gray-800/80 dark:bg-black/20">
            <div className="text-sm text-gray-500 dark:text-gray-400">
              Native inputs only: text fields, radios, checkboxes, selects, and a direct save action.
            </div>

            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={onClose}
                className="rounded-2xl border border-gray-200 bg-white px-4 py-2.5 text-sm font-medium text-gray-700 transition-colors hover:bg-gray-50 dark:border-gray-700 dark:bg-gray-900 dark:text-gray-200 dark:hover:bg-gray-800"
              >
                Close
              </button>
              <button
                type="button"
                onClick={() => void handleSave()}
                disabled={isBusy}
                className="rounded-2xl bg-black px-5 py-2.5 text-sm font-medium text-white transition-colors hover:bg-gray-800 disabled:opacity-60"
              >
                {editingPlanId ? "Save changes" : "Save LangGraph job"}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
    </>,
    document.body
  );
}
