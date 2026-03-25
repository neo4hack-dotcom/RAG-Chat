import React, { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import {
  X,
  Trash2,
  Activity,
  Terminal,
  AlertTriangle,
  Target,
  Cpu,
  CheckCircle2,
  FileText,
  Wrench,
} from "lucide-react";

export interface LogEvent {
  id: string;
  ts: string;
  ts_epoch: number;
  kind: "info" | "warning" | "error" | "decision" | "llm" | "tool" | "tool_call" | "sql" | "success";
  agent: string;
  message: string;
  data: Record<string, unknown>;
}

interface AgentConsoleModalProps {
  isOpen: boolean;
  onClose: () => void;
  isDark: boolean;
}

const AGENT_COLORS: Record<string, string> = {
  manager: "bg-orange-500/20 text-orange-600 dark:text-orange-400 border-orange-500/30",
  clickhouse: "bg-cyan-500/20 text-cyan-600 dark:text-cyan-400 border-cyan-500/30",
  data_analyst: "bg-violet-500/20 text-violet-600 dark:text-violet-400 border-violet-500/30",
  auto_ml: "bg-rose-500/20 text-rose-600 dark:text-rose-400 border-rose-500/30",
  data_cleaner: "bg-indigo-500/20 text-indigo-600 dark:text-indigo-400 border-indigo-500/30",
  anonymizer: "bg-zinc-500/20 text-zinc-700 dark:text-zinc-300 border-zinc-500/30",
  file_manager: "bg-emerald-500/20 text-emerald-600 dark:text-emerald-400 border-emerald-500/30",
  pdf_creator: "bg-slate-500/20 text-slate-600 dark:text-slate-400 border-slate-500/30",
  custom_agent: "bg-slate-500/20 text-slate-700 dark:text-slate-300 border-slate-500/30",
  oracle: "bg-amber-500/20 text-amber-600 dark:text-amber-400 border-amber-500/30",
  oracle_analyst: "bg-amber-500/20 text-amber-600 dark:text-amber-400 border-amber-500/30",
  data_quality: "bg-fuchsia-500/20 text-fuchsia-600 dark:text-fuchsia-400 border-fuchsia-500/30",
  rag: "bg-blue-500/20 text-blue-600 dark:text-blue-400 border-blue-500/30",
  mcp: "bg-teal-500/20 text-teal-600 dark:text-teal-400 border-teal-500/30",
  mcp_orchestrator: "bg-teal-500/20 text-teal-700 dark:text-teal-300 border-teal-500/30",
  planner: "bg-emerald-500/20 text-emerald-600 dark:text-emerald-400 border-emerald-500/30",
  system: "bg-gray-500/20 text-gray-600 dark:text-gray-400 border-gray-500/30",
};

const KIND_ICONS: Record<string, React.ReactNode> = {
  info: <Activity className="w-3.5 h-3.5" />,
  warning: <AlertTriangle className="w-3.5 h-3.5" />,
  error: <AlertTriangle className="w-3.5 h-3.5 text-red-500" />,
  decision: <Target className="w-3.5 h-3.5" />,
  llm: <Cpu className="w-3.5 h-3.5" />,
  tool: <Wrench className="w-3.5 h-3.5" />,
  tool_call: <Wrench className="w-3.5 h-3.5" />,
  sql: <FileText className="w-3.5 h-3.5" />,
  success: <CheckCircle2 className="w-3.5 h-3.5 text-emerald-500" />,
};

const KIND_COLORS: Record<string, string> = {
  info: "text-blue-500",
  warning: "text-amber-500",
  error: "text-red-500",
  decision: "text-emerald-500",
  llm: "text-purple-500",
  tool: "text-cyan-500",
  tool_call: "text-cyan-500",
  sql: "text-sky-500",
  success: "text-emerald-500",
};

export function AgentConsoleModal({ isOpen, onClose, isDark }: AgentConsoleModalProps) {
  const [logs, setLogs] = useState<LogEvent[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const [filter, setFilter] = useState<string>("all");
  const [loadError, setLoadError] = useState<string | null>(null);
  const logsEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!isOpen) return;

    let cancelled = false;

    const loadLogs = async () => {
      try {
        const response = await fetch("/api/logs");
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        const payload = await response.json();
        if (cancelled) return;

        const nextLogs = Array.isArray(payload.logs) ? payload.logs.slice(-1000) : [];
        nextLogs.sort((a: LogEvent, b: LogEvent) => a.ts_epoch - b.ts_epoch);
        setLogs(nextLogs);
        setIsConnected(true);
        setLoadError(null);
      } catch (err) {
        console.error("Failed to load logs", err);
        if (cancelled) return;
        setIsConnected(false);
        setLoadError(err instanceof Error ? err.message : "Failed to load logs");
      }
    };

    void loadLogs();
    const intervalId = window.setInterval(() => {
      void loadLogs();
    }, 2500);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
      setIsConnected(false);
    };
  }, [isOpen]);

  useEffect(() => {
    if (!isOpen) return;

    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onClose();
      }
    };

    document.addEventListener("keydown", handleEscape);
    return () => document.removeEventListener("keydown", handleEscape);
  }, [isOpen, onClose]);

  useEffect(() => {
    if (logsEndRef.current) {
      logsEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [logs]);

  if (!isOpen) return null;

  const handleClear = async () => {
    try {
      const response = await fetch("/api/logs/clear", { method: "DELETE" });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      setLogs([]);
      setLoadError(null);
    } catch (err) {
      console.error("Failed to clear logs", err);
      setLoadError(err instanceof Error ? err.message : "Failed to clear logs");
    }
  };

  const filteredLogs = logs.filter((log) => {
    if (filter === "all") return true;
    if (filter === "errors") return log.kind === "error" || log.kind === "warning";
    if (filter === "decisions") return log.kind === "decision";
    if (filter === "llm") return log.kind === "llm" || log.kind === "tool" || log.kind === "tool_call" || log.kind === "sql";
    return true;
  });

  return createPortal(
    <div
      className="fixed inset-0 z-[120] flex items-center justify-center bg-black/40 p-4 backdrop-blur-sm animate-fade-in"
      onMouseDown={onClose}
    >
      <div
        role="dialog"
        aria-modal="true"
        onMouseDown={(event) => event.stopPropagation()}
        className={`relative flex h-[85vh] w-full max-w-5xl flex-col overflow-hidden rounded-3xl border shadow-2xl shadow-black/50 transition-all duration-300 ${isDark ? "border-gray-800 bg-[#0f0f13]/90" : "border-gray-200 bg-white/90"} backdrop-blur-2xl`}
      >
        <div className={`flex shrink-0 items-center justify-between border-b px-6 py-4 ${isDark ? "border-gray-800" : "border-gray-200"}`}>
          <div className="flex items-center gap-3">
            <div className={`rounded-xl bg-gradient-to-tr p-2 text-white shadow-lg ${isDark ? "from-indigo-600 to-purple-600" : "from-indigo-500 to-purple-500"}`}>
              <Terminal className="h-5 w-5" />
            </div>
            <div>
              <h2 className={`text-lg font-semibold tracking-tight ${isDark ? "text-white" : "text-gray-900"}`}>Agent Observability Console</h2>
              <div className="mt-0.5 flex items-center gap-2 text-xs font-medium text-gray-500 dark:text-gray-400">
                <span className={`h-2 w-2 rounded-full ${isConnected ? "bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.8)]" : "bg-red-500"} animate-pulse`} />
                {isConnected ? "Live logs loaded (10min TTL)" : "Polling logs..."}
              </div>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <div className={`mr-2 flex items-center rounded-xl p-1 ${isDark ? "bg-black/50" : "bg-gray-100"}`}>
              <button
                onClick={() => setFilter("all")}
                className={`rounded-lg px-3 py-1.5 text-xs font-medium transition-colors ${filter === "all" ? (isDark ? "bg-gray-800 text-white shadow-sm" : "bg-white text-gray-900 shadow-sm") : "text-gray-500 hover:text-gray-700 dark:hover:text-gray-300"}`}
              >
                All Events
              </button>
              <button
                onClick={() => setFilter("decisions")}
                className={`rounded-lg px-3 py-1.5 text-xs font-medium transition-colors ${filter === "decisions" ? (isDark ? "bg-gray-800 text-emerald-400 shadow-sm" : "bg-white text-emerald-600 shadow-sm") : "text-gray-500 hover:text-emerald-500"}`}
              >
                Decisions
              </button>
              <button
                onClick={() => setFilter("llm")}
                className={`rounded-lg px-3 py-1.5 text-xs font-medium transition-colors ${filter === "llm" ? (isDark ? "bg-gray-800 text-purple-400 shadow-sm" : "bg-white text-purple-600 shadow-sm") : "text-gray-500 hover:text-purple-500"}`}
              >
                LLM & Tools
              </button>
              <button
                onClick={() => setFilter("errors")}
                className={`rounded-lg px-3 py-1.5 text-xs font-medium transition-colors ${filter === "errors" ? (isDark ? "bg-gray-800 text-red-400 shadow-sm" : "bg-white text-red-600 shadow-sm") : "text-gray-500 hover:text-red-500"}`}
              >
                Errors
              </button>
            </div>

            <button
              onClick={handleClear}
              className={`flex items-center justify-center rounded-xl p-2 transition-colors ${isDark ? "text-gray-400 hover:bg-red-900/30 hover:text-red-400" : "text-gray-500 hover:bg-red-50 hover:text-red-600"}`}
              title="Clear logs"
            >
              <Trash2 className="h-5 w-5" />
            </button>
            <div className={`mx-1 h-6 w-px ${isDark ? "bg-gray-800" : "bg-gray-200"}`} />
            <button
              onClick={onClose}
              className={`flex items-center justify-center rounded-xl p-2 transition-colors ${isDark ? "text-gray-400 hover:bg-white/10 hover:text-white" : "text-gray-500 hover:bg-black/5 hover:text-black"}`}
            >
              <X className="h-5 w-5" />
            </button>
          </div>
        </div>

        <div className={`flex flex-1 flex-col overflow-hidden ${isDark ? "bg-black/20" : "bg-gray-50/50"}`}>
          {loadError && (
            <div className={`mx-4 mt-4 rounded-2xl border px-4 py-3 text-sm ${isDark ? "border-red-900/50 bg-red-950/20 text-red-200" : "border-red-200 bg-red-50 text-red-700"}`}>
              {loadError}
            </div>
          )}

          {logs.length === 0 ? (
            <div className="flex flex-1 flex-col items-center justify-center gap-4 text-gray-400 dark:text-gray-600">
              <Terminal className="h-12 w-12 opacity-50" />
              <p className="text-sm font-medium">Waiting for agent activity...</p>
            </div>
          ) : (
            <div className="flex-1 space-y-3 overflow-y-auto p-4 font-mono text-[13px] scroll-smooth">
              {filteredLogs.map((log) => {
                const date = new Date(log.ts_epoch * 1000);
                const timeStr = `${date.toLocaleTimeString(undefined, {
                  hour12: false,
                  hour: "2-digit",
                  minute: "2-digit",
                  second: "2-digit",
                })}.${date.getMilliseconds().toString().padStart(3, "0")}`;

                return (
                  <div
                    key={log.id}
                    className={`group relative rounded-2xl border p-3 transition-all hover:shadow-md ${log.kind === "error" ? (isDark ? "border-red-900/50 bg-red-950/20" : "border-red-200 bg-red-50/50") : (isDark ? "border-gray-700/50 bg-gray-800/40 hover:bg-gray-800/60" : "border-gray-200 bg-white hover:border-gray-300")}`}
                  >
                    <div className="flex items-start gap-4">
                      <div className="min-w-[85px] shrink-0 pt-0.5">
                        <div className={`text-[11px] font-medium ${isDark ? "text-gray-500" : "text-gray-400"}`}>{timeStr}</div>
                        <div className={`mt-1.5 flex items-center ${KIND_COLORS[log.kind] || "text-gray-400"}`}>
                          {KIND_ICONS[log.kind] || KIND_ICONS.info}
                        </div>
                      </div>

                      <div className="min-w-0 flex-1">
                        <div className="mb-1.5 flex flex-wrap items-center gap-2">
                          <span className={`rounded-lg border px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider ${AGENT_COLORS[log.agent] || AGENT_COLORS.system}`}>
                            {log.agent}
                          </span>
                          <span className={`break-words font-semibold ${isDark ? "text-gray-200" : "text-gray-800"}`}>
                            {log.message}
                          </span>
                        </div>

                        {log.data && Object.keys(log.data).length > 0 && (
                          <div className={`mt-2 overflow-x-auto rounded-xl border p-2.5 text-xs whitespace-pre-wrap ${isDark ? "border-gray-800 bg-black/40 text-gray-300" : "border-gray-100 bg-gray-50 text-gray-600"}`}>
                            {"query" in log.data && log.data.query && (
                              <div className="mb-1">
                                <span className="select-none opacity-50">query: </span>
                                <span className={isDark ? "text-emerald-400" : "text-emerald-600"}>{String(log.data.query)}</span>
                              </div>
                            )}
                            {"rationale" in log.data && log.data.rationale && (
                              <div className="mb-1">
                                <span className="select-none opacity-50">rationale: </span>
                                <span className="italic">{String(log.data.rationale)}</span>
                              </div>
                            )}
                            {"sql" in log.data && log.data.sql && (
                              <div className="mb-1 mt-1 rounded bg-black/20 p-2 font-mono text-blue-400">
                                {String(log.data.sql)}
                              </div>
                            )}
                            {"tool" in log.data && log.data.tool && (
                              <div className="mb-1">
                                <span className="select-none opacity-50">tool: </span>
                                <span className={isDark ? "text-fuchsia-400" : "text-fuchsia-600"}>{String(log.data.tool)}</span>
                              </div>
                            )}
                            {"error" in log.data && log.data.error && (
                              <div className="mb-1">
                                <span className="select-none opacity-50">error: </span>
                                <span className={isDark ? "text-red-400" : "text-red-600"}>{String(log.data.error)}</span>
                              </div>
                            )}
                            {Object.entries(log.data)
                              .filter(([key]) => !["query", "rationale", "sql", "tool", "error"].includes(key))
                              .length > 0 && (
                              <div className="mt-1 flex flex-wrap gap-x-4 gap-y-1 opacity-80">
                                {Object.entries(log.data)
                                  .filter(([key]) => !["query", "rationale", "sql", "tool", "error"].includes(key))
                                  .map(([key, value]) => (
                                    <span key={key}>
                                      <span className="select-none opacity-50">{key}: </span>
                                      {typeof value === "object" ? JSON.stringify(value) : String(value)}
                                    </span>
                                  ))}
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                );
              })}
              <div ref={logsEndRef} className="h-1" />
            </div>
          )}
        </div>
      </div>
    </div>,
    document.body
  );
}
