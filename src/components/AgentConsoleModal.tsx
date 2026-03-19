import React, { useEffect, useState, useRef } from "react";
import { X, Trash2, Activity, Terminal, AlertTriangle, Clock, Zap, Target, Cpu, CheckCircle2, FileText, Wrench } from "lucide-react";

export interface LogEvent {
  id: string;
  ts: string;
  ts_epoch: number;
  kind: "info" | "warning" | "error" | "decision" | "llm" | "tool";
  agent: string;
  message: string;
  data: Record<string, any>;
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
  file_manager: "bg-emerald-500/20 text-emerald-600 dark:text-emerald-400 border-emerald-500/30",
  pdf_creator: "bg-slate-500/20 text-slate-600 dark:text-slate-400 border-slate-500/30",
  oracle_analyst: "bg-amber-500/20 text-amber-600 dark:text-amber-400 border-amber-500/30",
  data_quality: "bg-fuchsia-500/20 text-fuchsia-600 dark:text-fuchsia-400 border-fuchsia-500/30",
  system: "bg-gray-500/20 text-gray-600 dark:text-gray-400 border-gray-500/30",
};

const KIND_ICONS: Record<string, React.ReactNode> = {
  info: <Activity className="w-3.5 h-3.5" />,
  warning: <AlertTriangle className="w-3.5 h-3.5" />,
  error: <AlertTriangle className="w-3.5 h-3.5 text-red-500" />,
  decision: <Target className="w-3.5 h-3.5" />,
  llm: <Cpu className="w-3.5 h-3.5" />,
  tool: <Wrench className="w-3.5 h-3.5" />,
};

const KIND_COLORS: Record<string, string> = {
  info: "text-blue-500",
  warning: "text-amber-500",
  error: "text-red-500",
  decision: "text-emerald-500",
  llm: "text-purple-500",
  tool: "text-cyan-500",
};

export function AgentConsoleModal({ isOpen, onClose, isDark }: AgentConsoleModalProps) {
  const [logs, setLogs] = useState<LogEvent[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const [filter, setFilter] = useState<string>("all");
  const logsEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!isOpen) {
      return;
    }

    const eventSource = new EventSource("/api/logs/stream");

    eventSource.onopen = () => {
      setIsConnected(true);
    };

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.type === "connect") {
          setIsConnected(true);
        } else if (data.type === "history") {
          setLogs(data.logs);
        } else if (data.type === "log") {
          setLogs((prev) => [...prev, data.log].slice(-1000)); // Keep max 1000 logs in standard array
        }
      } catch (err) {
        console.error("Error parsing log stream", err);
      }
    };

    eventSource.onerror = (err) => {
      console.error("EventSource failed:", err);
      setIsConnected(false);
    };

    return () => {
      eventSource.close();
      setIsConnected(false);
    };
  }, [isOpen]);

  useEffect(() => {
    if (logsEndRef.current) {
      logsEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [logs]);

  if (!isOpen) return null;

  const handleClear = async () => {
    try {
      await fetch("/api/logs/clear", { method: "POST" });
      setLogs([]);
    } catch (err) {
      console.error("Failed to clear logs", err);
    }
  };

  const filteredLogs = logs.filter((log) => {
    if (filter === "all") return true;
    if (filter === "errors") return log.kind === "error" || log.kind === "warning";
    if (filter === "decisions") return log.kind === "decision";
    if (filter === "llm") return log.kind === "llm" || log.kind === "tool";
    return true;
  });

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/40 backdrop-blur-sm animate-fade-in">
      <div 
        className={`relative w-full max-w-5xl h-[85vh] flex flex-col rounded-3xl overflow-hidden shadow-2xl shadow-black/50 border transition-all duration-300 ${isDark ? 'bg-[#0f0f13]/90 border-gray-800' : 'bg-white/90 border-gray-200'} backdrop-blur-2xl`}
      >
        {/* Header */}
        <div className={`flex items-center justify-between px-6 py-4 border-b ${isDark ? 'border-gray-800' : 'border-gray-200'} shrink-0`}>
          <div className="flex items-center gap-3">
            <div className={`p-2 rounded-xl bg-gradient-to-tr ${isDark ? 'from-indigo-600 to-purple-600' : 'from-indigo-500 to-purple-500'} text-white shadow-lg`}>
              <Terminal className="w-5 h-5" />
            </div>
            <div>
              <h2 className={`font-semibold text-lg ${isDark ? 'text-white' : 'text-gray-900'} tracking-tight`}>Agent Observability Console</h2>
              <div className="flex items-center gap-2 text-xs font-medium text-gray-500 dark:text-gray-400 mt-0.5">
                <span className={`w-2 h-2 rounded-full ${isConnected ? 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.8)]' : 'bg-red-500'} animate-pulse`} />
                {isConnected ? 'Live stream connected (10min TTL)' : 'Reconnecting...'}
              </div>
            </div>
          </div>
          <div className="flex items-center gap-3">
            {/* Filters */}
            <div className={`flex items-center p-1 rounded-xl mr-2 ${isDark ? 'bg-black/50' : 'bg-gray-100'}`}>
              <button 
                onClick={() => setFilter("all")} 
                className={`px-3 py-1.5 text-xs font-medium rounded-lg transition-colors ${filter === "all" ? (isDark ? 'bg-gray-800 text-white shadow-sm' : 'bg-white text-gray-900 shadow-sm') : 'text-gray-500 hover:text-gray-700 dark:hover:text-gray-300'}`}
              >
                All Events
              </button>
              <button 
                onClick={() => setFilter("decisions")} 
                className={`px-3 py-1.5 text-xs font-medium rounded-lg transition-colors ${filter === "decisions" ? (isDark ? 'bg-gray-800 text-emerald-400 shadow-sm' : 'bg-white text-emerald-600 shadow-sm') : 'text-gray-500 hover:text-emerald-500'}`}
              >
                Decisions
              </button>
              <button 
                onClick={() => setFilter("llm")} 
                className={`px-3 py-1.5 text-xs font-medium rounded-lg transition-colors ${filter === "llm" ? (isDark ? 'bg-gray-800 text-purple-400 shadow-sm' : 'bg-white text-purple-600 shadow-sm') : 'text-gray-500 hover:text-purple-500'}`}
              >
                LLM & Tools
              </button>
              <button 
                onClick={() => setFilter("errors")} 
                className={`px-3 py-1.5 text-xs font-medium rounded-lg transition-colors ${filter === "errors" ? (isDark ? 'bg-gray-800 text-red-400 shadow-sm' : 'bg-white text-red-600 shadow-sm') : 'text-gray-500 hover:text-red-500'}`}
              >
                Errors
              </button>
            </div>
            
            <button
              onClick={handleClear}
              className={`p-2 rounded-xl transition-colors flex items-center justify-center ${isDark ? 'text-gray-400 hover:bg-red-900/30 hover:text-red-400' : 'text-gray-500 hover:bg-red-50 hover:text-red-600'}`}
              title="Clear logs"
            >
              <Trash2 className="w-5 h-5" />
            </button>
            <div className={`w-px h-6 mx-1 ${isDark ? 'bg-gray-800' : 'bg-gray-200'}`} />
            <button
              onClick={onClose}
              className={`p-2 rounded-xl transition-colors flex items-center justify-center ${isDark ? 'text-gray-400 hover:bg-white/10 hover:text-white' : 'text-gray-500 hover:bg-black/5 hover:text-black'}`}
            >
              <X className="w-5 h-5" />
            </button>
          </div>
        </div>

        {/* Console Body */}
        <div className={`flex-1 overflow-hidden flex flex-col ${isDark ? 'bg-black/20' : 'bg-gray-50/50'}`}>
          {logs.length === 0 ? (
            <div className="flex-1 flex flex-col items-center justify-center text-gray-400 dark:text-gray-600 gap-4">
              <Terminal className="w-12 h-12 opacity-50" />
              <p className="text-sm font-medium">Waiting for agent activity...</p>
            </div>
          ) : (
            <div className="flex-1 overflow-y-auto p-4 space-y-3 font-mono text-[13px] scroll-smooth">
              {filteredLogs.map((log) => {
                const date = new Date(log.ts_epoch * 1000);
                const timeStr = date.toLocaleTimeString(undefined, { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' }) + '.' + date.getMilliseconds().toString().padStart(3, '0');
                
                return (
                  <div 
                    key={log.id} 
                    className={`group relative p-3 rounded-2xl border transition-all hover:shadow-md ${log.kind === 'error' ? (isDark ? 'bg-red-950/20 border-red-900/50' : 'bg-red-50/50 border-red-200') : (isDark ? 'bg-gray-800/40 border-gray-700/50 hover:bg-gray-800/60' : 'bg-white border-gray-200 hover:border-gray-300')}`}
                  >
                    <div className="flex items-start gap-4">
                      {/* Left: Time & Icon */}
                      <div className="flex flex-col items-end gap-1.5 shrink-0 pt-0.5 min-w-[85px]">
                        <span className={`text-[11px] font-medium ${isDark ? 'text-gray-500' : 'text-gray-400'}`}>{timeStr}</span>
                        <div className={`flex items-center justify-center ${KIND_COLORS[log.kind] || 'text-gray-400'}`}>
                          {KIND_ICONS[log.kind] || KIND_ICONS['info']}
                        </div>
                      </div>

                      {/* Right: Content */}
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2 mb-1.5 flex-wrap">
                          <span className={`px-2 py-0.5 rounded-lg text-[10px] uppercase tracking-wider font-bold border ${AGENT_COLORS[log.agent] || AGENT_COLORS['system']}`}>
                            {log.agent}
                          </span>
                          <span className={`${isDark ? 'text-gray-200' : 'text-gray-800'} font-semibold break-words`}>
                            {log.message}
                          </span>
                        </div>
                        
                        {log.data && Object.keys(log.data).length > 0 && (
                          <div className={`mt-2 p-2.5 rounded-xl text-xs overflow-x-auto ${isDark ? 'bg-black/40 text-gray-300' : 'bg-gray-50 text-gray-600'} border ${isDark ? 'border-gray-800' : 'border-gray-100'} whitespace-pre-wrap`}>
                            {/* Format common known data fields nicely, fallback to JSON */}
                            {log.data.query && <div className="mb-1"><span className="opacity-50 select-none">query: </span><span className={isDark ? "text-emerald-400" : "text-emerald-600"}>{log.data.query}</span></div>}
                            {log.data.rationale && <div className="mb-1"><span className="opacity-50 select-none">rationale: </span><span className="italic">{log.data.rationale}</span></div>}
                            {log.data.sql && <div className="mb-1 mt-1 p-2 bg-black/20 rounded font-mono text-blue-400">{log.data.sql}</div>}
                            {log.data.tool && <div className="mb-1"><span className="opacity-50 select-none">tool: </span><span className={isDark ? "text-fuchsia-400" : "text-fuchsia-600"}>{log.data.tool}</span></div>}
                            {log.data.error && <div className="mb-1"><span className="opacity-50 select-none">error: </span><span className={isDark ? "text-red-400" : "text-red-600"}>{log.data.error}</span></div>}
                            {Object.entries(log.data).filter(([k]) => !['query', 'rationale', 'sql', 'tool', 'error'].includes(k)).length > 0 && (
                              <div className="opacity-80 mt-1 flex flex-wrap gap-x-4 gap-y-1">
                                {Object.entries(log.data)
                                  .filter(([k]) => !['query', 'rationale', 'sql', 'tool', 'error'].includes(k))
                                  .map(([k, v]) => (
                                    <span key={k}><span className="opacity-50 select-none">{k}: </span>{typeof v === 'object' ? JSON.stringify(v) : String(v)}</span>
                                  ))
                                }
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
    </div>
  );
}
