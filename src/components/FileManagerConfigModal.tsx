import React, { useEffect, useState } from "react";
import { CheckCircle2, FolderOpen, Loader2, Shield, Sparkles, X, XCircle } from "lucide-react";
import { FileManagerAgentConfig } from "../lib/utils";

interface FileManagerConfigModalProps {
  isOpen: boolean;
  config: FileManagerAgentConfig;
  onClose: () => void;
  onSave: (config: FileManagerAgentConfig) => void;
}

function normalizeLocalConfig(config: FileManagerAgentConfig): FileManagerAgentConfig {
  return {
    basePath: config.basePath ?? "",
    maxIterations: Math.min(15, Math.max(1, config.maxIterations ?? 10)),
    systemPrompt:
      config.systemPrompt ||
      "You are the File Management agent. Reply in English by default. Use filesystem tools instead of guessing, keep answers short and factual, ask for confirmation before destructive or overwrite actions, and present final user-facing answers in polished Markdown with concise structure and tasteful emphasis.",
  };
}

export function FileManagerConfigModal({
  isOpen,
  config,
  onClose,
  onSave,
}: FileManagerConfigModalProps) {
  const [localConfig, setLocalConfig] = useState<FileManagerAgentConfig>(() => normalizeLocalConfig(config));
  const [isTestingPath, setIsTestingPath] = useState(false);
  const [pathTestResult, setPathTestResult] = useState<null | {
    status: "ok" | "error";
    message: string;
    resolvedPath?: string;
    entryCount?: number;
  }>(null);

  useEffect(() => {
    setLocalConfig(normalizeLocalConfig(config));
    setIsTestingPath(false);
    setPathTestResult(null);
  }, [config, isOpen]);

  if (!isOpen) return null;

  const handleTestPath = async () => {
    setIsTestingPath(true);
    setPathTestResult(null);
    try {
      const response = await fetch("/api/file-manager/test-path", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          file_manager_config: {
            base_path: localConfig.basePath,
            max_iterations: localConfig.maxIterations,
            system_prompt: localConfig.systemPrompt,
          },
        }),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload.detail || `HTTP ${response.status}`);
      }
      setPathTestResult({
        status: "ok",
        message: localConfig.basePath
          ? "The configured folder is accessible."
          : "The default workspace access is available.",
        resolvedPath: payload.resolvedPath,
        entryCount: payload.entryCount,
      });
    } catch (error) {
      setPathTestResult({
        status: "error",
        message: error instanceof Error ? error.message : "Unable to access the configured folder.",
      });
    } finally {
      setIsTestingPath(false);
    }
  };

  return (
    <div className="fixed inset-0 z-[90] bg-black/45 backdrop-blur-sm flex items-center justify-center p-4">
      <div className="w-full max-w-2xl rounded-[2rem] border border-white/20 bg-[#f8f8f6] dark:bg-[#101115] shadow-2xl shadow-black/30 overflow-hidden">
        <div className="flex items-center justify-between gap-4 px-6 py-5 border-b border-gray-200/70 dark:border-gray-800/80 bg-white/80 dark:bg-black/20">
          <div>
            <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.2em] text-gray-500 dark:text-gray-400">
              <FolderOpen className="w-3.5 h-3.5" />
              File management
            </div>
            <h2 className="mt-1 text-xl font-semibold text-gray-900 dark:text-gray-100">
              Agent configuration
            </h2>
            <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
              Configure the access root, ReAct iteration budget, and the local-system prompt.
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="inline-flex items-center justify-center w-10 h-10 rounded-xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="p-6 space-y-5">
          <div className="rounded-[1.5rem] border border-emerald-200/70 dark:border-emerald-700/40 bg-emerald-50/80 dark:bg-emerald-900/10 p-4">
            <div className="flex items-start gap-3">
              <Shield className="w-5 h-5 text-emerald-600 dark:text-emerald-300 mt-0.5" />
              <div className="text-sm text-emerald-900 dark:text-emerald-200 space-y-1">
                <p className="font-medium">Safe by default</p>
                <p className="text-emerald-800/85 dark:text-emerald-300/85">
                  Destructive actions still require confirmation. Set an access root if you want to limit all file operations to a specific directory.
                </p>
              </div>
            </div>
          </div>

          <label className="block space-y-2">
            <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
              Access root
            </span>
            <div className="flex flex-col gap-3 md:flex-row">
              <input
                type="text"
                value={localConfig.basePath}
                onChange={(e) => {
                  setLocalConfig((prev) => ({ ...prev, basePath: e.target.value }));
                  setPathTestResult(null);
                }}
                placeholder="/path/to/shared-folder"
                className="w-full rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-emerald-400"
              />
              <button
                type="button"
                onClick={handleTestPath}
                disabled={isTestingPath}
                className="inline-flex items-center justify-center gap-2 rounded-2xl border border-emerald-200 dark:border-emerald-700 bg-emerald-50 dark:bg-emerald-900/15 px-4 py-3 text-sm font-medium text-emerald-900 dark:text-emerald-200 transition-colors hover:bg-emerald-100 dark:hover:bg-emerald-900/25 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {isTestingPath ? <Loader2 className="h-4 w-4 animate-spin" /> : <CheckCircle2 className="h-4 w-4" />}
                Test folder access
              </button>
            </div>
            <p className="text-xs text-gray-500 dark:text-gray-400">
              Leave empty to allow access to the full server-visible workspace.
            </p>
            {pathTestResult && (
              <div
                className={[
                  "rounded-2xl border px-4 py-3 text-sm",
                  pathTestResult.status === "ok"
                    ? "border-emerald-200 bg-emerald-50/90 text-emerald-900 dark:border-emerald-700/60 dark:bg-emerald-900/15 dark:text-emerald-200"
                    : "border-rose-200 bg-rose-50/90 text-rose-900 dark:border-rose-700/60 dark:bg-rose-900/15 dark:text-rose-200",
                ].join(" ")}
              >
                <div className="flex items-start gap-2">
                  {pathTestResult.status === "ok" ? (
                    <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0" />
                  ) : (
                    <XCircle className="mt-0.5 h-4 w-4 shrink-0" />
                  )}
                  <div className="space-y-1">
                    <p className="font-medium">{pathTestResult.message}</p>
                    {pathTestResult.resolvedPath && (
                      <p className="text-xs opacity-80">
                        Resolved path: <span className="font-mono">{pathTestResult.resolvedPath}</span>
                      </p>
                    )}
                    {typeof pathTestResult.entryCount === "number" && (
                      <p className="text-xs opacity-80">
                        Visible entries: {pathTestResult.entryCount}
                      </p>
                    )}
                  </div>
                </div>
              </div>
            )}
          </label>

          <label className="block space-y-2">
            <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
              Max iterations
            </span>
            <div className="flex items-center gap-4">
              <input
                type="range"
                min={1}
                max={15}
                step={1}
                value={localConfig.maxIterations}
                onChange={(e) => setLocalConfig((prev) => ({ ...prev, maxIterations: Number(e.target.value) }))}
                className="w-full accent-emerald-600"
              />
              <div className="w-14 rounded-xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-3 py-2 text-center text-sm font-semibold text-gray-900 dark:text-gray-100">
                {localConfig.maxIterations}
              </div>
            </div>
            <p className="text-xs text-gray-500 dark:text-gray-400">
              Hard-capped at 15 to avoid loops while keeping enough room for tool-driven reasoning.
            </p>
          </label>

          <label className="block space-y-2">
            <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
              System prompt
            </span>
            <textarea
              value={localConfig.systemPrompt}
              onChange={(e) => setLocalConfig((prev) => ({ ...prev, systemPrompt: e.target.value }))}
              rows={7}
              className="w-full rounded-[1.5rem] border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-emerald-400 resize-y"
            />
            <p className="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
              <Sparkles className="w-3.5 h-3.5" />
              The backend still uses only the locally configured LLM and keeps replies in English by default.
            </p>
          </label>
        </div>

        <div className="flex items-center justify-end gap-3 px-6 py-5 border-t border-gray-200/70 dark:border-gray-800/80 bg-white/70 dark:bg-black/10">
          <button
            type="button"
            onClick={onClose}
            className="px-4 py-2 rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm font-medium text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={() => onSave(normalizeLocalConfig(localConfig))}
            className="px-4 py-2 rounded-2xl bg-black text-white text-sm font-medium hover:bg-gray-800 transition-colors"
          >
            Save configuration
          </button>
        </div>
      </div>
    </div>
  );
}
