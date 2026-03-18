import React, { useEffect, useState } from "react";
import { FolderOpen, Shield, Sparkles, X } from "lucide-react";
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
      "You are the File Management agent. Reply in English by default. Use filesystem tools instead of guessing, keep answers short and factual, and ask for confirmation before destructive or overwrite actions.",
  };
}

export function FileManagerConfigModal({
  isOpen,
  config,
  onClose,
  onSave,
}: FileManagerConfigModalProps) {
  const [localConfig, setLocalConfig] = useState<FileManagerAgentConfig>(() => normalizeLocalConfig(config));

  useEffect(() => {
    setLocalConfig(normalizeLocalConfig(config));
  }, [config, isOpen]);

  if (!isOpen) return null;

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
              Configure the sandbox path, ReAct iteration budget, and the local-system prompt.
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
                  Destructive actions still require confirmation. Set a sandbox base path if you want to restrict all file operations to a specific directory.
                </p>
              </div>
            </div>
          </div>

          <label className="block space-y-2">
            <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">
              Sandbox base path
            </span>
            <input
              type="text"
              value={localConfig.basePath}
              onChange={(e) => setLocalConfig((prev) => ({ ...prev, basePath: e.target.value }))}
              placeholder="/Users/mathieumasson/Documents/Shared"
              className="w-full rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-950 px-4 py-3 text-sm text-gray-900 dark:text-gray-100 outline-none focus:border-emerald-400"
            />
            <p className="text-xs text-gray-500 dark:text-gray-400">
              Leave empty to allow access to the full server-visible workspace.
            </p>
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
