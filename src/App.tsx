import React, { useCallback, useEffect, useRef, useState } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { ChatInterface } from './components/ChatInterface';
import { SettingsModal } from './components/SettingsModal';
import { LandingPage } from './components/LandingPage';
import { ComingSoon } from './components/ComingSoon';
import {
  AppConfig,
  AppPreferences,
  Conversation,
  DEFAULT_PERSISTED_STATE,
  Page,
  PersistedAppState,
  hasMeaningfulPersistedAppState,
  normalizePersistedAppState,
} from './lib/utils';

const pageVariants = {
  initial: { opacity: 0, scale: 0.97, filter: 'blur(4px)' },
  animate: { opacity: 1, scale: 1, filter: 'blur(0px)' },
  exit: { opacity: 0, scale: 1.01, filter: 'blur(4px)' },
};

const pageTransition = { duration: 0.45, ease: 'easeOut' as const };

const DB_STATE_URL = '/api/db/state';
const DB_EXPORT_URL = '/api/db/export';
const DB_IMPORT_URL = '/api/db/import';
const APP_STATE_CACHE_KEY = 'ragnarok-app-state-cache';
const LEGACY_CONFIG_KEY = 'liquid-ai-config';
const LEGACY_CONVERSATIONS_KEY = 'ragnarok_conversations';
const LEGACY_DARK_KEY = 'ragnarok-dark';

function pickPersistedPayload(state: PersistedAppState) {
  return {
    config: state.config,
    conversations: state.conversations,
    preferences: state.preferences,
  };
}

function loadLocalFallbackState(): PersistedAppState {
  if (typeof window === 'undefined') {
    return normalizePersistedAppState(DEFAULT_PERSISTED_STATE);
  }

  try {
    const cached = localStorage.getItem(APP_STATE_CACHE_KEY);
    if (cached) {
      return normalizePersistedAppState(JSON.parse(cached));
    }
  } catch {
    // Ignore cache parsing issues and continue with legacy fallbacks.
  }

  let legacyConfig = DEFAULT_PERSISTED_STATE.config;
  let legacyConversations: Conversation[] = [];

  try {
    const savedConfig = localStorage.getItem(LEGACY_CONFIG_KEY);
    if (savedConfig) {
      legacyConfig = JSON.parse(savedConfig);
    }
  } catch {
    // Ignore malformed legacy config.
  }

  try {
    const savedConversations = localStorage.getItem(LEGACY_CONVERSATIONS_KEY);
    if (savedConversations) {
      legacyConversations = JSON.parse(savedConversations);
    }
  } catch {
    // Ignore malformed legacy conversations.
  }

  return normalizePersistedAppState({
    config: legacyConfig,
    conversations: legacyConversations,
    preferences: {
      ...DEFAULT_PERSISTED_STATE.preferences,
      darkMode: localStorage.getItem(LEGACY_DARK_KEY) === 'true',
      currentConversationId: legacyConversations[0]?.id ?? null,
    },
  });
}

function persistLocalCache(state: PersistedAppState) {
  if (typeof window === 'undefined') return;

  localStorage.setItem(APP_STATE_CACHE_KEY, JSON.stringify(state));
  localStorage.setItem(LEGACY_CONFIG_KEY, JSON.stringify(state.config));
  localStorage.setItem(LEGACY_CONVERSATIONS_KEY, JSON.stringify(state.conversations));
  localStorage.setItem(LEGACY_DARK_KEY, String(state.preferences.darkMode));
}

function HydrationScreen({ syncError }: { syncError: string | null }) {
  return (
    <div className="min-h-screen bg-[#f5f5f5] dark:bg-[#0f0f13] flex items-center justify-center px-6">
      <div className="glass-panel max-w-lg w-full rounded-[2rem] p-8 text-center shadow-2xl">
        <div className="w-14 h-14 mx-auto rounded-2xl bg-gradient-to-tr from-slate-700 to-slate-900 flex items-center justify-center shadow-md shadow-slate-900/20 mb-4">
          <span className="text-white text-xl font-semibold">R</span>
        </div>
        <h1 className="text-2xl font-semibold text-gray-900 dark:text-white">Syncing the DB</h1>
        <p className="mt-3 text-sm text-gray-600 dark:text-gray-300 leading-relaxed">
          RAGnarok is reloading the persistent state to show the latest version saved on the backend.
        </p>
        {syncError && (
          <p className="mt-4 text-sm text-red-600 dark:text-red-400">
            Latest issue detected: {syncError}
          </p>
        )}
      </div>
    </div>
  );
}

/**
 * Main Application Component
 */
export default function App() {
  const [appState, setAppState] = useState<PersistedAppState>(() => loadLocalFallbackState());
  const [isSettingsOpen, setIsSettingsOpen] = useState(false);
  const [isHydrating, setIsHydrating] = useState(true);
  const [isDbBusy, setIsDbBusy] = useState(false);
  const [syncError, setSyncError] = useState<string | null>(null);
  const [lastSyncedAt, setLastSyncedAt] = useState<string | null>(null);

  const latestStateRef = useRef(appState);
  const initialSyncCompleteRef = useRef(false);
  const skipNextPersistRef = useRef(false);

  useEffect(() => {
    latestStateRef.current = appState;
    persistLocalCache(appState);
  }, [appState]);

  const updatePreferences = useCallback((updater: Partial<AppPreferences> | ((prev: AppPreferences) => AppPreferences)) => {
    setAppState((prev) => {
      const nextPreferences =
        typeof updater === 'function'
          ? updater(prev.preferences)
          : { ...prev.preferences, ...updater };
      return normalizePersistedAppState({ ...prev, preferences: nextPreferences });
    });
  }, []);

  const setConfig = useCallback((config: AppConfig) => {
    setAppState((prev) => normalizePersistedAppState({ ...prev, config }));
  }, []);

  const setConversations = useCallback((updater: Conversation[] | ((prev: Conversation[]) => Conversation[])) => {
    setAppState((prev) => {
      const nextConversations =
        typeof updater === 'function' ? updater(prev.conversations) : updater;
      return normalizePersistedAppState({ ...prev, conversations: nextConversations });
    });
  }, []);

  const saveStateToDb = useCallback(
    async (snapshot: PersistedAppState, options?: { showBusy?: boolean; adoptSavedState?: boolean }) => {
      const { showBusy = false, adoptSavedState = true } = options ?? {};
      if (showBusy) setIsDbBusy(true);

      try {
        const response = await fetch(DB_STATE_URL, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(pickPersistedPayload(snapshot)),
        });

        if (!response.ok) {
          const errorPayload = await response.json().catch(() => ({}));
          throw new Error(errorPayload.detail || `HTTP ${response.status}`);
        }

        const savedState = normalizePersistedAppState(await response.json());
        const requestedFingerprint = JSON.stringify(pickPersistedPayload(snapshot));
        const currentFingerprint = JSON.stringify(pickPersistedPayload(latestStateRef.current));

        setLastSyncedAt(savedState.updatedAt ?? null);
        setSyncError(null);

        if (adoptSavedState && requestedFingerprint === currentFingerprint) {
          skipNextPersistRef.current = true;
          setAppState(savedState);
        }

        return savedState;
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Unable to save DB state.';
        setSyncError(message);
        throw error;
      } finally {
        if (showBusy) setIsDbBusy(false);
      }
    },
    []
  );

  const syncFromDb = useCallback(
    async ({ force = false, showBusy = false }: { force?: boolean; showBusy?: boolean } = {}) => {
      if (showBusy) setIsDbBusy(true);

      try {
        const response = await fetch(DB_STATE_URL);
        if (!response.ok) {
          const errorPayload = await response.json().catch(() => ({}));
          throw new Error(errorPayload.detail || `HTTP ${response.status}`);
        }

        const remoteState = normalizePersistedAppState(await response.json());
        const localState = latestStateRef.current;
        const remoteUpdatedAt = Date.parse(remoteState.updatedAt ?? '');
        const localUpdatedAt = Date.parse(localState.updatedAt ?? '');

        if (
          !hasMeaningfulPersistedAppState(remoteState) &&
          hasMeaningfulPersistedAppState(localState)
        ) {
          await saveStateToDb(localState, { adoptSavedState: true, showBusy: false });
          return;
        }

        if (
          force ||
          Number.isNaN(localUpdatedAt) ||
          (!Number.isNaN(remoteUpdatedAt) && remoteUpdatedAt >= localUpdatedAt)
        ) {
          skipNextPersistRef.current = true;
          setAppState(remoteState);
        }

        setLastSyncedAt(remoteState.updatedAt ?? null);
        setSyncError(null);
      } catch (error) {
        setSyncError(error instanceof Error ? error.message : 'Unable to sync DB state.');
      } finally {
        initialSyncCompleteRef.current = true;
        setIsHydrating(false);
        if (showBusy) setIsDbBusy(false);
      }
    },
    [saveStateToDb]
  );

  const exportDbBackup = useCallback(async () => {
    setIsDbBusy(true);
    try {
      const response = await fetch(DB_EXPORT_URL);
      if (!response.ok) {
        const errorPayload = await response.json().catch(() => ({}));
        throw new Error(errorPayload.detail || `HTTP ${response.status}`);
      }

      const blob = await response.blob();
      const contentDisposition = response.headers.get('Content-Disposition') || '';
      const filenameMatch = contentDisposition.match(/filename="([^"]+)"/);
      const filename = filenameMatch?.[1] || 'ragnarok-db-backup.json';
      const objectUrl = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = objectUrl;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(objectUrl);
      setSyncError(null);
    } catch (error) {
      setSyncError(error instanceof Error ? error.message : 'Unable to export DB backup.');
      throw error;
    } finally {
      setIsDbBusy(false);
    }
  }, []);

  const importDbBackup = useCallback(async (snapshot: unknown) => {
    setIsDbBusy(true);
    try {
      const response = await fetch(DB_IMPORT_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(snapshot),
      });

      if (!response.ok) {
        const errorPayload = await response.json().catch(() => ({}));
        throw new Error(errorPayload.detail || `HTTP ${response.status}`);
      }

      const importedState = normalizePersistedAppState(await response.json());
      skipNextPersistRef.current = true;
      initialSyncCompleteRef.current = true;
      setAppState(importedState);
      setLastSyncedAt(importedState.updatedAt ?? null);
      setSyncError(null);
      setIsHydrating(false);
    } catch (error) {
      setSyncError(error instanceof Error ? error.message : 'Unable to import DB backup.');
      throw error;
    } finally {
      setIsDbBusy(false);
    }
  }, []);

  useEffect(() => {
    void syncFromDb({ force: true, showBusy: false });
  }, [syncFromDb]);

  useEffect(() => {
    const handleFocus = () => {
      void syncFromDb({ force: true, showBusy: false });
    };

    const handleVisibilityChange = () => {
      if (document.visibilityState === 'visible') {
        void syncFromDb({ force: true, showBusy: false });
      }
    };

    window.addEventListener('focus', handleFocus);
    document.addEventListener('visibilitychange', handleVisibilityChange);

    return () => {
      window.removeEventListener('focus', handleFocus);
      document.removeEventListener('visibilitychange', handleVisibilityChange);
    };
  }, [syncFromDb]);

  useEffect(() => {
    if (!initialSyncCompleteRef.current) return;
    if (skipNextPersistRef.current) {
      skipNextPersistRef.current = false;
      return;
    }

    const timeoutId = window.setTimeout(() => {
      void saveStateToDb(latestStateRef.current, { adoptSavedState: true, showBusy: false });
    }, 350);

    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [appState, saveStateToDb]);

  const page = appState.preferences.page;
  const isDark = appState.preferences.darkMode;

  useEffect(() => {
    if (page === 'chat' && isDark) {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
  }, [isDark, page]);

  const navigate = useCallback((target: Page) => {
    updatePreferences({ page: target });
  }, [updatePreferences]);

  if (isHydrating) {
    return <HydrationScreen syncError={syncError} />;
  }

  return (
    <>
      <AnimatePresence mode="wait">
        {page === 'landing' && (
          <motion.div
            key="landing"
            variants={pageVariants}
            initial="initial"
            animate="animate"
            exit="exit"
            transition={pageTransition}
            style={{ position: 'fixed', inset: 0, overflow: 'auto' }}
          >
            <LandingPage
              onNavigate={navigate}
              documentationUrl={appState.config.documentationUrl}
              settingsAccessPassword={appState.config.settingsAccessPassword}
              onOpenSettings={() => setIsSettingsOpen(true)}
            />
          </motion.div>
        )}

        {page === 'chat' && (
          <motion.div
            key="chat"
            variants={pageVariants}
            initial="initial"
            animate="animate"
            exit="exit"
            transition={pageTransition}
            style={{ position: 'fixed', inset: 0 }}
          >
            <ChatInterface
              config={appState.config}
              conversations={appState.conversations}
              currentId={appState.preferences.currentConversationId}
              workflow={appState.preferences.workflow}
              agentRole={appState.preferences.agentRole}
              mcpToolId={appState.preferences.selectedMcpToolId}
              onConversationsChange={setConversations}
              onCurrentIdChange={(currentConversationId) => updatePreferences({ currentConversationId })}
              onWorkflowChange={(workflow) => updatePreferences({ workflow })}
              onAgentRoleChange={(agentRole) => updatePreferences({ agentRole })}
              onMcpToolIdChange={(selectedMcpToolId) => updatePreferences({ selectedMcpToolId })}
              onConfigChange={setConfig}
              isDark={isDark}
              onToggleDark={() => updatePreferences((prev) => ({ ...prev, darkMode: !prev.darkMode }))}
              onGoHome={() => navigate('landing')}
            />
          </motion.div>
        )}

        {(page === 'dataviz' || page === 'agents') && (
          <motion.div
            key={page}
            variants={pageVariants}
            initial="initial"
            animate="animate"
            exit="exit"
            transition={pageTransition}
            style={{ position: 'fixed', inset: 0, overflow: 'auto' }}
          >
            <ComingSoon page={page} onBack={() => navigate('landing')} />
          </motion.div>
        )}
      </AnimatePresence>

      <SettingsModal
        isOpen={isSettingsOpen}
        onClose={() => setIsSettingsOpen(false)}
        config={appState.config}
        onSave={setConfig}
        onExportDb={exportDbBackup}
        onImportDb={importDbBackup}
        onSyncFromDb={() => syncFromDb({ force: true, showBusy: true })}
        isDbBusy={isDbBusy}
        lastSyncedAt={lastSyncedAt}
        syncError={syncError}
      />
    </>
  );
}
