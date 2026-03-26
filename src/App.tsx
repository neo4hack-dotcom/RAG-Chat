import React, { useCallback, useEffect, useRef, useState } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { ChatInterface } from './components/ChatInterface';
import { SettingsModal } from './components/SettingsModal';
import { LandingPage } from './components/LandingPage';
import { ComingSoon } from './components/ComingSoon';
import { AgentsToolsPage } from './components/AgentsToolsPage';
import {
  AppConfig,
  AppPreferences,
  Conversation,
  DEFAULT_PERSISTED_STATE,
  Page,
  PersistedAppState,
  hasMeaningfulPersistedAppState,
  isAutomationConversation,
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
const APP_STATE_CACHE_KEY_PREFIX = 'ragnarok-app-state-cache';
const CLIENT_USER_ID_KEY = 'ragnarok-client-user-id';
const LEGACY_CONFIG_KEY = 'liquid-ai-config';
const LEGACY_CONVERSATIONS_KEY = 'ragnarok_conversations';
const LEGACY_DARK_KEY = 'ragnarok-dark';

function readPageFromLocation(): Page {
  if (typeof window === 'undefined') return 'landing';
  const pathname = window.location.pathname.replace(/\/+$/, '') || '/';
  const hash = window.location.hash.replace(/^#/, '').replace(/\/+$/, '');
  const candidate = hash || pathname;
  switch (candidate) {
    case '/chat':
      return 'chat';
    case '/dataviz':
      return 'dataviz';
    case '/agents':
      return 'agents';
    case '/admin':
      return 'admin';
    default:
      return 'landing';
  }
}

function urlForPage(page: Page): string {
  switch (page) {
    case 'chat':
      return '/chat';
    case 'dataviz':
      return '/dataviz';
    case 'agents':
      return '/agents';
    case 'admin':
      return '/admin';
    default:
      return '/';
  }
}

function applyRouteToState(state: PersistedAppState): PersistedAppState {
  const routePage = readPageFromLocation();
  if (state.preferences.page === routePage) return state;
  return normalizePersistedAppState({
    ...state,
    preferences: {
      ...state.preferences,
      page: routePage,
    },
  });
}

function getOrCreateClientUserId(): string {
  if (typeof window === 'undefined') return 'anonymous';
  const existing = localStorage.getItem(CLIENT_USER_ID_KEY);
  if (existing) return existing;
  const nextId =
    typeof crypto !== 'undefined' && 'randomUUID' in crypto
      ? `user_${crypto.randomUUID().replace(/-/g, '')}`
      : `user_${Math.random().toString(36).slice(2)}${Date.now().toString(36)}`;
  localStorage.setItem(CLIENT_USER_ID_KEY, nextId);
  return nextId;
}

function appStateCacheKeyForUser(userId: string): string {
  return `${APP_STATE_CACHE_KEY_PREFIX}:${userId}`;
}

function buildDbHeaders(userId: string, includeJson = false): HeadersInit {
  return {
    ...(includeJson ? { 'Content-Type': 'application/json' } : {}),
    'X-RAGnarok-User-Id': userId,
  };
}

function pickPersistedPayload(state: PersistedAppState) {
  return {
    config: state.config,
    conversations: state.conversations.filter((conversation) => !isAutomationConversation(conversation)),
    preferences: state.preferences,
  };
}

function mergeAutomationConversations(local: Conversation[], remote: Conversation[]) {
  const standardConversations = local.filter((conversation) => !isAutomationConversation(conversation));
  const automationConversations = remote
    .filter((conversation) => isAutomationConversation(conversation))
    .sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));
  return [...automationConversations, ...standardConversations];
}

function loadLocalFallbackState(userId: string): PersistedAppState {
  if (typeof window === 'undefined') {
    return normalizePersistedAppState(DEFAULT_PERSISTED_STATE);
  }

  try {
    const cached = localStorage.getItem(appStateCacheKeyForUser(userId));
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

function persistLocalCache(state: PersistedAppState, userId: string) {
  if (typeof window === 'undefined') return;

  localStorage.setItem(appStateCacheKeyForUser(userId), JSON.stringify(state));
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

function AdminAccessScreen({
  password,
  passwordInput,
  error,
  onPasswordChange,
  onSubmit,
  onBack,
}: {
  password: string;
  passwordInput: string;
  error: string;
  onPasswordChange: (value: string) => void;
  onSubmit: () => void;
  onBack: () => void;
}) {
  return (
    <div className="min-h-screen bg-[#f5f5f5] dark:bg-[#0f0f13] flex items-center justify-center px-6">
      <div className="glass-panel max-w-lg w-full rounded-[2rem] p-8 shadow-2xl">
        <div className="w-14 h-14 rounded-2xl bg-gradient-to-tr from-slate-700 to-slate-900 flex items-center justify-center shadow-md shadow-slate-900/20 mb-5">
          <span className="text-white text-xl font-semibold">A</span>
        </div>
        <h1 className="text-2xl font-semibold text-gray-900 dark:text-white">Admin Settings</h1>
        <p className="mt-3 text-sm text-gray-600 dark:text-gray-300 leading-relaxed">
          This route is reserved for configuration. End users do not see any settings button in the main application.
        </p>
        <p className="mt-2 text-xs text-gray-500 dark:text-gray-400">
          Current admin route: <code>/admin</code>
        </p>
        <div className="mt-6 space-y-3">
          <input
            type="password"
            value={passwordInput}
            onChange={(e) => onPasswordChange(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                e.preventDefault();
                onSubmit();
              }
            }}
            autoFocus
            placeholder="Admin password"
            className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none focus:border-slate-400"
          />
          {error && <p className="text-xs text-red-600">{error}</p>}
        </div>
        <div className="mt-6 flex items-center justify-end gap-2">
          <button
            type="button"
            onClick={onBack}
            className="px-4 py-2 rounded-full text-sm font-medium text-gray-500 hover:text-gray-800 hover:bg-gray-100 transition-colors"
          >
            Back to app
          </button>
          <button
            type="button"
            onClick={onSubmit}
            className="px-4 py-2 rounded-full bg-gray-900 text-white text-sm font-medium hover:bg-black transition-colors"
          >
            Unlock settings
          </button>
        </div>
      </div>
    </div>
  );
}

/**
 * Main Application Component
 */
export default function App() {
  const clientUserIdRef = useRef<string>(getOrCreateClientUserId());
  const [appState, setAppState] = useState<PersistedAppState>(() => applyRouteToState(loadLocalFallbackState(clientUserIdRef.current)));
  const [isSettingsOpen, setIsSettingsOpen] = useState(false);
  const [isAdminUnlocked, setIsAdminUnlocked] = useState(false);
  const [adminPasswordInput, setAdminPasswordInput] = useState('');
  const [adminAccessError, setAdminAccessError] = useState('');
  const [isHydrating, setIsHydrating] = useState(true);
  const [isDbBusy, setIsDbBusy] = useState(false);
  const [syncError, setSyncError] = useState<string | null>(null);
  const [lastSyncedAt, setLastSyncedAt] = useState<string | null>(null);

  const latestStateRef = useRef(appState);
  const lastSavedConfigFingerprintRef = useRef(JSON.stringify(appState.config));
  const initialSyncCompleteRef = useRef(false);
  const skipNextPersistRef = useRef(false);
  const isSettingsOpenRef = useRef(isSettingsOpen);

  useEffect(() => {
    latestStateRef.current = appState;
    persistLocalCache(appState, clientUserIdRef.current);
  }, [appState]);

  useEffect(() => {
    isSettingsOpenRef.current = isSettingsOpen;
  }, [isSettingsOpen]);

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
          headers: buildDbHeaders(clientUserIdRef.current, true),
          body: JSON.stringify({
            ...pickPersistedPayload(snapshot),
            include_config: JSON.stringify(snapshot.config) !== lastSavedConfigFingerprintRef.current,
          }),
        });

        if (!response.ok) {
          const errorPayload = await response.json().catch(() => ({}));
          throw new Error(errorPayload.detail || `HTTP ${response.status}`);
        }

        const savedState = normalizePersistedAppState(await response.json());
        const routedSavedState = applyRouteToState(savedState);
        const requestedFingerprint = JSON.stringify(pickPersistedPayload(snapshot));
        const currentFingerprint = JSON.stringify(pickPersistedPayload(latestStateRef.current));

        setLastSyncedAt(routedSavedState.updatedAt ?? null);
        setSyncError(null);
        lastSavedConfigFingerprintRef.current = JSON.stringify(routedSavedState.config);

        if (adoptSavedState && requestedFingerprint === currentFingerprint) {
          skipNextPersistRef.current = true;
          setAppState(routedSavedState);
        }

        return routedSavedState;
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
        const response = await fetch(DB_STATE_URL, {
          headers: buildDbHeaders(clientUserIdRef.current),
        });
        if (!response.ok) {
          const errorPayload = await response.json().catch(() => ({}));
          throw new Error(errorPayload.detail || `HTTP ${response.status}`);
        }

        const remoteState = applyRouteToState(normalizePersistedAppState(await response.json()));
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
        lastSavedConfigFingerprintRef.current = JSON.stringify(remoteState.config);
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
        headers: buildDbHeaders(clientUserIdRef.current, true),
        body: JSON.stringify(snapshot),
      });

      if (!response.ok) {
        const errorPayload = await response.json().catch(() => ({}));
        throw new Error(errorPayload.detail || `HTTP ${response.status}`);
      }

      const importedState = applyRouteToState(normalizePersistedAppState(await response.json()));
      skipNextPersistRef.current = true;
      initialSyncCompleteRef.current = true;
      setAppState(importedState);
      setLastSyncedAt(importedState.updatedAt ?? null);
      setSyncError(null);
      lastSavedConfigFingerprintRef.current = JSON.stringify(importedState.config);
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
      if (isSettingsOpenRef.current) return;
      void syncFromDb({ force: true, showBusy: false });
    };

    const handleVisibilityChange = () => {
      if (isSettingsOpenRef.current) return;
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
    const syncRoute = () => {
      const routePage = readPageFromLocation();
      setAppState((prev) => (
        prev.preferences.page === routePage
          ? prev
          : normalizePersistedAppState({
              ...prev,
              preferences: {
                ...prev.preferences,
                page: routePage,
              },
            })
      ));
    };

    window.addEventListener('popstate', syncRoute);
    window.addEventListener('hashchange', syncRoute);
    return () => {
      window.removeEventListener('popstate', syncRoute);
      window.removeEventListener('hashchange', syncRoute);
    };
  }, []);

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
  const effectiveSettingsPassword = appState.config.settingsAccessPassword || 'MM@2026';

  useEffect(() => {
    if (page !== 'chat') return;

    const syncAutomationConversations = async () => {
      if (document.visibilityState !== 'visible') return;
      if (isSettingsOpenRef.current) return;

      try {
        const response = await fetch(DB_STATE_URL, {
          headers: buildDbHeaders(clientUserIdRef.current),
        });
        if (!response.ok) return;

        const remoteState = applyRouteToState(normalizePersistedAppState(await response.json()));
        const localState = latestStateRef.current;
        const remoteAutomationConversations = remoteState.conversations.filter((conversation) => isAutomationConversation(conversation));
        const localAutomationConversations = localState.conversations.filter((conversation) => isAutomationConversation(conversation));

        const remoteFingerprint = JSON.stringify(remoteAutomationConversations);
        const localFingerprint = JSON.stringify(localAutomationConversations);
        if (remoteFingerprint === localFingerprint) return;

        const newestRemoteAutomation = [...remoteAutomationConversations].sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0))[0] ?? null;
        const newestLocalAutomation = [...localAutomationConversations].sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0))[0] ?? null;
        const hasFreshAutomationUpdate =
          Boolean(newestRemoteAutomation)
          && ((newestRemoteAutomation?.updatedAt || 0) > (newestLocalAutomation?.updatedAt || 0));

        skipNextPersistRef.current = true;
        setAppState((prev) =>
          normalizePersistedAppState({
            ...prev,
            conversations: mergeAutomationConversations(prev.conversations, remoteState.conversations),
            preferences: {
              ...prev.preferences,
              currentConversationId:
                hasFreshAutomationUpdate && newestRemoteAutomation
                  ? newestRemoteAutomation.id
                  : prev.preferences.currentConversationId,
            },
            updatedAt: remoteState.updatedAt,
          })
        );
      } catch {
        // Keep background automation sync quiet to avoid UI noise.
      }
    };

    void syncAutomationConversations();
    const intervalId = window.setInterval(syncAutomationConversations, 8000);
    return () => window.clearInterval(intervalId);
  }, [page]);

  useEffect(() => {
    if (page === 'chat' && isDark) {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
  }, [isDark, page]);

  const navigate = useCallback((target: Page) => {
    if (typeof window !== 'undefined') {
      const nextUrl = urlForPage(target);
      const currentUrl = `${window.location.pathname}${window.location.search}`;
      if (currentUrl !== nextUrl) {
        window.history.pushState({}, '', nextUrl);
      }
    }
    updatePreferences({ page: target });
    if (target !== 'admin') {
      setIsSettingsOpen(false);
      setIsAdminUnlocked(false);
      setAdminPasswordInput('');
      setAdminAccessError('');
    }
  }, [updatePreferences]);

  useEffect(() => {
    if (page === 'admin' && isAdminUnlocked && !isSettingsOpen) {
      setIsSettingsOpen(true);
    }
  }, [page, isAdminUnlocked, isSettingsOpen]);

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
              agenticDataVizUrl={appState.config.agenticDataVizUrl}
              portalAppsCount={appState.config.portalApps.filter((app) => app.name.trim() && app.url.trim()).length}
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
              selectedCustomAgentId={appState.preferences.selectedCustomAgentId}
              onConversationsChange={setConversations}
              onCurrentIdChange={(currentConversationId) => updatePreferences({ currentConversationId })}
              onWorkflowChange={(workflow) => updatePreferences({ workflow })}
              onAgentRoleChange={(agentRole) => updatePreferences({ agentRole })}
              onMcpToolIdChange={(selectedMcpToolId) => updatePreferences({ selectedMcpToolId })}
              onSelectedCustomAgentIdChange={(selectedCustomAgentId) => updatePreferences({ selectedCustomAgentId })}
              onConfigChange={setConfig}
              isDark={isDark}
              onToggleDark={() => updatePreferences((prev) => ({ ...prev, darkMode: !prev.darkMode }))}
              onGoHome={() => navigate('landing')}
            />
          </motion.div>
        )}

        {page === 'dataviz' && (
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

        {page === 'agents' && (
          <motion.div
            key="agents"
            variants={pageVariants}
            initial="initial"
            animate="animate"
            exit="exit"
            transition={pageTransition}
            style={{ position: 'fixed', inset: 0, overflow: 'auto' }}
          >
            <AgentsToolsPage
              apps={appState.config.portalApps}
              onBack={() => navigate('landing')}
            />
          </motion.div>
        )}

        {page === 'admin' && (
          <motion.div
            key="admin"
            variants={pageVariants}
            initial="initial"
            animate="animate"
            exit="exit"
            transition={pageTransition}
            style={{ position: 'fixed', inset: 0, overflow: 'auto' }}
          >
            <AdminAccessScreen
              password={effectiveSettingsPassword}
              passwordInput={adminPasswordInput}
              error={adminAccessError}
              onPasswordChange={(value) => {
                setAdminPasswordInput(value);
                if (adminAccessError) setAdminAccessError('');
              }}
              onSubmit={() => {
                if (adminPasswordInput === effectiveSettingsPassword) {
                  setAdminAccessError('');
                  setAdminPasswordInput('');
                  setIsAdminUnlocked(true);
                  setIsSettingsOpen(true);
                  return;
                }
                setAdminAccessError('Incorrect password.');
              }}
              onBack={() => navigate('landing')}
            />
          </motion.div>
        )}
      </AnimatePresence>

      <SettingsModal
        isOpen={isSettingsOpen}
        onClose={() => {
          setIsSettingsOpen(false);
          if (page === 'admin') {
            setIsAdminUnlocked(false);
            navigate('landing');
          }
        }}
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
