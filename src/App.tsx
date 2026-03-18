import React, { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { ChatInterface } from './components/ChatInterface';
import { SettingsModal } from './components/SettingsModal';
import { LandingPage } from './components/LandingPage';
import { ComingSoon } from './components/ComingSoon';
import { AppConfig, DEFAULT_CONFIG } from './lib/utils';

type Page = 'landing' | 'chat' | 'dataviz' | 'agents';

const pageVariants = {
  initial: { opacity: 0, scale: 0.97, filter: 'blur(4px)' },
  animate: { opacity: 1, scale: 1, filter: 'blur(0px)' },
  exit:    { opacity: 0, scale: 1.01, filter: 'blur(4px)' },
};

const pageTransition = { duration: 0.45, ease: [0.16, 1, 0.3, 1] };

/**
 * Main Application Component
 */
export default function App() {
  const [page, setPage] = useState<Page>('landing');
  const [isSettingsOpen, setIsSettingsOpen] = useState(false);

  // Dark mode — only active inside the chat
  const [isDark, setIsDark] = useState<boolean>(() => {
    return localStorage.getItem('ragnarok-dark') === 'true';
  });

  useEffect(() => {
    if (page === 'chat' && isDark) {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
    localStorage.setItem('ragnarok-dark', String(isDark));
  }, [isDark, page]);

  const [config, setConfig] = useState<AppConfig>(() => {
    const saved = localStorage.getItem('liquid-ai-config');
    if (saved) {
      try { return JSON.parse(saved); } catch { /* ignore */ }
    }
    return DEFAULT_CONFIG;
  });

  useEffect(() => {
    localStorage.setItem('liquid-ai-config', JSON.stringify(config));
  }, [config]);

  const navigate = (target: Page) => setPage(target);

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
            <LandingPage onNavigate={navigate} />
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
              config={config}
              onOpenSettings={() => setIsSettingsOpen(true)}
              isDark={isDark}
              onToggleDark={() => setIsDark(d => !d)}
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
        config={config}
        onSave={setConfig}
      />
    </>
  );
}
