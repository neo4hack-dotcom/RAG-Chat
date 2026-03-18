import React, { useState, useEffect } from 'react';
import { ChatInterface } from './components/ChatInterface';
import { SettingsModal } from './components/SettingsModal';
import { AppConfig, DEFAULT_CONFIG } from './lib/utils';

export default function App() {
  const [isSettingsOpen, setIsSettingsOpen] = useState(false);
  
  // Load config from localStorage or use default
  const [config, setConfig] = useState<AppConfig>(() => {
    const saved = localStorage.getItem('liquid-ai-config');
    if (saved) {
      try {
        return JSON.parse(saved);
      } catch (e) {
        console.error("Failed to parse config", e);
      }
    }
    return DEFAULT_CONFIG;
  });

  // Save config to localStorage whenever it changes
  useEffect(() => {
    localStorage.setItem('liquid-ai-config', JSON.stringify(config));
  }, [config]);

  return (
    <>
      <ChatInterface
        config={config}
        onOpenSettings={() => setIsSettingsOpen(true)}
      />
      <SettingsModal
        isOpen={isSettingsOpen}
        onClose={() => setIsSettingsOpen(false)}
        config={config}
        onSave={setConfig}
      />
    </>
  );
}
