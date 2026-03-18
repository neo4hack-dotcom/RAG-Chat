import React, { useState, useEffect } from 'react';
import { ChatInterface } from './components/ChatInterface';
import { SettingsModal } from './components/SettingsModal';
import { AppConfig, DEFAULT_CONFIG } from './lib/utils';

/**
 * Main Application Component
 * Manages the global state for application configuration and settings modal visibility.
 */
export default function App() {
  // State to control the visibility of the settings modal
  const [isSettingsOpen, setIsSettingsOpen] = useState(false);
  
  // Load configuration from localStorage on initial render, or fallback to DEFAULT_CONFIG
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

  // Persist configuration to localStorage whenever it changes
  useEffect(() => {
    localStorage.setItem('liquid-ai-config', JSON.stringify(config));
  }, [config]);

  return (
    <>
      {/* Main Chat Interface */}
      <ChatInterface
        config={config}
        onOpenSettings={() => setIsSettingsOpen(true)}
      />
      
      {/* Settings Modal overlay */}
      <SettingsModal
        isOpen={isSettingsOpen}
        onClose={() => setIsSettingsOpen(false)}
        config={config}
        onSave={setConfig}
      />
    </>
  );
}
