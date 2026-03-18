import React, { useState } from "react";
import { Settings, X, Save, Server, Key, Bot, MessageSquare, RefreshCw, CheckCircle2, XCircle, Zap, Loader2, Database, Layers, SlidersHorizontal, Network, Plus, Trash2 } from "lucide-react";
import { AppConfig, McpTool } from "../lib/utils";

interface SettingsModalProps {
  isOpen: boolean;
  onClose: () => void;
  config: AppConfig;
  onSave: (config: AppConfig) => void;
}

/**
 * SettingsModal Component
 * Provides a UI for configuring application settings, including LLM provider details,
 * RAG parameters (Elasticsearch, Embeddings), and system prompts.
 */
export function SettingsModal({ isOpen, onClose, config, onSave }: SettingsModalProps) {
  // Local state to hold configuration changes before saving
  const [localConfig, setLocalConfig] = useState<AppConfig>({
    provider: config.provider || 'ollama',
    baseUrl: config.baseUrl || (config as any).endpoint || 'http://localhost:11434',
    apiKey: config.apiKey || '',
    model: config.model || '',
    systemPrompt: config.systemPrompt || '',
    elasticsearchUrl: config.elasticsearchUrl || 'http://localhost:9200',
    elasticsearchIndex: config.elasticsearchIndex || 'rag_documents',
    elasticsearchUsername: config.elasticsearchUsername || '',
    elasticsearchPassword: config.elasticsearchPassword || '',
    embeddingModel: config.embeddingModel || 'nomic-embed-text',
    chunkSize: config.chunkSize || 512,
    chunkOverlap: config.chunkOverlap || 50,
    knnNeighbors: config.knnNeighbors || 50,
    mcpTools: config.mcpTools ?? [],
  });
  
  // State for available models fetched from the provider
  const [models, setModels] = useState<string[]>([]);
  const [isRefreshing, setIsRefreshing] = useState(false);

  // State for available embedding models
  const [embeddingModels, setEmbeddingModels] = useState<string[]>([]);
  const [isRefreshingEmbed, setIsRefreshingEmbed] = useState(false);
  
  // Connection test states for LLM
  const [testStatus, setTestStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
  const [testMessage, setTestMessage] = useState('');
  
  // Tab state (LLM, RAG, or MCP settings)
  const [activeTab, setActiveTab] = useState<'llm' | 'rag' | 'mcp'>('llm');

  // Connection test states for Elasticsearch
  const [esTestStatus, setEsTestStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
  const [esTestMessage, setEsTestMessage] = useState('');
  
  // Connection test states for Embedding model
  const [embedTestStatus, setEmbedTestStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
  const [embedTestMessage, setEmbedTestMessage] = useState('');

  if (!isOpen) return null;

  // Test connection to the Elasticsearch instance
  const testElasticsearchConnection = async () => {
    setEsTestStatus('testing');
    setEsTestMessage('');
    try {
      const url = localConfig.elasticsearchUrl.replace(/\/$/, '');
      const headers: Record<string, string> = {};
      if (localConfig.elasticsearchUsername) {
        const credentials = btoa(`${localConfig.elasticsearchUsername}:${localConfig.elasticsearchPassword}`);
        headers['Authorization'] = `Basic ${credentials}`;
      }
      const response = await fetch(`${url}/`, { headers });
      if (!response.ok) throw new Error(`HTTP Error: ${response.status}`);
      const data = await response.json();
      setEsTestStatus('success');
      setEsTestMessage(`Connected to cluster: ${data.cluster_name || 'Elasticsearch'}`);
    } catch (err) {
      console.error("ES connection error:", err);
      setEsTestStatus('error');
      setEsTestMessage(err instanceof Error ? err.message : 'Failed to connect');
    }
  };

  // Test connection to the Embedding model provider
  const testEmbeddingConnection = async () => {
    setEmbedTestStatus('testing');
    setEmbedTestMessage('');
    try {
      const baseUrl = localConfig.embeddingBaseUrl.replace(/\/$/, '');
      const headers: Record<string, string> = {};
      if (localConfig.embeddingApiKey) {
        headers['Authorization'] = `Bearer ${localConfig.embeddingApiKey}`;
      }
      const response = await fetch(`${baseUrl}/models`, { headers });
      if (!response.ok) throw new Error(`HTTP Error: ${response.status}`);
      const data = await response.json();
      const modelsCount = data.data?.length || 0;
      setEmbedTestStatus('success');
      setEmbedTestMessage(`Connected! Found ${modelsCount} models.`);
    } catch (err) {
      console.error("Embedding connection error:", err);
      setEmbedTestStatus('error');
      setEmbedTestMessage(err instanceof Error ? err.message : 'Failed to connect');
    }
  };

  // Fetch available models from the configured embedding provider
  const fetchEmbeddingModels = async () => {
    setIsRefreshingEmbed(true);
    try {
      const baseUrl = localConfig.embeddingBaseUrl.replace(/\/$/, '');
      const headers: Record<string, string> = {};
      if (localConfig.embeddingApiKey) {
        headers['Authorization'] = `Bearer ${localConfig.embeddingApiKey}`;
      }
      const response = await fetch(`${baseUrl}/models`, { headers });
      if (!response.ok) throw new Error(`HTTP Error: ${response.status}`);
      const data = await response.json();
      const fetched: string[] = data.data?.map((m: any) => m.id) || data.models?.map((m: any) => m.name) || [];
      setEmbeddingModels(fetched);
      if (fetched.length > 0 && !fetched.includes(localConfig.embeddingModel)) {
        setLocalConfig(prev => ({ ...prev, embeddingModel: fetched[0] }));
      }
    } catch (err) {
      console.error("Error fetching embedding models:", err);
    } finally {
      setIsRefreshingEmbed(false);
    }
  };

  // Fetch available models from the configured LLM provider
  const fetchModels = async (testConnection = false) => {
    if (testConnection) {
      setTestStatus('testing');
      setTestMessage('');
    } else {
      setIsRefreshing(true);
    }

    try {
      const baseUrl = localConfig.baseUrl.replace(/\/$/, '');
      let url = '';
      
      if (localConfig.provider === 'ollama') {
        url = `${baseUrl}/api/tags`;
      } else {
        url = `${baseUrl}/models`;
      }

      const headers: Record<string, string> = {};
      if (localConfig.apiKey) {
        headers['Authorization'] = `Bearer ${localConfig.apiKey}`;
      }

      const response = await fetch(url, { headers });
      
      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status}`);
      }

      const data = await response.json();
      let fetchedModels: string[] = [];

      if (localConfig.provider === 'ollama') {
        fetchedModels = data.models?.map((m: any) => m.name) || [];
      } else {
        fetchedModels = data.data?.map((m: any) => m.id) || [];
      }

      setModels(fetchedModels);
      
      if (testConnection) {
        setTestStatus('success');
        setTestMessage(`Connected! Found ${fetchedModels.length} models.`);
      }
      
      // Auto-select first model if none selected
      if (fetchedModels.length > 0 && !fetchedModels.includes(localConfig.model)) {
        setLocalConfig(prev => ({ ...prev, model: fetchedModels[0] }));
      }
      
    } catch (err) {
      console.error("Error fetching models:", err);
      if (testConnection) {
        setTestStatus('error');
        setTestMessage(err instanceof Error ? err.message : 'Failed to connect');
      }
    } finally {
      setIsRefreshing(false);
    }
  };

  // Handle saving the configuration and closing the modal
  const handleSave = () => {
    onSave(localConfig);
    onClose();
  };

  return (
    <>
      <div
        onClick={onClose}
        className="fixed inset-0 bg-black/20 backdrop-blur-sm z-40 animate-fade-in"
      />
      <div className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-full max-w-2xl z-50 p-6 animate-scale-in">
        <div className="glass-panel rounded-[2rem] p-8 shadow-2xl max-h-[90vh] overflow-y-auto">
          <div className="flex items-center justify-between mb-6">
            <h2 className="text-2xl font-semibold flex items-center gap-3">
              <Settings className="w-6 h-6 text-blue-500" />
              Configuration
            </h2>
            <button
              onClick={onClose}
              className="p-2 rounded-full hover:bg-black/5 transition-colors"
            >
              <X className="w-5 h-5 text-gray-500" />
            </button>
          </div>

          <div className="flex gap-4 border-b border-gray-200 mb-6">
            <button
              onClick={() => setActiveTab('llm')}
              className={`pb-3 px-2 text-sm font-medium transition-colors border-b-2 ${activeTab === 'llm' ? 'border-blue-500 text-blue-600' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
            >
              LLM Settings
            </button>
            <button
              onClick={() => setActiveTab('rag')}
              className={`pb-3 px-2 text-sm font-medium transition-colors border-b-2 ${activeTab === 'rag' ? 'border-blue-500 text-blue-600' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
            >
              RAG & Elasticsearch
            </button>
            <button
              onClick={() => setActiveTab('mcp')}
              className={`pb-3 px-2 text-sm font-medium transition-colors border-b-2 ${activeTab === 'mcp' ? 'border-teal-500 text-teal-600' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
            >
              MCP Tools
            </button>
          </div>

          {activeTab === 'mcp' ? (
            <div className="space-y-5">
              <div className="flex items-center justify-between">
                <h3 className="text-sm font-semibold text-gray-900 flex items-center gap-2">
                  <Network className="w-4 h-4 text-teal-500" /> MCP Tools
                </h3>
                <button
                  onClick={() => {
                    const newTool: McpTool = { id: `mcp_${Date.now()}`, label: 'New Tool', url: '' };
                    setLocalConfig(prev => ({ ...prev, mcpTools: [...(prev.mcpTools ?? []), newTool] }));
                  }}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium bg-teal-50 text-teal-700 border border-teal-200 rounded-xl hover:bg-teal-100 transition-colors"
                >
                  <Plus className="w-3.5 h-3.5" /> Add Tool
                </button>
              </div>
              <p className="text-xs text-gray-500 -mt-3">Ces outils apparaissent dans le dropdown du bouton MCP dans l'interface de chat.</p>

              {(localConfig.mcpTools ?? []).length === 0 && (
                <div className="text-center py-8 text-sm text-gray-400 border border-dashed border-gray-200 rounded-xl">
                  Aucun outil MCP. Cliquez sur « Add Tool » pour en ajouter.
                </div>
              )}

              <div className="space-y-3">
                {(localConfig.mcpTools ?? []).map((tool: McpTool, idx: number) => (
                  <div key={tool.id} className="flex items-start gap-3 p-3 bg-white/50 border border-gray-200 rounded-xl">
                    <div className="flex-1 grid grid-cols-2 gap-2">
                      <div>
                        <label className="text-xs font-medium text-gray-500 mb-1 block">Label</label>
                        <input
                          type="text"
                          value={tool.label}
                          onChange={(e) => {
                            const updated = [...(localConfig.mcpTools ?? [])];
                            updated[idx] = { ...tool, label: e.target.value };
                            setLocalConfig(prev => ({ ...prev, mcpTools: updated }));
                          }}
                          className="w-full bg-white border border-gray-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-teal-500/50 transition-all"
                          placeholder="Mon outil MCP"
                        />
                      </div>
                      <div>
                        <label className="text-xs font-medium text-gray-500 mb-1 block">URL</label>
                        <input
                          type="text"
                          value={tool.url}
                          onChange={(e) => {
                            const updated = [...(localConfig.mcpTools ?? [])];
                            updated[idx] = { ...tool, url: e.target.value };
                            setLocalConfig(prev => ({ ...prev, mcpTools: updated }));
                          }}
                          className="w-full bg-white border border-gray-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-teal-500/50 transition-all"
                          placeholder="http://localhost:3000"
                        />
                      </div>
                    </div>
                    <button
                      onClick={() => {
                        const updated = (localConfig.mcpTools ?? []).filter((_: McpTool, i: number) => i !== idx);
                        setLocalConfig(prev => ({ ...prev, mcpTools: updated }));
                      }}
                      className="mt-5 p-1.5 text-red-400 hover:text-red-600 hover:bg-red-50 rounded-lg transition-colors"
                    >
                      <Trash2 className="w-4 h-4" />
                    </button>
                  </div>
                ))}
              </div>
            </div>
          ) : activeTab === 'llm' ? (
            <div className="space-y-5">
                <div>
                  <label className="flex items-center gap-2 text-sm font-medium text-gray-700 mb-2">
                    <Zap className="w-4 h-4" /> Provider
                  </label>
                  <div className="flex gap-6 items-center bg-white/50 border border-gray-200 rounded-xl px-4 py-3">
                    <label className="flex items-center gap-2 cursor-pointer text-sm font-medium">
                      <input 
                        type="radio" 
                        name="provider" 
                        value="ollama" 
                        checked={localConfig.provider === 'ollama'}
                        onChange={() => setLocalConfig({ ...localConfig, provider: 'ollama', baseUrl: 'http://localhost:11434' })}
                        className="text-blue-500 focus:ring-blue-500"
                      />
                      Ollama
                    </label>
                    <label className="flex items-center gap-2 cursor-pointer text-sm font-medium">
                      <input 
                        type="radio" 
                        name="provider" 
                        value="openai" 
                        checked={localConfig.provider === 'openai'}
                        onChange={() => setLocalConfig({ ...localConfig, provider: 'openai', baseUrl: 'http://localhost:1234/v1' })}
                        className="text-blue-500 focus:ring-blue-500"
                      />
                      OpenAI Compatible
                    </label>
                  </div>
                </div>

                <div>
                  <label className="flex items-center gap-2 text-sm font-medium text-gray-700 mb-2">
                    <Server className="w-4 h-4" /> Base URL
                  </label>
                  <div className="flex gap-2">
                    <input
                      type="text"
                      value={localConfig.baseUrl}
                      onChange={(e) => setLocalConfig({ ...localConfig, baseUrl: e.target.value })}
                      className="flex-1 bg-white/50 border border-gray-200 rounded-xl px-4 py-3 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                      placeholder={localConfig.provider === 'ollama' ? "http://localhost:11434" : "http://localhost:1234/v1"}
                    />
                    <button 
                      onClick={() => fetchModels(true)}
                      disabled={testStatus === 'testing'}
                      className="px-4 py-2 bg-gray-100 hover:bg-gray-200 text-gray-700 rounded-xl font-medium transition-colors flex items-center gap-2 whitespace-nowrap text-sm"
                    >
                      {testStatus === 'testing' ? <Loader2 className="w-4 h-4 animate-spin" /> : 'Test Connection'}
                    </button>
                  </div>
                  {testStatus === 'success' && <p className="text-emerald-600 text-xs mt-2 flex items-center gap-1"><CheckCircle2 className="w-3 h-3"/> {testMessage}</p>}
                  {testStatus === 'error' && <p className="text-red-600 text-xs mt-2 flex items-center gap-1"><XCircle className="w-3 h-3"/> {testMessage}</p>}
                </div>

                {localConfig.provider === 'openai' && (
                  <div>
                    <label className="flex items-center gap-2 text-sm font-medium text-gray-700 mb-2">
                      <Key className="w-4 h-4" /> API Key (Optional for local)
                    </label>
                    <input
                      type="password"
                      value={localConfig.apiKey}
                      onChange={(e) => setLocalConfig({ ...localConfig, apiKey: e.target.value })}
                      className="w-full bg-white/50 border border-gray-200 rounded-xl px-4 py-3 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                      placeholder="sk-..."
                    />
                  </div>
                )}

                <div>
                  <div className="flex items-center justify-between mb-2">
                    <label className="flex items-center gap-2 text-sm font-medium text-gray-700">
                      <Bot className="w-4 h-4" /> Model Name
                    </label>
                    <button 
                      onClick={() => fetchModels(false)}
                      className="text-blue-500 hover:text-blue-600 flex items-center gap-1 text-xs font-medium"
                    >
                      <RefreshCw className={`w-3 h-3 ${isRefreshing ? 'animate-spin' : ''}`} /> Refresh Models
                    </button>
                  </div>
                  {models.length > 0 ? (
                    <select
                      value={localConfig.model}
                      onChange={(e) => setLocalConfig({ ...localConfig, model: e.target.value })}
                      className="w-full bg-white/50 border border-gray-200 rounded-xl px-4 py-3 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all appearance-none"
                    >
                      {models.map(m => <option key={m} value={m}>{m}</option>)}
                    </select>
                  ) : (
                    <input
                      type="text"
                      value={localConfig.model}
                      onChange={(e) => setLocalConfig({ ...localConfig, model: e.target.value })}
                      className="w-full bg-white/50 border border-gray-200 rounded-xl px-4 py-3 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                      placeholder="llama3, gpt-4, etc."
                    />
                  )}
                </div>

                <div>
                  <label className="flex items-center gap-2 text-sm font-medium text-gray-700 mb-2">
                    <MessageSquare className="w-4 h-4" /> System Prompt
                  </label>
                  <textarea
                    value={localConfig.systemPrompt}
                    onChange={(e) => setLocalConfig({ ...localConfig, systemPrompt: e.target.value })}
                    className="w-full bg-white/50 border border-gray-200 rounded-xl px-4 py-3 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all min-h-[100px] resize-none"
                    placeholder="You are a helpful assistant..."
                  />
                </div>
            </div>
          ) : (
            <div className="space-y-5">
              <div>
                <label className="flex items-center gap-2 text-sm font-medium text-gray-700 mb-2">
                  <Database className="w-4 h-4" /> Elasticsearch URL
                </label>
                <div className="flex gap-2">
                  <input
                    type="text"
                    value={localConfig.elasticsearchUrl}
                    onChange={(e) => setLocalConfig({ ...localConfig, elasticsearchUrl: e.target.value })}
                    className="flex-1 bg-white/50 border border-gray-200 rounded-xl px-4 py-3 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                    placeholder="http://localhost:9200"
                  />
                  <button 
                    onClick={testElasticsearchConnection}
                    disabled={esTestStatus === 'testing'}
                    className="px-4 py-2 bg-gray-100 hover:bg-gray-200 text-gray-700 rounded-xl font-medium transition-colors flex items-center gap-2 whitespace-nowrap text-sm"
                  >
                    {esTestStatus === 'testing' ? <Loader2 className="w-4 h-4 animate-spin" /> : 'Test Connection'}
                  </button>
                </div>
                {esTestStatus === 'success' && <p className="text-emerald-600 text-xs mt-2 flex items-center gap-1"><CheckCircle2 className="w-3 h-3"/> {esTestMessage}</p>}
                {esTestStatus === 'error' && <p className="text-red-600 text-xs mt-2 flex items-center gap-1"><XCircle className="w-3 h-3"/> {esTestMessage}</p>}
              </div>

              <div>
                <label className="flex items-center gap-2 text-sm font-medium text-gray-700 mb-2">
                  <Layers className="w-4 h-4" /> Elasticsearch Index
                </label>
                <input
                  type="text"
                  value={localConfig.elasticsearchIndex}
                  onChange={(e) => setLocalConfig({ ...localConfig, elasticsearchIndex: e.target.value })}
                  className="w-full bg-white/50 border border-gray-200 rounded-xl px-4 py-3 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                  placeholder="rag_documents"
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="flex items-center gap-2 text-sm font-medium text-gray-700 mb-2">
                    <Key className="w-4 h-4" /> Username
                  </label>
                  <input
                    type="text"
                    value={localConfig.elasticsearchUsername}
                    onChange={(e) => setLocalConfig({ ...localConfig, elasticsearchUsername: e.target.value })}
                    className="w-full bg-white/50 border border-gray-200 rounded-xl px-4 py-3 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                    placeholder="elastic"
                    autoComplete="username"
                  />
                </div>
                <div>
                  <label className="flex items-center gap-2 text-sm font-medium text-gray-700 mb-2">
                    <Key className="w-4 h-4" /> Password
                  </label>
                  <input
                    type="password"
                    value={localConfig.elasticsearchPassword}
                    onChange={(e) => setLocalConfig({ ...localConfig, elasticsearchPassword: e.target.value })}
                    className="w-full bg-white/50 border border-gray-200 rounded-xl px-4 py-3 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                    placeholder="••••••••"
                    autoComplete="current-password"
                  />
                </div>
              </div>

              <div className="pt-4 border-t border-gray-100">
                <h3 className="text-sm font-semibold text-gray-900 mb-4 flex items-center gap-2">
                  <Bot className="w-4 h-4 text-blue-500" /> Embedding Model Configuration
                </h3>
                
                <div className="space-y-4">
                  <div>
                    <label className="flex items-center gap-2 text-sm font-medium text-gray-700 mb-2">
                      <Server className="w-4 h-4" /> OpenAI-Compatible Base URL
                    </label>
                    <div className="flex gap-2">
                      <input
                        type="text"
                        value={localConfig.embeddingBaseUrl}
                        onChange={(e) => setLocalConfig({ ...localConfig, embeddingBaseUrl: e.target.value })}
                        className="flex-1 bg-white/50 border border-gray-200 rounded-xl px-4 py-3 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                        placeholder="http://localhost:11434/v1"
                      />
                      <button 
                        onClick={testEmbeddingConnection}
                        disabled={embedTestStatus === 'testing'}
                        className="px-4 py-2 bg-gray-100 hover:bg-gray-200 text-gray-700 rounded-xl font-medium transition-colors flex items-center gap-2 whitespace-nowrap text-sm"
                      >
                        {embedTestStatus === 'testing' ? <Loader2 className="w-4 h-4 animate-spin" /> : 'Test Connection'}
                      </button>
                    </div>
                    {embedTestStatus === 'success' && <p className="text-emerald-600 text-xs mt-2 flex items-center gap-1"><CheckCircle2 className="w-3 h-3"/> {embedTestMessage}</p>}
                    {embedTestStatus === 'error' && <p className="text-red-600 text-xs mt-2 flex items-center gap-1"><XCircle className="w-3 h-3"/> {embedTestMessage}</p>}
                  </div>

                  <div>
                    <label className="flex items-center gap-2 text-sm font-medium text-gray-700 mb-2">
                      <Key className="w-4 h-4" /> API Key (Optional)
                    </label>
                    <input
                      type="password"
                      value={localConfig.embeddingApiKey}
                      onChange={(e) => setLocalConfig({ ...localConfig, embeddingApiKey: e.target.value })}
                      className="w-full bg-white/50 border border-gray-200 rounded-xl px-4 py-3 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                      placeholder="sk-..."
                    />
                  </div>

                  <div>
                    <div className="flex items-center justify-between mb-2">
                      <label className="flex items-center gap-2 text-sm font-medium text-gray-700">
                        <Bot className="w-4 h-4" /> Model Name
                      </label>
                      <button
                        onClick={fetchEmbeddingModels}
                        className="text-blue-500 hover:text-blue-600 flex items-center gap-1 text-xs font-medium"
                      >
                        <RefreshCw className={`w-3 h-3 ${isRefreshingEmbed ? 'animate-spin' : ''}`} /> Refresh Models
                      </button>
                    </div>
                    {embeddingModels.length > 0 ? (
                      <select
                        value={localConfig.embeddingModel}
                        onChange={(e) => setLocalConfig({ ...localConfig, embeddingModel: e.target.value })}
                        className="w-full bg-white/50 border border-gray-200 rounded-xl px-4 py-3 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all appearance-none"
                      >
                        {embeddingModels.map(m => <option key={m} value={m}>{m}</option>)}
                      </select>
                    ) : (
                      <input
                        type="text"
                        value={localConfig.embeddingModel}
                        onChange={(e) => setLocalConfig({ ...localConfig, embeddingModel: e.target.value })}
                        className="w-full bg-white/50 border border-gray-200 rounded-xl px-4 py-3 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                        placeholder="nomic-embed-text"
                      />
                    )}
                    <p className="text-xs text-gray-500 mt-1">Used to vectorize user queries locally.</p>
                  </div>
                </div>
              </div>

              <div className="pt-4 border-t border-gray-100 grid grid-cols-3 gap-4">
                <div>
                  <label className="flex items-center gap-2 text-sm font-medium text-gray-700 mb-2">
                    <SlidersHorizontal className="w-4 h-4" /> Chunk Size
                  </label>
                  <input
                    type="number"
                    value={localConfig.chunkSize}
                    onChange={(e) => setLocalConfig({ ...localConfig, chunkSize: parseInt(e.target.value) || 512 })}
                    className="w-full bg-white/50 border border-gray-200 rounded-xl px-4 py-3 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                  />
                </div>
                <div>
                  <label className="flex items-center gap-2 text-sm font-medium text-gray-700 mb-2">
                    <SlidersHorizontal className="w-4 h-4" /> Chunk Overlap
                  </label>
                  <input
                    type="number"
                    value={localConfig.chunkOverlap}
                    onChange={(e) => setLocalConfig({ ...localConfig, chunkOverlap: parseInt(e.target.value) || 50 })}
                    className="w-full bg-white/50 border border-gray-200 rounded-xl px-4 py-3 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                  />
                </div>
                <div>
                  <label className="flex items-center gap-2 text-sm font-medium text-gray-700 mb-2">
                    <SlidersHorizontal className="w-4 h-4" /> KNN Neighbors
                  </label>
                  <input
                    type="number"
                    value={localConfig.knnNeighbors}
                    onChange={(e) => setLocalConfig({ ...localConfig, knnNeighbors: parseInt(e.target.value) || 50 })}
                    className="w-full bg-white/50 border border-gray-200 rounded-xl px-4 py-3 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                  />
                </div>
              </div>
            </div>
          )}

          <div className="mt-8 flex justify-end">
                <button
                  onClick={handleSave}
                  className="bg-black text-white px-6 py-3 rounded-xl font-medium flex items-center gap-2 hover:bg-gray-800 transition-colors shadow-lg shadow-black/10"
                >
                  <Save className="w-4 h-4" />
                  Save Configuration
                </button>
              </div>
            </div>
          </div>
        </>
  );
}
