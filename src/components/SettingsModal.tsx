import React, { useEffect, useRef, useState } from "react";
import { Settings, X, Save, Server, Key, Bot, MessageSquare, RefreshCw, CheckCircle2, XCircle, Zap, Loader2, Database, Layers, SlidersHorizontal, Network, Plus, Trash2, FolderOpen, UploadCloud, Download, Send } from "lucide-react";
import { AppConfig, McpTool, BuiltInAgentRole } from "../lib/utils";

const BUILT_IN_AGENT_OPTIONS: { role: BuiltInAgentRole; title: string; description: string }[] = [
  { role: 'manager', title: 'Agent Manager', description: 'Routes requests and orchestrates the specialist agents.' },
  { role: 'clickhouse_query', title: 'Clickhouse SQL', description: 'Handles direct ClickHouse questions, schema lookups, previews, and charts.' },
  { role: 'data_analyst', title: 'Data Analyst', description: 'Runs deeper multi-step ClickHouse investigations.' },
  { role: 'auto_ml', title: 'Auto-ML', description: 'Benchmarks machine-learning models on a scoped ClickHouse dataset.' },
  { role: 'data_cleaner', title: 'Data Cleaner', description: 'Profiles data quality issues and proposes cleanup SQL.' },
  { role: 'anonymizer', title: 'Anonymizer', description: 'Scans ClickHouse tables for likely PII and masking strategies.' },
  { role: 'email_sender', title: 'Email Sender', description: 'Sends text and file attachments through the configured SMTP server.' },
  { role: 'file_management', title: 'File Management', description: 'Browses, edits, creates, and moves files through backend Python tools.' },
  { role: 'pdf_creator', title: 'PDF Creator', description: 'Turns analyses or text into polished PDF exports.' },
  { role: 'oracle_analyst', title: 'Oracle SQL', description: 'Handles natural-language analysis and SQL execution against Oracle.' },
];

interface SettingsModalProps {
  isOpen: boolean;
  onClose: () => void;
  config: AppConfig;
  onSave: (config: AppConfig) => void;
  onExportDb: () => Promise<void>;
  onImportDb: (snapshot: unknown) => Promise<void>;
  onSyncFromDb: () => Promise<void>;
  isDbBusy: boolean;
  lastSyncedAt: string | null;
  syncError: string | null;
}

function buildLocalConfig(config: AppConfig): AppConfig {
  return {
    provider: config.provider || 'ollama',
    baseUrl: config.baseUrl || (config as any).endpoint || 'http://localhost:11434',
    apiKey: config.apiKey || '',
    model: config.model || '',
    systemPrompt: config.systemPrompt || '',
    managerUseRagFunctionalContext: config.managerUseRagFunctionalContext ?? false,
    agentVisibility: {
      manager: config.agentVisibility?.manager ?? true,
      clickhouse_query: config.agentVisibility?.clickhouse_query ?? true,
      data_analyst: config.agentVisibility?.data_analyst ?? true,
      file_management: config.agentVisibility?.file_management ?? true,
      pdf_creator: config.agentVisibility?.pdf_creator ?? true,
      oracle_analyst: config.agentVisibility?.oracle_analyst ?? true,
      auto_ml: config.agentVisibility?.auto_ml ?? true,
      data_cleaner: config.agentVisibility?.data_cleaner ?? true,
      anonymizer: config.agentVisibility?.anonymizer ?? true,
      email_sender: config.agentVisibility?.email_sender ?? true,
    },
    disableSslVerification: config.disableSslVerification ?? false,
    elasticsearchUrl: config.elasticsearchUrl || 'http://localhost:9200',
    elasticsearchIndex: config.elasticsearchIndex || 'rag_documents',
    elasticsearchUsername: config.elasticsearchUsername || '',
    elasticsearchPassword: config.elasticsearchPassword || '',
    embeddingBaseUrl: config.embeddingBaseUrl || 'http://localhost:11434/v1',
    embeddingApiKey: config.embeddingApiKey || '',
    embeddingModel: config.embeddingModel || 'nomic-embed-text',
    embeddingVerifySsl: config.embeddingVerifySsl ?? true,
    chunkSize: config.chunkSize || 512,
    chunkOverlap: config.chunkOverlap || 50,
    knnNeighbors: config.knnNeighbors || 50,
    mcpTools: (config.mcpTools ?? []).map((tool) => ({
      id: tool.id || `mcp_${Date.now()}`,
      label: tool.label || 'New Tool',
      url: tool.url || '',
      description: tool.description || '',
      presetQuestions: Array.isArray(tool.presetQuestions)
        ? tool.presetQuestions.map((preset, presetIndex) => ({
            id: preset.id || `${tool.id || 'mcp'}_preset_${presetIndex + 1}`,
            label: preset.label || '',
            prompt: preset.prompt || '',
            preferredTool: preset.preferredTool || '',
          }))
        : [],
    })),
    documentationUrl: config.documentationUrl ?? '',
    agenticDataVizUrl: config.agenticDataVizUrl ?? '',
    portalApps: Array.isArray(config.portalApps)
      ? config.portalApps.map((app, index) => ({
          id: app.id || `portal_app_${index + 1}`,
          name: app.name || '',
          url: app.url || '',
          description: app.description || '',
        }))
      : [],
    customAgents: Array.isArray(config.customAgents)
      ? config.customAgents.map((agent, index) => ({
          id: agent.id || `custom_agent_${index + 1}`,
          title: agent.title || `Custom Agent ${index + 1}`,
          description: agent.description || '',
          pythonCode: agent.pythonCode || '',
          systemPrompt: agent.systemPrompt || '',
          managerRoutingHint: agent.managerRoutingHint || '',
          status: agent.status || 'draft',
          statusMessage: agent.statusMessage || '',
          enabled: agent.enabled ?? false,
          badgeColor: agent.badgeColor || 'zinc',
        }))
      : [],
    settingsAccessPassword: config.settingsAccessPassword || 'MM@2026',
    clickhouseHost: config.clickhouseHost || 'localhost',
    clickhousePort: config.clickhousePort || 8123,
    clickhouseDatabase: config.clickhouseDatabase || 'default',
    clickhouseUsername: config.clickhouseUsername || 'default',
    clickhousePassword: config.clickhousePassword || '',
    clickhouseSecure: config.clickhouseSecure ?? false,
    clickhouseVerifySsl: config.clickhouseVerifySsl ?? true,
    clickhouseHttpPath: config.clickhouseHttpPath ?? '',
    clickhouseQueryLimit: config.clickhouseQueryLimit || 200,
    oracleConnections: Array.isArray(config.oracleConnections)
      ? config.oracleConnections.map((connection, index) => ({
          id: connection.id || `oracle_${index + 1}`,
          label: connection.label || `Oracle ${index + 1}`,
          host: connection.host || 'localhost',
          port: Number(connection.port) || 1521,
          serviceName: connection.serviceName || '',
          sid: connection.sid || '',
          dsn: connection.dsn || '',
          username: connection.username || '',
          password: connection.password || '',
        }))
      : [
          {
            id: 'oracle_default',
            label: 'Default Oracle',
            host: 'localhost',
            port: 1521,
            serviceName: '',
            sid: '',
            dsn: '',
            username: '',
            password: '',
          },
        ],
    oracleAnalystConfig: {
      connectionId: config.oracleAnalystConfig?.connectionId || config.oracleConnections?.[0]?.id || 'oracle_default',
      rowLimit: Math.min(50000, Math.max(1, Number(config.oracleAnalystConfig?.rowLimit ?? 1000) || 1000)),
      maxRetries: Math.min(10, Math.max(1, Number(config.oracleAnalystConfig?.maxRetries ?? 3) || 3)),
      maxIterations: Math.min(20, Math.max(1, Number(config.oracleAnalystConfig?.maxIterations ?? 8) || 8)),
      toolkitId: config.oracleAnalystConfig?.toolkitId ?? '',
      systemPrompt:
        config.oracleAnalystConfig?.systemPrompt ??
        'You are the Oracle SQL agent. Reply in English. Use the Oracle tools before making assumptions, generate optimized Oracle SQL with explicit columns, and present final user-facing answers in polished Markdown with clear sections, concise bullets, and tasteful emphasis. Safe semantic HTML fragments are allowed when they improve readability.',
    },
    fileManagerConfig: {
      basePath: config.fileManagerConfig?.basePath ?? '',
      maxIterations: Math.min(15, Math.max(1, config.fileManagerConfig?.maxIterations ?? 10)),
      systemPrompt: config.fileManagerConfig?.systemPrompt ?? 'You are the File Management agent. Reply in English by default. Use filesystem tools instead of guessing, keep answers short and factual, ask for confirmation before destructive or overwrite actions, and present final user-facing answers in polished Markdown with concise structure and tasteful emphasis.',
    },
    emailSenderConfig: {
      host: config.emailSenderConfig?.host ?? '',
      port: Math.min(65535, Math.max(1, Number(config.emailSenderConfig?.port ?? 587) || 587)),
      secure: config.emailSenderConfig?.secure ?? false,
      startTls: config.emailSenderConfig?.startTls ?? true,
      username: config.emailSenderConfig?.username ?? '',
      password: config.emailSenderConfig?.password ?? '',
      fromEmail: config.emailSenderConfig?.fromEmail ?? '',
      fromName: config.emailSenderConfig?.fromName ?? 'RAGnarok',
      replyTo: config.emailSenderConfig?.replyTo ?? '',
      allowedRecipients: Array.isArray(config.emailSenderConfig?.allowedRecipients) ? config.emailSenderConfig!.allowedRecipients.filter(Boolean) : [],
      systemPrompt: config.emailSenderConfig?.systemPrompt ?? 'You are the Email Sender agent. Reply in English. Help the user prepare and send an email with text and optional file attachments. Ask only for the missing delivery details, never send to recipients outside the configured allowlist, and present final user-facing answers in polished Markdown with concise structure and tasteful emphasis.',
    },
  };
}

function inferUrlScheme(rawUrl: string): 'http' | 'https' {
  return String(rawUrl || '').trim().toLowerCase().startsWith('https://') ? 'https' : 'http';
}

function rewriteUrlScheme(rawUrl: string, scheme: 'http' | 'https'): string {
  const trimmed = String(rawUrl || '').trim();
  if (!trimmed) {
    return `${scheme}://`;
  }
  if (/^https?:\/\//i.test(trimmed)) {
    return trimmed.replace(/^https?:\/\//i, `${scheme}://`);
  }
  return `${scheme}://${trimmed.replace(/^\/+/, '')}`;
}

/**
 * SettingsModal Component
 * Provides a UI for configuring application settings, including LLM provider details,
 * RAG parameters (Elasticsearch, Embeddings), and system prompts.
 */
export function SettingsModal({
  isOpen,
  onClose,
  config,
  onSave,
  onExportDb,
  onImportDb,
  onSyncFromDb,
  isDbBusy,
  lastSyncedAt,
  syncError,
}: SettingsModalProps) {
  // Local state to hold configuration changes before saving
  const [localConfig, setLocalConfig] = useState<AppConfig>(() => buildLocalConfig(config));
  const wasOpenRef = useRef(false);
  const lastHydratedConfigRef = useRef(JSON.stringify(buildLocalConfig(config)));
  const currentDraftFingerprintRef = useRef(lastHydratedConfigRef.current);
  
  // State for available models fetched from the provider
  const [models, setModels] = useState<string[]>([]);
  const [isRefreshing, setIsRefreshing] = useState(false);

  // State for available embedding models
  const [embeddingModels, setEmbeddingModels] = useState<string[]>([]);
  const [isRefreshingEmbed, setIsRefreshingEmbed] = useState(false);
  const [embeddingModelsMessage, setEmbeddingModelsMessage] = useState('');
  
  // Connection test states for LLM
  const [testStatus, setTestStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
  const [testMessage, setTestMessage] = useState('');
  
  // Tab state (LLM, RAG, ClickHouse, MCP, or DB backup settings)
  const [activeTab, setActiveTab] = useState<'llm' | 'rag' | 'clickhouse' | 'oracle' | 'apps' | 'agents' | 'mcp' | 'storage'>('llm');

  // Connection test states for OpenSearch
  const [esTestStatus, setEsTestStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
  const [esTestMessage, setEsTestMessage] = useState('');
  const [clickhouseTestStatus, setClickhouseTestStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
  const [clickhouseTestMessage, setClickhouseTestMessage] = useState('');
  const [clickhouseTablesPreview, setClickhouseTablesPreview] = useState<string[]>([]);
  const [oracleTestStates, setOracleTestStates] = useState<Record<string, { status: 'idle' | 'testing' | 'success' | 'error'; message: string; tables: string[] }>>({});
  const [customAgentAnalysisState, setCustomAgentAnalysisState] = useState<Record<string, { status: 'idle' | 'running' | 'success' | 'error'; message: string }>>({});
  const [smtpTestStatus, setSmtpTestStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
  const [smtpTestMessage, setSmtpTestMessage] = useState('');

  // Setup index status
  const [setupStatus, setSetupStatus] = useState<'idle' | 'running' | 'success' | 'error'>('idle');
  const [setupMessage, setSetupMessage] = useState('');

  // Document ingest state
  const [ingestDocName, setIngestDocName] = useState('');
  const [ingestText, setIngestText] = useState('');
  const [ingestStatus, setIngestStatus] = useState<'idle' | 'indexing' | 'success' | 'error'>('idle');
  const [ingestMessage, setIngestMessage] = useState('');
  const fileInputRef = useRef<HTMLInputElement>(null);
  const dbImportInputRef = useRef<HTMLInputElement>(null);

  // Connection test states for Embedding model
  const [embedTestStatus, setEmbedTestStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
  const [embedTestMessage, setEmbedTestMessage] = useState('');

  // MCP test states: map of toolId → { status, tools }
  const [mcpTestStates, setMcpTestStates] = useState<Record<string, { status: 'idle' | 'testing' | 'success' | 'error'; message: string; tools: { name: string; description: string }[] }>>({});
  const [dbStatus, setDbStatus] = useState<'idle' | 'running' | 'success' | 'error'>('idle');
  const [dbMessage, setDbMessage] = useState('');
  const isDirectEmbeddingEndpoint = /(?:\/embeddings|:embeddings)$/i.test((localConfig.embeddingBaseUrl || '').trim());
  const embeddingUrlScheme = inferUrlScheme(localConfig.embeddingBaseUrl);

  useEffect(() => {
    currentDraftFingerprintRef.current = JSON.stringify(localConfig);
  }, [localConfig]);

  useEffect(() => {
    const nextLocalConfig = buildLocalConfig(config);
    const nextFingerprint = JSON.stringify(nextLocalConfig);
    const previousHydratedFingerprint = lastHydratedConfigRef.current;
    const currentDraftFingerprint = currentDraftFingerprintRef.current;
    const isOpening = isOpen && !wasOpenRef.current;
    const hasUnsavedChanges = currentDraftFingerprint !== previousHydratedFingerprint;
    const shouldAdoptExternalConfig =
      !isOpen ||
      isOpening ||
      (!hasUnsavedChanges && nextFingerprint !== previousHydratedFingerprint);

    if (shouldAdoptExternalConfig) {
      setLocalConfig(nextLocalConfig);
      lastHydratedConfigRef.current = nextFingerprint;
      currentDraftFingerprintRef.current = nextFingerprint;
    }

    wasOpenRef.current = isOpen;
  }, [config, isOpen]);

  if (!isOpen) return null;

  // Test MCP connection via backend
  const testMcpConnection = async (tool: McpTool) => {
    setMcpTestStates(prev => ({ ...prev, [tool.id]: { status: 'testing', message: '', tools: [] } }));
    try {
      const response = await fetch('/api/mcp/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          url: tool.url,
          disable_ssl_verification: localConfig.disableSslVerification ?? false,
        }),
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${response.status}`);
      }
      const data = await response.json();
      setMcpTestStates(prev => ({
        ...prev,
        [tool.id]: { status: 'success', message: `${data.tool_count} tool(s) available`, tools: data.tools },
      }));
    } catch (err) {
      setMcpTestStates(prev => ({
        ...prev,
        [tool.id]: { status: 'error', message: err instanceof Error ? err.message : 'Connection failed', tools: [] },
      }));
    }
  };

  // Test OpenSearch connection via backend
  const testElasticsearchConnection = async () => {
    setEsTestStatus('testing');
    setEsTestMessage('');
    try {
      const response = await fetch('/api/opensearch/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          url: localConfig.elasticsearchUrl,
          username: localConfig.elasticsearchUsername || undefined,
          password: localConfig.elasticsearchPassword || undefined,
        }),
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${response.status}`);
      }
      const data = await response.json();
      setEsTestStatus('success');
      setEsTestMessage(`Connected · cluster: ${data.cluster_name} · v${data.version}`);
    } catch (err) {
      setEsTestStatus('error');
      setEsTestMessage(err instanceof Error ? err.message : 'Failed to connect');
    }
  };

  const testClickHouseConnection = async () => {
    setClickhouseTestStatus('testing');
    setClickhouseTestMessage('');
    setClickhouseTablesPreview([]);
    try {
      const response = await fetch('/api/clickhouse/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          clickhouse: {
            host: localConfig.clickhouseHost,
            port: localConfig.clickhousePort,
            database: localConfig.clickhouseDatabase,
            username: localConfig.clickhouseUsername,
            password: localConfig.clickhousePassword,
            secure: localConfig.clickhouseSecure,
            verify_ssl: localConfig.disableSslVerification ? false : localConfig.clickhouseVerifySsl,
            http_path: localConfig.clickhouseHttpPath,
            query_limit: localConfig.clickhouseQueryLimit,
          },
        }),
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${response.status}`);
      }
      const data = await response.json();
      setClickhouseTestStatus('success');
      setClickhouseTestMessage(`Connected · db: ${data.database} · v${data.version} · ${data.table_count} table(s)`);
      setClickhouseTablesPreview(data.tables || []);
    } catch (err) {
      setClickhouseTestStatus('error');
      setClickhouseTestMessage(err instanceof Error ? err.message : 'Failed to connect');
    }
  };

  const testEmailSenderConnection = async () => {
    setSmtpTestStatus('testing');
    setSmtpTestMessage('');
    try {
      const response = await fetch('/api/email-sender/test-smtp', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          email_sender_config: {
            host: localConfig.emailSenderConfig.host,
            port: localConfig.emailSenderConfig.port,
            secure: localConfig.emailSenderConfig.secure,
            start_tls: localConfig.emailSenderConfig.startTls,
            username: localConfig.emailSenderConfig.username,
            password: localConfig.emailSenderConfig.password,
            from_email: localConfig.emailSenderConfig.fromEmail,
            from_name: localConfig.emailSenderConfig.fromName,
            reply_to: localConfig.emailSenderConfig.replyTo,
            allowed_recipients: localConfig.emailSenderConfig.allowedRecipients,
            system_prompt: localConfig.emailSenderConfig.systemPrompt,
          },
        }),
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${response.status}`);
      }
      const data = await response.json();
      setSmtpTestStatus('success');
      setSmtpTestMessage(`Connected to ${data.host}:${data.port} · ${data.allowedRecipients?.length ?? 0} allowed recipient(s)`);
    } catch (err) {
      setSmtpTestStatus('error');
      setSmtpTestMessage(err instanceof Error ? err.message : 'SMTP connection failed');
    }
  };

  const updateOracleConnection = (index: number, patch: Partial<AppConfig['oracleConnections'][number]>) => {
    setLocalConfig((prev) => {
      const currentConnections = [...(prev.oracleConnections ?? [])];
      if (!currentConnections[index]) return prev;
      const previousId = currentConnections[index].id;
      const nextId = patch.id !== undefined
        ? (String(patch.id).trim() || `oracle_${index + 1}`)
        : previousId;
      currentConnections[index] = {
        ...currentConnections[index],
        ...patch,
        id: nextId,
      };
      const nextSelectedConnectionId = prev.oracleAnalystConfig.connectionId === previousId
        ? nextId
        : prev.oracleAnalystConfig.connectionId;
      return {
        ...prev,
        oracleConnections: currentConnections,
        oracleAnalystConfig: {
          ...prev.oracleAnalystConfig,
          connectionId: currentConnections.some((connection) => connection.id === nextSelectedConnectionId)
            ? nextSelectedConnectionId
            : (currentConnections[0]?.id ?? prev.oracleAnalystConfig.connectionId),
        },
      };
    });
  };

  const addOracleConnection = () => {
    const nextId = `oracle_${Date.now()}`;
    setLocalConfig((prev) => ({
      ...prev,
      oracleConnections: [
        ...(prev.oracleConnections ?? []),
        {
          id: nextId,
          label: `Oracle ${(prev.oracleConnections?.length ?? 0) + 1}`,
          host: 'localhost',
          port: 1521,
          serviceName: '',
          sid: '',
          dsn: '',
          username: '',
          password: '',
        },
      ],
      oracleAnalystConfig: {
        ...prev.oracleAnalystConfig,
        connectionId: prev.oracleAnalystConfig.connectionId || nextId,
      },
    }));
  };

  const deleteOracleConnection = (index: number) => {
    setLocalConfig((prev) => {
      const currentConnections = [...(prev.oracleConnections ?? [])];
      if (currentConnections.length <= 1 || !currentConnections[index]) {
        return prev;
      }
      const removedId = currentConnections[index].id;
      const nextConnections = currentConnections.filter((_, connectionIndex) => connectionIndex !== index);
      return {
        ...prev,
        oracleConnections: nextConnections,
        oracleAnalystConfig: {
          ...prev.oracleAnalystConfig,
          connectionId: prev.oracleAnalystConfig.connectionId === removedId
            ? (nextConnections[0]?.id ?? prev.oracleAnalystConfig.connectionId)
            : prev.oracleAnalystConfig.connectionId,
        },
      };
    });
  };

  const testOracleConnection = async (connectionId: string) => {
    const connection = (localConfig.oracleConnections ?? []).find((item) => item.id === connectionId);
    if (!connection) return;
    setOracleTestStates((prev) => ({
      ...prev,
      [connectionId]: { status: 'testing', message: '', tables: [] },
    }));
    try {
      const response = await fetch('/api/oracle/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          connection: {
            id: connection.id,
            label: connection.label,
            host: connection.host,
            port: connection.port,
            service_name: connection.serviceName,
            sid: connection.sid,
            dsn: connection.dsn,
            username: connection.username,
            password: connection.password,
          },
        }),
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${response.status}`);
      }
      const data = await response.json();
      const summary = `Connected · schema: ${data.current_schema || 'unknown'} · user: ${data.session_user || 'unknown'} · ${data.table_count} table(s)`;
      setOracleTestStates((prev) => ({
        ...prev,
        [connectionId]: {
          status: 'success',
          message: summary,
          tables: Array.isArray(data.tables) ? data.tables : [],
        },
      }));
    } catch (err) {
      setOracleTestStates((prev) => ({
        ...prev,
        [connectionId]: {
          status: 'error',
          message: err instanceof Error ? err.message : 'Failed to connect',
          tables: [],
        },
      }));
    }
  };

  // Setup kNN index via backend
  const setupIndex = async () => {
    setSetupStatus('running');
    setSetupMessage('');
    try {
      const response = await fetch('/api/opensearch/setup-index', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          opensearch: {
            url: localConfig.elasticsearchUrl,
            index: localConfig.elasticsearchIndex,
            username: localConfig.elasticsearchUsername || undefined,
            password: localConfig.elasticsearchPassword || undefined,
          },
          embedding_dimension: 768,
        }),
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${response.status}`);
      }
      const data = await response.json();
      setSetupStatus('success');
      setSetupMessage(data.status === 'exists' ? `Index "${data.index}" already exists.` : `Index "${data.index}" created.`);
    } catch (err) {
      setSetupStatus('error');
      setSetupMessage(err instanceof Error ? err.message : 'Setup failed');
    }
  };

  // Load file content into the ingest textarea
  const handleFileLoad = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    if (!ingestDocName) setIngestDocName(file.name.replace(/\.[^.]+$/, ''));
    const reader = new FileReader();
    reader.onload = (ev) => setIngestText(ev.target?.result as string ?? '');
    reader.readAsText(file);
    e.target.value = '';
  };

  // Ingest document into OpenSearch via backend
  const ingestDocument = async () => {
    if (!ingestText.trim() || !ingestDocName.trim()) return;
    setIngestStatus('indexing');
    setIngestMessage('');
    try {
      const response = await fetch('/api/documents/ingest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          text: ingestText,
          doc_name: ingestDocName,
          opensearch: {
            url: localConfig.elasticsearchUrl,
            index: localConfig.elasticsearchIndex,
            username: localConfig.elasticsearchUsername || undefined,
            password: localConfig.elasticsearchPassword || undefined,
          },
          embedding_base_url: localConfig.embeddingBaseUrl,
          embedding_api_key: localConfig.embeddingApiKey || undefined,
          embedding_model: localConfig.embeddingModel,
          embedding_verify_ssl: localConfig.disableSslVerification ? false : localConfig.embeddingVerifySsl,
          chunk_size: localConfig.chunkSize,
          chunk_overlap: localConfig.chunkOverlap,
        }),
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${response.status}`);
      }
      const data = await response.json();
      setIngestStatus('success');
      setIngestMessage(`Indexed ${data.chunks_indexed} chunks (doc id: ${data.doc_id.slice(0, 8)}…)`);
      setIngestText('');
      setIngestDocName('');
    } catch (err) {
      setIngestStatus('error');
      setIngestMessage(err instanceof Error ? err.message : 'Ingest failed');
    }
  };

  // Test the embedding model by generating a real vector and checking OpenSearch index compatibility
  const testEmbeddingConnection = async () => {
    setEmbedTestStatus('testing');
    setEmbedTestMessage('');
    try {
      const response = await fetch('/api/embedding/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          embedding_base_url: localConfig.embeddingBaseUrl,
          embedding_model: localConfig.embeddingModel,
          embedding_api_key: localConfig.embeddingApiKey || undefined,
          embedding_verify_ssl: localConfig.disableSslVerification ? false : localConfig.embeddingVerifySsl,
          opensearch: localConfig.elasticsearchUrl ? {
            url: localConfig.elasticsearchUrl,
            index: localConfig.elasticsearchIndex,
            username: localConfig.elasticsearchUsername || undefined,
            password: localConfig.elasticsearchPassword || undefined,
          } : undefined,
        }),
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error((err as any).detail || `HTTP ${response.status}`);
      }
      const data = await response.json();
      let msg = `Model OK · dimension: ${data.dimension}`;
      if (data.opensearch) {
        if (data.opensearch.status === 'compatible') {
          msg += ` · compatible with the OpenSearch index ✓`;
        } else if (data.opensearch.status === 'incompatible') {
          msg += ` · ⚠ ${data.opensearch.message}`;
        } else if (data.opensearch.status === 'no_index') {
          msg += ` · index not found (use "Setup Index")`;
        } else if (data.opensearch.status === 'error') {
          msg += ` · OpenSearch: ${data.opensearch.message}`;
        }
      }
      setEmbedTestStatus('success');
      setEmbedTestMessage(msg);
    } catch (err) {
      setEmbedTestStatus('error');
      setEmbedTestMessage(err instanceof Error ? err.message : 'Failed to connect');
    }
  };

  // Fetch available models from the configured embedding provider
  const fetchEmbeddingModels = async () => {
    setIsRefreshingEmbed(true);
    setEmbeddingModelsMessage('');
    try {
      const response = await fetch('/api/embedding/models', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          base_url: localConfig.embeddingBaseUrl,
          api_key: localConfig.embeddingApiKey || undefined,
          disable_ssl_verification: (localConfig.disableSslVerification ?? false) || !localConfig.embeddingVerifySsl,
        }),
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error((err as any).detail || `HTTP Error: ${response.status}`);
      }
      const data = await response.json();
      const fetched: string[] = Array.isArray(data.models) ? data.models : [];
      setEmbeddingModels(fetched);
      setEmbeddingModelsMessage(typeof data.message === 'string' ? data.message : '');
      if (fetched.length > 0 && !fetched.includes(localConfig.embeddingModel)) {
        setLocalConfig(prev => ({ ...prev, embeddingModel: fetched[0] }));
      }
    } catch (err) {
      console.error("Error fetching embedding models:", err);
      setEmbeddingModels([]);
      setEmbeddingModelsMessage(err instanceof Error ? err.message : 'Model discovery failed');
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
      const response = await fetch('/api/llm/models', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          provider: localConfig.provider,
          base_url: localConfig.baseUrl,
          api_key: localConfig.apiKey || undefined,
          disable_ssl_verification: localConfig.disableSslVerification ?? false,
        }),
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error((err as any).detail || `HTTP Error: ${response.status}`);
      }

      const data = await response.json();
      const fetchedModels: string[] = Array.isArray(data.models) ? data.models : [];

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

  const runDbAction = async (successMessage: string, action: () => Promise<void>) => {
    setDbStatus('running');
    setDbMessage('');
    try {
      await action();
      setDbStatus('success');
      setDbMessage(successMessage);
    } catch (error) {
      setDbStatus('error');
      setDbMessage(error instanceof Error ? error.message : 'DB action failed');
    }
  };

  const handleExportDb = async () => {
    await runDbAction('DB backup exported successfully.', onExportDb);
  };

  const handleSyncDb = async () => {
    await runDbAction('The interface is now aligned with the latest DB version.', onSyncFromDb);
  };

  const handleImportDbFile = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    try {
      const text = await file.text();
      const snapshot = JSON.parse(text);
      await runDbAction(`Backup "${file.name}" imported successfully.`, () => onImportDb(snapshot));
    } catch (error) {
      setDbStatus('error');
      setDbMessage(error instanceof Error ? error.message : 'Import DB failed');
    } finally {
      e.target.value = '';
    }
  };

  const addPortalApp = () => {
    setLocalConfig((prev) => ({
      ...prev,
      portalApps: [
        ...(prev.portalApps ?? []),
        {
          id: `portal_app_${Date.now()}`,
          name: '',
          url: '',
          description: '',
        },
      ],
    }));
  };

  const updatePortalApp = (index: number, patch: Partial<AppConfig['portalApps'][number]>) => {
    setLocalConfig((prev) => {
      const nextApps = [...(prev.portalApps ?? [])];
      if (!nextApps[index]) return prev;
      nextApps[index] = {
        ...nextApps[index],
        ...patch,
      };
      return {
        ...prev,
        portalApps: nextApps,
      };
    });
  };

  const deletePortalApp = (index: number) => {
    setLocalConfig((prev) => ({
      ...prev,
      portalApps: (prev.portalApps ?? []).filter((_, appIndex) => appIndex !== index),
    }));
  };

  const addCustomAgent = () => {
    setLocalConfig((prev) => ({
      ...prev,
      customAgents: [
        ...(prev.customAgents ?? []),
        {
          id: `custom_agent_${Date.now()}`,
          title: `Custom Agent ${(prev.customAgents?.length ?? 0) + 1}`,
          description: '',
          pythonCode: '',
          systemPrompt: '',
          managerRoutingHint: '',
          status: 'draft',
          statusMessage: 'Paste Python code, then click Analyze & build.',
          enabled: false,
          badgeColor: 'zinc',
        },
      ],
    }));
  };

  const updateCustomAgent = (index: number, patch: Partial<AppConfig['customAgents'][number]>) => {
    setLocalConfig((prev) => {
      const nextAgents = [...(prev.customAgents ?? [])];
      if (!nextAgents[index]) return prev;
      nextAgents[index] = {
        ...nextAgents[index],
        ...patch,
      };
      return {
        ...prev,
        customAgents: nextAgents,
      };
    });
  };

  const deleteCustomAgent = (index: number) => {
    setLocalConfig((prev) => ({
      ...prev,
      customAgents: (prev.customAgents ?? []).filter((_, agentIndex) => agentIndex !== index),
    }));
  };

  const analyzeCustomAgent = async (index: number) => {
    const agent = (localConfig.customAgents ?? [])[index];
    if (!agent) return;
    const agentId = agent.id || `custom_agent_${index + 1}`;
    setCustomAgentAnalysisState((prev) => ({
      ...prev,
      [agentId]: { status: 'running', message: '' },
    }));
    try {
      const response = await fetch('/api/custom-agent/analyze', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          title: agent.title,
          description: agent.description,
          python_code: agent.pythonCode,
          llm_base_url: localConfig.baseUrl,
          llm_model: localConfig.model,
          llm_api_key: localConfig.apiKey || undefined,
          llm_provider: localConfig.provider,
          disable_ssl_verification: localConfig.disableSslVerification ?? false,
        }),
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${response.status}`);
      }
      const data = await response.json();
      updateCustomAgent(index, {
        title: data.profile?.title || agent.title,
        description: data.profile?.description || agent.description,
        systemPrompt: data.profile?.systemPrompt || '',
        managerRoutingHint: data.profile?.managerRoutingHint || '',
        badgeColor: data.profile?.badgeColor || 'zinc',
        status: 'ready',
        statusMessage: data.profile?.statusMessage || 'The custom agent profile is ready.',
      });
      setCustomAgentAnalysisState((prev) => ({
        ...prev,
        [agentId]: { status: 'success', message: data.profile?.statusMessage || 'The custom agent profile is ready.' },
      }));
    } catch (err) {
      updateCustomAgent(index, {
        status: 'error',
        statusMessage: err instanceof Error ? err.message : 'Custom agent analysis failed.',
      });
      setCustomAgentAnalysisState((prev) => ({
        ...prev,
        [agentId]: { status: 'error', message: err instanceof Error ? err.message : 'Custom agent analysis failed.' },
      }));
    }
  };

  const formattedLastSync = lastSyncedAt
    ? new Date(lastSyncedAt).toLocaleString()
    : 'No successful sync has been confirmed yet.';

  return (
    <>
      <div
        onClick={onClose}
        className="fixed inset-0 bg-black/20 backdrop-blur-sm z-40 animate-fade-in"
      />
      <div className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-full max-w-[55rem] z-50 p-6 animate-scale-in">
        <div className="glass-panel rounded-[2rem] p-8 shadow-2xl max-h-[90vh] overflow-y-auto">
          <div className="flex items-center justify-between mb-6">
            <h2 className="text-2xl font-semibold flex items-center gap-3 dark:text-white">
              <Settings className="w-6 h-6 text-blue-500" />
              Settings
            </h2>
            <button
              onClick={onClose}
              className="p-2 rounded-full hover:bg-black/5 dark:hover:bg-white/10 transition-colors"
            >
              <X className="w-5 h-5 text-gray-500 dark:text-gray-400" />
            </button>
          </div>

          <div className="flex gap-4 border-b border-gray-200 dark:border-gray-800 mb-6">
            <button
              onClick={() => setActiveTab('llm')}
              className={`pb-3 px-2 text-sm font-medium transition-colors border-b-2 ${activeTab === 'llm' ? 'border-blue-500 text-blue-600' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200'}`}
            >
              LLM Settings
            </button>
            <button
              onClick={() => setActiveTab('rag')}
              className={`pb-3 px-2 text-sm font-medium transition-colors border-b-2 ${activeTab === 'rag' ? 'border-blue-500 text-blue-600' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200'}`}
            >
              RAG & OpenSearch
            </button>
            <button
              onClick={() => setActiveTab('clickhouse')}
              className={`pb-3 px-2 text-sm font-medium transition-colors border-b-2 ${activeTab === 'clickhouse' ? 'border-cyan-500 text-cyan-600' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200'}`}
            >
              ClickHouse SQL
            </button>
            <button
              onClick={() => setActiveTab('mcp')}
              className={`pb-3 px-2 text-sm font-medium transition-colors border-b-2 ${activeTab === 'mcp' ? 'border-teal-500 text-teal-600' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
            >
              MCP Tools
            </button>
            <button
              onClick={() => setActiveTab('oracle')}
              className={`pb-3 px-2 text-sm font-medium transition-colors border-b-2 ${activeTab === 'oracle' ? 'border-orange-500 text-orange-600' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
            >
              Oracle SQL
            </button>
            <button
              onClick={() => setActiveTab('apps')}
              className={`pb-3 px-2 text-sm font-medium transition-colors border-b-2 ${activeTab === 'apps' ? 'border-sky-500 text-sky-600' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
            >
              App Portal
            </button>
            <button
              onClick={() => setActiveTab('agents')}
              className={`pb-3 px-2 text-sm font-medium transition-colors border-b-2 ${activeTab === 'agents' ? 'border-slate-500 text-slate-700 dark:text-slate-200' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
            >
              Custom Agents
            </button>
            <button
              onClick={() => setActiveTab('storage')}
              className={`pb-3 px-2 text-sm font-medium transition-colors border-b-2 ${activeTab === 'storage' ? 'border-amber-500 text-amber-600' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
            >
              DB Backup
            </button>
          </div>

          {activeTab === 'storage' ? (
            <div className="space-y-5">
              <div className="p-4 rounded-2xl bg-amber-50/80 dark:bg-amber-900/20 border border-amber-200/70 dark:border-amber-700/40">
                <div className="flex items-center gap-2 mb-2">
                  <Database className="w-4 h-4 text-amber-600 dark:text-amber-300" />
                  <h3 className="text-sm font-semibold text-amber-900 dark:text-amber-200">Source of truth: `DB.json`</h3>
                </div>
                <p className="text-xs text-amber-800/90 dark:text-amber-300/90 leading-relaxed">
                  Connection details, conversations, and durable preferences are saved on the backend. Use this section to export, import, and realign the interface with the latest database version.
                </p>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                <button
                  onClick={handleSyncDb}
                  disabled={isDbBusy}
                  className="px-4 py-3 rounded-2xl bg-white/80 dark:bg-gray-800/70 border border-gray-200 dark:border-gray-700 text-sm font-medium text-gray-800 dark:text-gray-100 hover:bg-white dark:hover:bg-gray-800 transition-colors flex items-center justify-center gap-2 disabled:opacity-60"
                >
                  {isDbBusy ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                  Resync now
                </button>
                <button
                  onClick={handleExportDb}
                  disabled={isDbBusy}
                  className="px-4 py-3 rounded-2xl bg-white/80 dark:bg-gray-800/70 border border-gray-200 dark:border-gray-700 text-sm font-medium text-gray-800 dark:text-gray-100 hover:bg-white dark:hover:bg-gray-800 transition-colors flex items-center justify-center gap-2 disabled:opacity-60"
                >
                  {isDbBusy ? <Loader2 className="w-4 h-4 animate-spin" /> : <Download className="w-4 h-4" />}
                  Export backup
                </button>
                <button
                  onClick={() => dbImportInputRef.current?.click()}
                  disabled={isDbBusy}
                  className="px-4 py-3 rounded-2xl bg-black text-white text-sm font-medium hover:bg-gray-800 transition-colors flex items-center justify-center gap-2 disabled:opacity-60"
                >
                  {isDbBusy ? <Loader2 className="w-4 h-4 animate-spin" /> : <UploadCloud className="w-4 h-4" />}
                  Import backup
                </button>
                <input
                  ref={dbImportInputRef}
                  type="file"
                  accept=".json,application/json"
                  className="hidden"
                  onChange={handleImportDbFile}
                />
              </div>

              <div className="p-4 rounded-2xl bg-white/70 dark:bg-gray-800/60 border border-gray-200 dark:border-gray-700 space-y-2">
                <div className="flex items-center justify-between gap-3">
                  <span className="text-sm font-medium text-gray-900 dark:text-gray-100">Latest DB sync</span>
                  <span className="text-xs text-gray-500 dark:text-gray-400 text-right">{formattedLastSync}</span>
                </div>
                {dbStatus === 'success' && (
                  <p className="text-emerald-600 text-xs flex items-center gap-1">
                    <CheckCircle2 className="w-3 h-3" /> {dbMessage}
                  </p>
                )}
                {dbStatus === 'error' && (
                  <p className="text-red-600 text-xs flex items-center gap-1">
                    <XCircle className="w-3 h-3" /> {dbMessage}
                  </p>
                )}
                {syncError && (
                  <p className="text-red-600 text-xs flex items-center gap-1">
                    <XCircle className="w-3 h-3" /> Sync backend: {syncError}
                  </p>
                )}
              </div>

              <div className="p-4 rounded-2xl bg-white/70 dark:bg-gray-800/60 border border-gray-200 dark:border-gray-700 space-y-3">
                <div className="flex items-center gap-2">
                  <Key className="w-4 h-4 text-amber-600 dark:text-amber-300" />
                  <h3 className="text-sm font-semibold text-gray-900 dark:text-gray-100">Settings access protection</h3>
                </div>
                <p className="text-xs text-gray-500 dark:text-gray-400 leading-relaxed">
                  The discreet button on the landing page asks for this password before opening Settings. Default value: <span className="font-mono">MM@2026</span>.
                </p>
                <div>
                  <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Access password</label>
                  <input
                    type="password"
                    value={localConfig.settingsAccessPassword}
                    onChange={(e) => setLocalConfig(prev => ({ ...prev, settingsAccessPassword: e.target.value }))}
                    className="w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl px-3 py-2.5 text-sm text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-amber-500/40 transition-all"
                    placeholder="MM@2026"
                  />
                </div>
              </div>
            </div>
          ) : activeTab === 'oracle' ? (
            <div className="space-y-6">
              <div className="p-4 rounded-2xl bg-orange-50/80 dark:bg-orange-900/20 border border-orange-200/70 dark:border-orange-700/40">
                <div className="flex items-center gap-2 mb-2">
                  <Database className="w-4 h-4 text-orange-600 dark:text-orange-300" />
                  <h3 className="text-sm font-semibold text-orange-900 dark:text-orange-200">Oracle SQL</h3>
                </div>
                <p className="text-xs text-orange-800/90 dark:text-orange-300/90 leading-relaxed">
                  Configure one or more Oracle connections, then choose which connection the Oracle SQL agent should use by default. The backend stays Python-only and uses the local LLM to inspect schema, validate SQL, execute safe read-only queries, and summarize the result in English.
                </p>
              </div>

              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <h3 className="text-sm font-semibold text-gray-900 dark:text-gray-100 flex items-center gap-2">
                    <Database className="w-4 h-4 text-orange-500" /> Oracle Connections
                  </h3>
                  <button
                    onClick={addOracleConnection}
                    className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium bg-orange-50 text-orange-700 border border-orange-200 rounded-xl hover:bg-orange-100 transition-colors"
                  >
                    <Plus className="w-3.5 h-3.5" /> Add connection
                  </button>
                </div>

                {(localConfig.oracleConnections ?? []).map((connection, index) => {
                  const testState = oracleTestStates[connection.id];
                  const isDefaultConnection = localConfig.oracleAnalystConfig.connectionId === connection.id;
                  return (
                    <div key={connection.id || `oracle-${index}`} className="p-4 bg-white/60 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-2xl space-y-4">
                      <div className="flex items-center justify-between gap-3">
                        <div>
                          <h4 className="text-sm font-semibold text-gray-900 dark:text-gray-100">{connection.label || `Oracle ${index + 1}`}</h4>
                          <p className="text-[11px] text-gray-500 dark:text-gray-400">Use either a full DSN or the host + port + service name / SID combination.</p>
                        </div>
                        {isDefaultConnection && (
                          <span className="px-2 py-1 rounded-full bg-orange-100 text-orange-700 text-[10px] font-semibold border border-orange-200">
                            Default connection
                          </span>
                        )}
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div>
                          <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Connection ID</label>
                          <input
                            type="text"
                            value={connection.id}
                            onChange={(e) => updateOracleConnection(index, { id: e.target.value })}
                            className="w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl px-3 py-2.5 text-sm text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-orange-500/40 transition-all"
                            placeholder={`oracle_${index + 1}`}
                          />
                        </div>
                        <div>
                          <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Label</label>
                          <input
                            type="text"
                            value={connection.label}
                            onChange={(e) => updateOracleConnection(index, { label: e.target.value })}
                            className="w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl px-3 py-2.5 text-sm text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-orange-500/40 transition-all"
                            placeholder="Finance Oracle"
                          />
                        </div>
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div>
                          <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Host</label>
                          <input
                            type="text"
                            value={connection.host}
                            onChange={(e) => updateOracleConnection(index, { host: e.target.value })}
                            className="w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl px-3 py-2.5 text-sm text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-orange-500/40 transition-all"
                            placeholder="localhost"
                          />
                        </div>
                        <div>
                          <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Port</label>
                          <input
                            type="number"
                            value={connection.port}
                            onChange={(e) => updateOracleConnection(index, { port: parseInt(e.target.value, 10) || 1521 })}
                            className="w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl px-3 py-2.5 text-sm text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-orange-500/40 transition-all"
                            placeholder="1521"
                          />
                        </div>
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div>
                          <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Service name</label>
                          <input
                            type="text"
                            value={connection.serviceName}
                            onChange={(e) => updateOracleConnection(index, { serviceName: e.target.value })}
                            className="w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl px-3 py-2.5 text-sm text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-orange-500/40 transition-all"
                            placeholder="ORCLPDB1"
                          />
                        </div>
                        <div>
                          <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">SID</label>
                          <input
                            type="text"
                            value={connection.sid}
                            onChange={(e) => updateOracleConnection(index, { sid: e.target.value })}
                            className="w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl px-3 py-2.5 text-sm text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-orange-500/40 transition-all"
                            placeholder="Leave empty if you use service name"
                          />
                        </div>
                      </div>

                      <div>
                        <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Full DSN (optional)</label>
                        <input
                          type="text"
                          value={connection.dsn}
                          onChange={(e) => updateOracleConnection(index, { dsn: e.target.value })}
                          className="w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl px-3 py-2.5 text-sm text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-orange-500/40 transition-all"
                          placeholder="DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)..."
                        />
                        <p className="text-[11px] text-gray-500 dark:text-gray-400 mt-1">If provided, the DSN overrides the host + port + service name / SID combination.</p>
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div>
                          <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Username</label>
                          <input
                            type="text"
                            value={connection.username}
                            onChange={(e) => updateOracleConnection(index, { username: e.target.value })}
                            className="w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl px-3 py-2.5 text-sm text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-orange-500/40 transition-all"
                            placeholder="system"
                            autoComplete="username"
                          />
                        </div>
                        <div>
                          <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Password</label>
                          <input
                            type="password"
                            value={connection.password}
                            onChange={(e) => updateOracleConnection(index, { password: e.target.value })}
                            className="w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl px-3 py-2.5 text-sm text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-orange-500/40 transition-all"
                            placeholder="••••••••"
                            autoComplete="current-password"
                          />
                        </div>
                      </div>

                      <div className="flex flex-wrap items-center gap-2">
                        <button
                          onClick={() => testOracleConnection(connection.id)}
                          disabled={testState?.status === 'testing'}
                          className="px-4 py-2 bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-200 rounded-xl font-medium transition-colors flex items-center gap-2 whitespace-nowrap text-sm disabled:opacity-60"
                        >
                          {testState?.status === 'testing' ? <Loader2 className="w-4 h-4 animate-spin" /> : 'Test Oracle'}
                        </button>
                        <button
                          onClick={() => setLocalConfig((prev) => ({
                            ...prev,
                            oracleAnalystConfig: {
                              ...prev.oracleAnalystConfig,
                              connectionId: connection.id,
                            },
                          }))}
                          className="px-4 py-2 bg-orange-50 hover:bg-orange-100 text-orange-700 border border-orange-200 rounded-xl font-medium transition-colors text-sm"
                        >
                          Use as default
                        </button>
                        <button
                          onClick={() => deleteOracleConnection(index)}
                          disabled={(localConfig.oracleConnections ?? []).length <= 1}
                          className="px-4 py-2 bg-red-50 hover:bg-red-100 text-red-700 border border-red-200 rounded-xl font-medium transition-colors text-sm disabled:opacity-40"
                        >
                          Delete
                        </button>
                      </div>

                      {testState?.status === 'success' && (
                        <div className="space-y-2">
                          <p className="text-emerald-600 text-xs flex items-center gap-1"><CheckCircle2 className="w-3 h-3" /> {testState.message}</p>
                          {testState.tables.length > 0 && (
                            <div className="space-y-1">
                              <p className="text-[11px] font-medium text-gray-600 dark:text-gray-300">Accessible tables preview</p>
                              <div className="flex flex-wrap gap-1">
                                {testState.tables.map((table) => (
                                  <span
                                    key={`${connection.id}-${table}`}
                                    className="px-2 py-0.5 bg-orange-50 dark:bg-orange-900/30 text-orange-700 dark:text-orange-300 border border-orange-200 dark:border-orange-700 rounded-md text-[10px] font-medium"
                                  >
                                    {table}
                                  </span>
                                ))}
                              </div>
                            </div>
                          )}
                        </div>
                      )}
                      {testState?.status === 'error' && (
                        <p className="text-red-600 text-xs flex items-center gap-1"><XCircle className="w-3 h-3" /> {testState.message}</p>
                      )}
                    </div>
                  );
                })}
              </div>

              <div className="pt-2 border-t border-gray-100 dark:border-gray-800 space-y-4">
                <h3 className="text-sm font-semibold text-gray-900 dark:text-gray-100 flex items-center gap-2">
                  <Bot className="w-4 h-4 text-orange-500" /> Oracle SQL Defaults
                </h3>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Default connection</label>
                    <select
                      value={localConfig.oracleAnalystConfig.connectionId}
                      onChange={(e) => setLocalConfig((prev) => ({
                        ...prev,
                        oracleAnalystConfig: {
                          ...prev.oracleAnalystConfig,
                          connectionId: e.target.value,
                        },
                      }))}
                      className="w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl px-3 py-2.5 text-sm text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-orange-500/40 transition-all"
                    >
                      {(localConfig.oracleConnections ?? []).map((connection) => (
                        <option key={connection.id} value={connection.id}>
                          {connection.label || connection.id}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Toolkit ID (optional)</label>
                    <input
                      type="text"
                      value={localConfig.oracleAnalystConfig.toolkitId}
                      onChange={(e) => setLocalConfig((prev) => ({
                        ...prev,
                        oracleAnalystConfig: {
                          ...prev.oracleAnalystConfig,
                          toolkitId: e.target.value,
                        },
                      }))}
                      className="w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl px-3 py-2.5 text-sm text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-orange-500/40 transition-all"
                      placeholder="default"
                    />
                  </div>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <div>
                    <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Row limit</label>
                    <input
                      type="number"
                      min={1}
                      max={50000}
                      value={localConfig.oracleAnalystConfig.rowLimit}
                      onChange={(e) => setLocalConfig((prev) => ({
                        ...prev,
                        oracleAnalystConfig: {
                          ...prev.oracleAnalystConfig,
                          rowLimit: Math.min(50000, Math.max(1, parseInt(e.target.value, 10) || 1000)),
                        },
                      }))}
                      className="w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl px-3 py-2.5 text-sm text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-orange-500/40 transition-all"
                    />
                  </div>
                  <div>
                    <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Max retries</label>
                    <input
                      type="number"
                      min={1}
                      max={10}
                      value={localConfig.oracleAnalystConfig.maxRetries}
                      onChange={(e) => setLocalConfig((prev) => ({
                        ...prev,
                        oracleAnalystConfig: {
                          ...prev.oracleAnalystConfig,
                          maxRetries: Math.min(10, Math.max(1, parseInt(e.target.value, 10) || 3)),
                        },
                      }))}
                      className="w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl px-3 py-2.5 text-sm text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-orange-500/40 transition-all"
                    />
                  </div>
                  <div>
                    <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Max iterations</label>
                    <input
                      type="number"
                      min={1}
                      max={20}
                      value={localConfig.oracleAnalystConfig.maxIterations}
                      onChange={(e) => setLocalConfig((prev) => ({
                        ...prev,
                        oracleAnalystConfig: {
                          ...prev.oracleAnalystConfig,
                          maxIterations: Math.min(20, Math.max(1, parseInt(e.target.value, 10) || 8)),
                        },
                      }))}
                      className="w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl px-3 py-2.5 text-sm text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-orange-500/40 transition-all"
                    />
                  </div>
                </div>

                <p className="text-[11px] text-gray-500 dark:text-gray-400 -mt-2">
                  The ReAct runtime is capped at 8 iterations for safety. The compatibility setting above is kept for configuration consistency.
                </p>

                <div>
                  <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Custom system prompt</label>
                  <textarea
                    value={localConfig.oracleAnalystConfig.systemPrompt}
                    onChange={(e) => setLocalConfig((prev) => ({
                      ...prev,
                      oracleAnalystConfig: {
                        ...prev.oracleAnalystConfig,
                        systemPrompt: e.target.value,
                      },
                    }))}
                    className="w-full min-h-[140px] bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl px-3 py-3 text-sm text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-orange-500/40 transition-all resize-none"
                    placeholder="You are the Oracle SQL agent..."
                  />
                </div>
              </div>
            </div>
          ) : activeTab === 'apps' ? (
            <div className="space-y-6">
              <div className="p-4 rounded-[1.75rem] bg-[linear-gradient(135deg,rgba(240,249,255,0.95),rgba(224,242,254,0.88),rgba(239,246,255,0.92))] border border-sky-200/80 shadow-[0_18px_40px_rgba(14,165,233,0.10)]">
                <div className="flex items-center justify-between gap-4">
                  <div>
                    <div className="flex items-center gap-2 mb-1.5">
                      <Layers className="w-4 h-4 text-sky-500" />
                      <h3 className="text-sm font-semibold text-sky-950">Agents & Tools app portal</h3>
                    </div>
                    <p className="text-xs text-sky-900/80 leading-relaxed max-w-2xl">
                      Configure the external apps you want to expose on the landing page through <strong>Agents & Tools</strong>. Each configured app becomes a modern glass tile on the portal page, opens in a new browser tab, and reveals its description only on hover.
                    </p>
                  </div>
                  <button
                    onClick={addPortalApp}
                    className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium bg-white/80 text-sky-700 border border-sky-200 rounded-xl hover:bg-white transition-colors shadow-sm"
                  >
                    <Plus className="w-3.5 h-3.5" /> Add app
                  </button>
                </div>
              </div>

              <div className="rounded-[1.75rem] border border-emerald-200/80 bg-[linear-gradient(135deg,rgba(236,253,245,0.96),rgba(220,252,231,0.88),rgba(240,253,250,0.92))] p-5 shadow-[0_18px_40px_rgba(16,185,129,0.10)]">
                <div className="flex items-center gap-2 mb-1.5">
                  <Server className="w-4 h-4 text-emerald-500" />
                  <h3 className="text-sm font-semibold text-emerald-950">Agentic Data Viz redirect</h3>
                </div>
                <p className="text-xs text-emerald-900/80 leading-relaxed mb-3">
                  If you set a URL here, the <strong>Agentic Data Viz</strong> card on the home page will open that application in a new browser tab. Leave it empty to keep the current in-app page.
                </p>
                <input
                  type="url"
                  value={localConfig.agenticDataVizUrl}
                  onChange={(e) => setLocalConfig((prev) => ({ ...prev, agenticDataVizUrl: e.target.value }))}
                  className="w-full bg-white/85 border border-white/80 rounded-2xl px-3 py-2.5 text-sm text-gray-900 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-emerald-500/40 transition-all"
                  placeholder="https://dataviz.example.com"
                />
                <p className="text-xs text-emerald-900/70 mt-2">
                  This redirect is considered active as soon as the URL is filled in.
                </p>
              </div>

              {(localConfig.portalApps ?? []).length === 0 ? (
                <div className="rounded-[1.75rem] border border-dashed border-sky-200 bg-white/70 px-6 py-10 text-center shadow-[0_10px_24px_rgba(148,163,184,0.08)]">
                  <p className="text-sm font-medium text-gray-700">No app tile configured yet.</p>
                  <p className="text-xs text-gray-500 mt-2">Create your first app tile to populate the Agents & Tools page.</p>
                </div>
              ) : (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {(localConfig.portalApps ?? []).map((app, index) => (
                    <div
                      key={app.id || `portal-app-${index}`}
                      className="rounded-[1.9rem] border border-white/70 bg-[linear-gradient(140deg,rgba(255,255,255,0.88),rgba(248,250,252,0.74),rgba(240,249,255,0.70))] backdrop-blur-xl p-5 shadow-[0_20px_40px_rgba(15,23,42,0.08)]"
                    >
                      <div className="flex items-start justify-between gap-3 mb-4">
                        <div>
                          <p className="text-[11px] uppercase tracking-[0.2em] text-sky-500 font-semibold">App tile {index + 1}</p>
                          <h4 className="text-base font-semibold text-gray-900 mt-1">{app.name || 'Untitled app'}</h4>
                        </div>
                        <button
                          onClick={() => deletePortalApp(index)}
                          className="w-9 h-9 rounded-full border border-red-100 bg-white/80 text-red-400 hover:text-red-600 hover:border-red-200 transition-colors flex items-center justify-center"
                          title="Delete app tile"
                        >
                          <Trash2 className="w-4 h-4" />
                        </button>
                      </div>

                      <div className="space-y-3">
                        <div>
                          <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Application name</label>
                          <input
                            type="text"
                            value={app.name}
                            onChange={(e) => updatePortalApp(index, { name: e.target.value })}
                            className="w-full bg-white/85 border border-white/80 rounded-2xl px-3 py-2.5 text-sm text-gray-900 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-sky-500/40 transition-all"
                            placeholder="Sales cockpit"
                          />
                        </div>
                        <div>
                          <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">URL</label>
                          <input
                            type="url"
                            value={app.url}
                            onChange={(e) => updatePortalApp(index, { url: e.target.value })}
                            className="w-full bg-white/85 border border-white/80 rounded-2xl px-3 py-2.5 text-sm text-gray-900 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-sky-500/40 transition-all"
                            placeholder="https://app.example.com"
                          />
                        </div>
                        <div>
                          <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Hover description</label>
                          <textarea
                            value={app.description}
                            onChange={(e) => updatePortalApp(index, { description: e.target.value })}
                            className="w-full min-h-[110px] bg-white/85 border border-white/80 rounded-[1.4rem] px-3 py-3 text-sm text-gray-900 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-sky-500/40 transition-all resize-none"
                            placeholder="Short explanation shown only when the user hovers the tile."
                          />
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          ) : activeTab === 'agents' ? (
            <div className="space-y-6">
              <div className="rounded-[1.75rem] border border-slate-200/80 bg-[linear-gradient(135deg,rgba(255,255,255,0.95),rgba(248,250,252,0.92),rgba(241,245,249,0.88))] p-5 shadow-[0_18px_40px_rgba(15,23,42,0.08)]">
                <div className="flex items-center gap-2 mb-1.5">
                  <Bot className="w-4 h-4 text-slate-700" />
                  <h3 className="text-sm font-semibold text-slate-950">Built-in agent visibility</h3>
                </div>
                <p className="text-xs text-slate-700/80 leading-relaxed max-w-3xl">
                  Disable an agent here to remove it from the Tools island in chat. The setting is shared for every user because it lives in the global configuration.
                </p>
                <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                  {BUILT_IN_AGENT_OPTIONS.map((agent) => {
                    const enabled = localConfig.agentVisibility?.[agent.role] !== false;
                    return (
                      <label
                        key={agent.role}
                        className="flex items-start justify-between gap-3 rounded-[1.35rem] border border-white/80 bg-white/85 px-4 py-3 shadow-[0_10px_24px_rgba(148,163,184,0.08)]"
                      >
                        <div className="min-w-0">
                          <p className="text-sm font-semibold text-slate-900">{agent.title}</p>
                          <p className="mt-1 text-[11px] leading-relaxed text-slate-600">{agent.description}</p>
                        </div>
                        <span className="inline-flex flex-col items-end gap-1 text-[11px] font-medium text-slate-500">
                          <input
                            type="checkbox"
                            checked={enabled}
                            onChange={(e) => setLocalConfig((prev) => ({
                              ...prev,
                              agentVisibility: {
                                ...prev.agentVisibility,
                                [agent.role]: e.target.checked,
                              },
                            }))}
                            className="h-4 w-4 rounded text-slate-700 focus:ring-slate-400"
                          />
                          {enabled ? 'Visible' : 'Hidden'}
                        </span>
                      </label>
                    );
                  })}
                </div>
              </div>

              <div className="rounded-[1.75rem] border border-slate-200/80 bg-[linear-gradient(135deg,rgba(255,255,255,0.95),rgba(248,250,252,0.92),rgba(241,245,249,0.88))] p-5 shadow-[0_18px_40px_rgba(15,23,42,0.08)]">
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <div className="flex items-center gap-2 mb-1.5">
                      <Send className="w-4 h-4 text-slate-700" />
                      <h3 className="text-sm font-semibold text-slate-950">Email Sender</h3>
                    </div>
                    <p className="text-xs text-slate-700/80 leading-relaxed max-w-3xl">
                      Configure the SMTP delivery used by the Email Sender agent. Allowed recipients are enforced globally and also reused by the Agent Manager and the MCP Orchestrator when they delegate email delivery.
                    </p>
                  </div>
                  <button
                    type="button"
                    onClick={testEmailSenderConnection}
                    className="inline-flex items-center gap-1.5 rounded-xl border border-slate-200 bg-white/90 px-3 py-1.5 text-xs font-medium text-slate-700 shadow-sm hover:bg-white transition-colors"
                  >
                    {smtpTestStatus === 'testing' ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <RefreshCw className="w-3.5 h-3.5" />}
                    Test SMTP
                  </button>
                </div>

                <div className="mt-4 grid gap-4 md:grid-cols-2">
                  <div>
                    <label className="text-xs font-medium text-slate-700">SMTP host</label>
                    <input
                      type="text"
                      value={localConfig.emailSenderConfig.host}
                      onChange={(e) => setLocalConfig(prev => ({ ...prev, emailSenderConfig: { ...prev.emailSenderConfig, host: e.target.value } }))}
                      className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm"
                      placeholder="smtp.example.com"
                    />
                  </div>
                  <div>
                    <label className="text-xs font-medium text-slate-700">Port</label>
                    <input
                      type="number"
                      value={localConfig.emailSenderConfig.port}
                      onChange={(e) => setLocalConfig(prev => ({ ...prev, emailSenderConfig: { ...prev.emailSenderConfig, port: Number(e.target.value) || 587 } }))}
                      className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm"
                    />
                  </div>
                  <div>
                    <label className="text-xs font-medium text-slate-700">Username</label>
                    <input
                      type="text"
                      value={localConfig.emailSenderConfig.username}
                      onChange={(e) => setLocalConfig(prev => ({ ...prev, emailSenderConfig: { ...prev.emailSenderConfig, username: e.target.value } }))}
                      className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm"
                      placeholder="smtp-user"
                    />
                  </div>
                  <div>
                    <label className="text-xs font-medium text-slate-700">Password</label>
                    <input
                      type="password"
                      value={localConfig.emailSenderConfig.password}
                      onChange={(e) => setLocalConfig(prev => ({ ...prev, emailSenderConfig: { ...prev.emailSenderConfig, password: e.target.value } }))}
                      className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm"
                      placeholder="SMTP password"
                    />
                  </div>
                  <div>
                    <label className="text-xs font-medium text-slate-700">From email</label>
                    <input
                      type="email"
                      value={localConfig.emailSenderConfig.fromEmail}
                      onChange={(e) => setLocalConfig(prev => ({ ...prev, emailSenderConfig: { ...prev.emailSenderConfig, fromEmail: e.target.value } }))}
                      className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm"
                      placeholder="no-reply@example.com"
                    />
                  </div>
                  <div>
                    <label className="text-xs font-medium text-slate-700">From name</label>
                    <input
                      type="text"
                      value={localConfig.emailSenderConfig.fromName}
                      onChange={(e) => setLocalConfig(prev => ({ ...prev, emailSenderConfig: { ...prev.emailSenderConfig, fromName: e.target.value } }))}
                      className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm"
                      placeholder="RAGnarok"
                    />
                  </div>
                  <div>
                    <label className="text-xs font-medium text-slate-700">Reply-To</label>
                    <input
                      type="email"
                      value={localConfig.emailSenderConfig.replyTo}
                      onChange={(e) => setLocalConfig(prev => ({ ...prev, emailSenderConfig: { ...prev.emailSenderConfig, replyTo: e.target.value } }))}
                      className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm"
                      placeholder="support@example.com"
                    />
                  </div>
                  <div className="flex items-center gap-6 pt-6">
                    <label className="inline-flex items-center gap-2 text-xs font-medium text-slate-700">
                      <input
                        type="checkbox"
                        checked={localConfig.emailSenderConfig.secure}
                        onChange={(e) => setLocalConfig(prev => ({ ...prev, emailSenderConfig: { ...prev.emailSenderConfig, secure: e.target.checked } }))}
                        className="h-4 w-4 rounded text-slate-700 focus:ring-slate-400"
                      />
                      SMTPS / SSL
                    </label>
                    <label className="inline-flex items-center gap-2 text-xs font-medium text-slate-700">
                      <input
                        type="checkbox"
                        checked={localConfig.emailSenderConfig.startTls}
                        onChange={(e) => setLocalConfig(prev => ({ ...prev, emailSenderConfig: { ...prev.emailSenderConfig, startTls: e.target.checked } }))}
                        className="h-4 w-4 rounded text-slate-700 focus:ring-slate-400"
                      />
                      STARTTLS
                    </label>
                  </div>
                </div>

                <div className="mt-4 grid gap-4 md:grid-cols-2">
                  <div>
                    <label className="text-xs font-medium text-slate-700">Allowed recipients</label>
                    <textarea
                      value={(localConfig.emailSenderConfig.allowedRecipients ?? []).join('\n')}
                      onChange={(e) => setLocalConfig(prev => ({
                        ...prev,
                        emailSenderConfig: {
                          ...prev.emailSenderConfig,
                          allowedRecipients: e.target.value.split(/\n|,|;/).map((item) => item.trim()).filter(Boolean),
                        },
                      }))}
                      className="mt-1 min-h-[140px] w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm"
                      placeholder={"user1@example.com\nuser2@example.com"}
                    />
                    <p className="mt-1 text-[11px] text-slate-500">Only these recipients can receive email from the app.</p>
                  </div>
                  <div>
                    <label className="text-xs font-medium text-slate-700">Email Sender system prompt</label>
                    <textarea
                      value={localConfig.emailSenderConfig.systemPrompt}
                      onChange={(e) => setLocalConfig(prev => ({ ...prev, emailSenderConfig: { ...prev.emailSenderConfig, systemPrompt: e.target.value } }))}
                      className="mt-1 min-h-[140px] w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm"
                    />
                    <div className="mt-2 rounded-xl border border-slate-200 bg-white/80 px-3 py-2 text-xs text-slate-600">
                      {smtpTestStatus === 'success' && <span className="text-emerald-600">{smtpTestMessage}</span>}
                      {smtpTestStatus === 'error' && <span className="text-rose-600">{smtpTestMessage}</span>}
                      {smtpTestStatus === 'idle' && 'Run "Test SMTP" to verify the connection before using the agent.'}
                      {smtpTestStatus === 'testing' && <span className="text-slate-500">Testing SMTP connection…</span>}
                    </div>
                  </div>
                </div>
              </div>

              <div className="rounded-[1.75rem] border border-slate-200/80 bg-[linear-gradient(135deg,rgba(255,255,255,0.95),rgba(248,250,252,0.92),rgba(241,245,249,0.88))] p-5 shadow-[0_18px_40px_rgba(15,23,42,0.08)]">
                <div className="flex items-center justify-between gap-4">
                  <div>
                    <div className="flex items-center gap-2 mb-1.5">
                      <Bot className="w-4 h-4 text-slate-700" />
                      <h3 className="text-sm font-semibold text-slate-950">Custom Python agents</h3>
                    </div>
                    <p className="text-xs text-slate-700/80 leading-relaxed max-w-2xl">
                      Paste Python code for a new agent. The local LLM analyzes the implementation draft, proposes a title, a user-facing description, a dedicated system prompt, and a routing hint for the Agent Manager. You can then enable or disable the agent to control whether it appears in the sub-agent menu.
                    </p>
                  </div>
                  <button
                    onClick={addCustomAgent}
                    className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium bg-white/90 text-slate-700 border border-slate-200 rounded-xl hover:bg-white transition-colors shadow-sm"
                  >
                    <Plus className="w-3.5 h-3.5" /> Add custom agent
                  </button>
                </div>
              </div>

              {(localConfig.customAgents ?? []).length === 0 ? (
                <div className="rounded-[1.75rem] border border-dashed border-slate-200 bg-white/70 px-6 py-10 text-center shadow-[0_10px_24px_rgba(148,163,184,0.08)]">
                  <p className="text-sm font-medium text-gray-700">No custom agent configured yet.</p>
                  <p className="text-xs text-gray-500 mt-2">Add one, paste Python code, then analyze it with the local LLM.</p>
                </div>
              ) : (
                <div className="space-y-4">
                  {(localConfig.customAgents ?? []).map((agent, index) => {
                    const analysisState = customAgentAnalysisState[agent.id];
                    return (
                      <div
                        key={agent.id || `custom-agent-${index}`}
                        className="rounded-[1.9rem] border border-white/70 bg-[linear-gradient(140deg,rgba(255,255,255,0.88),rgba(248,250,252,0.74),rgba(241,245,249,0.70))] backdrop-blur-xl p-5 shadow-[0_20px_40px_rgba(15,23,42,0.08)]"
                      >
                        <div className="flex items-start justify-between gap-3 mb-4">
                          <div>
                            <p className="text-[11px] uppercase tracking-[0.2em] text-slate-500 font-semibold">Custom agent {index + 1}</p>
                            <h4 className="text-base font-semibold text-gray-900 mt-1">{agent.title || 'Untitled custom agent'}</h4>
                            <p className="mt-1 text-xs text-gray-500">{agent.statusMessage || 'Paste Python code, then analyze the draft.'}</p>
                          </div>
                          <div className="flex items-center gap-2">
                            <label className="inline-flex items-center gap-2 rounded-full border border-slate-200 bg-white/85 px-3 py-1.5 text-xs font-medium text-slate-700">
                              <input
                                type="checkbox"
                                checked={agent.enabled}
                                onChange={(e) => updateCustomAgent(index, { enabled: e.target.checked })}
                                className="h-4 w-4 rounded text-slate-700 focus:ring-slate-400"
                              />
                              Enabled
                            </label>
                            <button
                              onClick={() => deleteCustomAgent(index)}
                              className="w-9 h-9 rounded-full border border-red-100 bg-white/80 text-red-400 hover:text-red-600 hover:border-red-200 transition-colors flex items-center justify-center"
                              title="Delete custom agent"
                            >
                              <Trash2 className="w-4 h-4" />
                            </button>
                          </div>
                        </div>

                        <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
                          <div className="space-y-3">
                            <div>
                              <label className="text-xs font-medium text-gray-500 mb-1 block">Agent title</label>
                              <input
                                type="text"
                                value={agent.title}
                                onChange={(e) => updateCustomAgent(index, { title: e.target.value })}
                                className="w-full bg-white/85 border border-white/80 rounded-2xl px-3 py-2.5 text-sm text-gray-900 focus:outline-none focus:ring-2 focus:ring-slate-500/30 transition-all"
                                placeholder="SQL Governance Reviewer"
                              />
                            </div>
                            <div>
                              <label className="text-xs font-medium text-gray-500 mb-1 block">User-facing description</label>
                              <textarea
                                value={agent.description}
                                onChange={(e) => updateCustomAgent(index, { description: e.target.value })}
                                className="w-full min-h-[100px] bg-white/85 border border-white/80 rounded-[1.4rem] px-3 py-3 text-sm text-gray-900 focus:outline-none focus:ring-2 focus:ring-slate-500/30 transition-all resize-none"
                                placeholder="Short description shown in the chat intro card."
                              />
                            </div>
                            <div className="grid grid-cols-2 gap-3">
                              <div>
                                <label className="text-xs font-medium text-gray-500 mb-1 block">Badge color</label>
                                <input
                                  type="text"
                                  value={agent.badgeColor}
                                  onChange={(e) => updateCustomAgent(index, { badgeColor: e.target.value })}
                                  className="w-full bg-white/85 border border-white/80 rounded-2xl px-3 py-2.5 text-sm text-gray-900 focus:outline-none focus:ring-2 focus:ring-slate-500/30 transition-all"
                                  placeholder="zinc"
                                />
                              </div>
                              <div>
                                <label className="text-xs font-medium text-gray-500 mb-1 block">Manager routing hint</label>
                                <input
                                  type="text"
                                  value={agent.managerRoutingHint}
                                  onChange={(e) => updateCustomAgent(index, { managerRoutingHint: e.target.value })}
                                  className="w-full bg-white/85 border border-white/80 rounded-2xl px-3 py-2.5 text-sm text-gray-900 focus:outline-none focus:ring-2 focus:ring-slate-500/30 transition-all"
                                  placeholder="Use when the user asks for..."
                                />
                              </div>
                            </div>
                          </div>

                          <div className="space-y-3">
                            <div>
                              <label className="text-xs font-medium text-gray-500 mb-1 block">Python agent code</label>
                              <textarea
                                value={agent.pythonCode}
                                onChange={(e) => updateCustomAgent(index, { pythonCode: e.target.value, status: 'draft', enabled: false })}
                                className="w-full min-h-[210px] bg-white/90 border border-white/80 rounded-[1.4rem] px-3 py-3 text-sm font-mono text-gray-900 focus:outline-none focus:ring-2 focus:ring-slate-500/30 transition-all resize-y"
                                placeholder="Paste the Python code that defines the agent behavior."
                              />
                            </div>
                            <div>
                              <label className="text-xs font-medium text-gray-500 mb-1 block">Generated system prompt</label>
                              <textarea
                                value={agent.systemPrompt}
                                onChange={(e) => updateCustomAgent(index, { systemPrompt: e.target.value })}
                                className="w-full min-h-[120px] bg-white/85 border border-white/80 rounded-[1.4rem] px-3 py-3 text-sm text-gray-900 focus:outline-none focus:ring-2 focus:ring-slate-500/30 transition-all resize-none"
                                placeholder="Filled automatically after analysis. You can fine-tune it afterwards."
                              />
                            </div>
                          </div>
                        </div>

                        <div className="mt-4 flex flex-wrap items-center gap-3">
                          <button
                            onClick={() => void analyzeCustomAgent(index)}
                            disabled={!agent.pythonCode.trim() || analysisState?.status === 'running'}
                            className="inline-flex items-center gap-2 rounded-full bg-black px-4 py-2 text-xs font-medium text-white transition-colors hover:bg-gray-800 disabled:opacity-50"
                          >
                            {analysisState?.status === 'running' ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <RefreshCw className="w-3.5 h-3.5" />}
                            Analyze & build
                          </button>
                          <span className="text-xs text-gray-500">
                            Status: <strong>{agent.status}</strong>{agent.enabled ? ' · visible in Tools' : ' · hidden from Tools'}
                          </span>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          ) : activeTab === 'mcp' ? (
            <div className="space-y-5">
              <div className="flex items-center justify-between">
                <h3 className="text-sm font-semibold text-gray-900 dark:text-gray-100 flex items-center gap-2">
                  <Network className="w-4 h-4 text-teal-500" /> MCP Tools
                </h3>
                <button
                  onClick={() => {
                    const newTool: McpTool = { id: `mcp_${Date.now()}`, label: 'New Tool', url: '', description: '', presetQuestions: [] };
                    setLocalConfig(prev => ({ ...prev, mcpTools: [...(prev.mcpTools ?? []), newTool] }));
                  }}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium bg-teal-50 text-teal-700 border border-teal-200 rounded-xl hover:bg-teal-100 transition-colors"
                >
                  <Plus className="w-3.5 h-3.5" /> Add Tool
                </button>
              </div>
              <p className="text-xs text-gray-500 dark:text-gray-400 -mt-3">These tools appear in the MCP button dropdown inside the chat interface. Add a short English description so the MCP Orchestrator can understand what each MCP is best at before planning its steps. You can also define optional starter questions so users can click and launch a guided MCP request immediately.</p>

              {(localConfig.mcpTools ?? []).length === 0 && (
                <div className="text-center py-8 text-sm text-gray-400 dark:text-gray-500 border border-dashed border-gray-200 dark:border-gray-700 rounded-xl">
                  No MCP tools yet. Click "Add Tool" to create one.
                </div>
              )}

              <div className="space-y-3">
                {(localConfig.mcpTools ?? []).map((tool: McpTool, idx: number) => {
                  const testState = mcpTestStates[tool.id];
                  return (
                    <div key={tool.id} className="p-3 bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl space-y-2">
                      <div className="flex items-start gap-3">
                        <div className="flex-1 grid grid-cols-2 gap-2">
                          <div>
                            <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Label</label>
                            <input
                              type="text"
                              value={tool.label}
                              onChange={(e) => {
                                const updated = [...(localConfig.mcpTools ?? [])];
                                updated[idx] = { ...tool, label: e.target.value };
                                setLocalConfig(prev => ({ ...prev, mcpTools: updated }));
                              }}
                              className="w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg px-3 py-2 text-sm text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-teal-500/50 transition-all"
                              placeholder="My MCP tool"
                            />
                          </div>
                          <div>
                            <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">URL (SSE or HTTP)</label>
                            <input
                              type="text"
                              value={tool.url}
                              onChange={(e) => {
                                const updated = [...(localConfig.mcpTools ?? [])];
                                updated[idx] = { ...tool, url: e.target.value };
                                setLocalConfig(prev => ({ ...prev, mcpTools: updated }));
                              }}
                              className="w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg px-3 py-2 text-sm text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-teal-500/50 transition-all"
                              placeholder="http://localhost:3000/sse"
                            />
                          </div>
                          <div className="col-span-2">
                            <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Description</label>
                            <textarea
                              value={tool.description || ''}
                              onChange={(e) => {
                                const updated = [...(localConfig.mcpTools ?? [])];
                                updated[idx] = { ...tool, description: e.target.value };
                                setLocalConfig(prev => ({ ...prev, mcpTools: updated }));
                              }}
                              className="w-full min-h-[84px] bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg px-3 py-2 text-sm text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-teal-500/50 transition-all resize-none"
                              placeholder="Explain in English what this MCP can do, what systems it can reach, and when the orchestrator should prefer it."
                            />
                          </div>
                          <div className="col-span-2 rounded-xl border border-teal-100 dark:border-teal-900/50 bg-teal-50/60 dark:bg-teal-950/20 p-3 space-y-3">
                            <div className="flex items-center justify-between gap-3">
                              <div>
                                <p className="text-xs font-semibold text-teal-800 dark:text-teal-200">Starter questions</p>
                                <p className="text-[11px] text-teal-700/80 dark:text-teal-300/80">Optional clickable questions shown in the chat when this MCP is selected. Leave empty if users should start with a blank chat.</p>
                              </div>
                              <button
                                onClick={() => {
                                  const updated = [...(localConfig.mcpTools ?? [])];
                                  const nextQuestions = [
                                    ...(tool.presetQuestions ?? []),
                                    {
                                      id: `${tool.id || 'mcp'}_preset_${Date.now()}`,
                                      label: '',
                                      prompt: '',
                                      preferredTool: '',
                                    },
                                  ];
                                  updated[idx] = { ...tool, presetQuestions: nextQuestions };
                                  setLocalConfig(prev => ({ ...prev, mcpTools: updated }));
                                }}
                                className="flex items-center gap-1.5 px-2.5 py-1.5 text-[11px] font-medium bg-white text-teal-700 border border-teal-200 rounded-lg hover:bg-teal-50 transition-colors"
                              >
                                <Plus className="w-3.5 h-3.5" /> Add question
                              </button>
                            </div>

                            {(tool.presetQuestions ?? []).length === 0 ? (
                              <div className="rounded-lg border border-dashed border-teal-200 dark:border-teal-900/60 bg-white/70 dark:bg-slate-900/30 px-3 py-3 text-[11px] text-teal-700/80 dark:text-teal-300/80">
                                No starter question yet. Users will see an empty MCP chat and can type freely.
                              </div>
                            ) : (
                              <div className="space-y-2">
                                {(tool.presetQuestions ?? []).map((preset, presetIndex) => (
                                  <div key={preset.id} className="rounded-lg border border-white/80 dark:border-slate-800 bg-white/80 dark:bg-slate-900/50 p-3 space-y-2">
                                    <div className="flex items-center justify-between gap-3">
                                      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-teal-500">
                                        Question {presetIndex + 1}
                                      </p>
                                      <button
                                        onClick={() => {
                                          const updated = [...(localConfig.mcpTools ?? [])];
                                          const nextQuestions = (tool.presetQuestions ?? []).filter((_, index) => index !== presetIndex);
                                          updated[idx] = { ...tool, presetQuestions: nextQuestions };
                                          setLocalConfig(prev => ({ ...prev, mcpTools: updated }));
                                        }}
                                        className="p-1.5 text-red-400 hover:text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20 rounded-lg transition-colors"
                                      >
                                        <Trash2 className="w-3.5 h-3.5" />
                                      </button>
                                    </div>
                                    <div className="grid gap-2 md:grid-cols-2">
                                      <div>
                                        <label className="text-[11px] font-medium text-gray-500 dark:text-gray-400 mb-1 block">Button label</label>
                                        <input
                                          type="text"
                                          value={preset.label}
                                          onChange={(e) => {
                                            const updated = [...(localConfig.mcpTools ?? [])];
                                            const nextQuestions = [...(tool.presetQuestions ?? [])];
                                            nextQuestions[presetIndex] = { ...preset, label: e.target.value };
                                            updated[idx] = { ...tool, presetQuestions: nextQuestions };
                                            setLocalConfig(prev => ({ ...prev, mcpTools: updated }));
                                          }}
                                          className="w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg px-3 py-2 text-sm text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-teal-500/50 transition-all"
                                          placeholder="List today's open tickets"
                                        />
                                      </div>
                                      <div>
                                        <label className="text-[11px] font-medium text-gray-500 dark:text-gray-400 mb-1 block">Preferred MCP tool (optional)</label>
                                        <input
                                          type="text"
                                          value={preset.preferredTool}
                                          onChange={(e) => {
                                            const updated = [...(localConfig.mcpTools ?? [])];
                                            const nextQuestions = [...(tool.presetQuestions ?? [])];
                                            nextQuestions[presetIndex] = { ...preset, preferredTool: e.target.value };
                                            updated[idx] = { ...tool, presetQuestions: nextQuestions };
                                            setLocalConfig(prev => ({ ...prev, mcpTools: updated }));
                                          }}
                                          className="w-full bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg px-3 py-2 text-sm text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-teal-500/50 transition-all"
                                          placeholder="get_incidents"
                                        />
                                      </div>
                                    </div>
                                    <div>
                                      <label className="text-[11px] font-medium text-gray-500 dark:text-gray-400 mb-1 block">Question sent to chat</label>
                                      <textarea
                                        value={preset.prompt}
                                        onChange={(e) => {
                                          const updated = [...(localConfig.mcpTools ?? [])];
                                          const nextQuestions = [...(tool.presetQuestions ?? [])];
                                          nextQuestions[presetIndex] = { ...preset, prompt: e.target.value };
                                          updated[idx] = { ...tool, presetQuestions: nextQuestions };
                                          setLocalConfig(prev => ({ ...prev, mcpTools: updated }));
                                        }}
                                        className="w-full min-h-[72px] bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg px-3 py-2 text-sm text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-teal-500/50 transition-all resize-none"
                                        placeholder="Summarize the top five incidents created today and highlight anything still unresolved."
                                      />
                                    </div>
                                  </div>
                                ))}
                              </div>
                            )}
                          </div>
                        </div>
                        <div className="flex flex-col gap-1 pt-5">
                          <button
                            onClick={() => testMcpConnection(tool)}
                            disabled={!tool.url || testState?.status === 'testing'}
                            className="px-3 py-1.5 bg-teal-50 dark:bg-teal-900/30 hover:bg-teal-100 dark:hover:bg-teal-800/40 text-teal-700 dark:text-teal-300 border border-teal-200 dark:border-teal-700 rounded-lg text-xs font-medium transition-colors flex items-center gap-1.5 whitespace-nowrap disabled:opacity-50"
                          >
                            {testState?.status === 'testing' ? <Loader2 className="w-3 h-3 animate-spin" /> : <Zap className="w-3 h-3" />}
                            Test
                          </button>
                          <button
                            onClick={() => {
                              const updated = (localConfig.mcpTools ?? []).filter((_: McpTool, i: number) => i !== idx);
                              setLocalConfig(prev => ({ ...prev, mcpTools: updated }));
                            }}
                            className="p-1.5 text-red-400 hover:text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20 rounded-lg transition-colors flex items-center justify-center"
                          >
                            <Trash2 className="w-4 h-4" />
                          </button>
                        </div>
                      </div>
                      {testState?.status === 'success' && (
                        <div className="space-y-1">
                          <p className="text-emerald-600 text-xs flex items-center gap-1"><CheckCircle2 className="w-3 h-3" /> {testState.message}</p>
                          {testState.tools.length > 0 && (
                            <div className="flex flex-wrap gap-1 mt-1">
                              {testState.tools.map(t => (
                                <span key={t.name} title={t.description} className="px-2 py-0.5 bg-teal-50 dark:bg-teal-900/30 text-teal-700 dark:text-teal-300 border border-teal-200 dark:border-teal-700 rounded-md text-[10px] font-medium">
                                  {t.name}
                                </span>
                              ))}
                            </div>
                          )}
                        </div>
                      )}
                      {testState?.status === 'error' && (
                        <p className="text-red-600 text-xs flex items-center gap-1"><XCircle className="w-3 h-3" /> {testState.message}</p>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          ) : activeTab === 'llm' ? (
            <div className="space-y-5">
                <div className="rounded-2xl border border-amber-200/80 bg-amber-50/70 px-4 py-3">
                  <label className="flex items-start gap-3 cursor-pointer select-none">
                    <input
                      type="checkbox"
                      checked={localConfig.disableSslVerification}
                      onChange={(e) => setLocalConfig({ ...localConfig, disableSslVerification: e.target.checked })}
                      className="mt-0.5 w-4 h-4 rounded text-amber-500 focus:ring-amber-500"
                    />
                    <span>
                      <span className="block text-sm font-medium text-amber-900">Disable SSL verification for backend calls</span>
                      <span className="block text-xs text-amber-800/80 mt-1">
                        Applies to LLM, embeddings, ClickHouse, MCP, and backend model discovery. Use this only for self-signed or internal certificates.
                      </span>
                    </span>
                  </label>
                </div>

                <div>
                  <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                    <Zap className="w-4 h-4" /> Provider
                  </label>
                  <div className="flex gap-6 items-center bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3">
                    <label className="flex items-center gap-2 cursor-pointer text-sm font-medium dark:text-gray-200">
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
                    <label className="flex items-center gap-2 cursor-pointer text-sm font-medium dark:text-gray-200">
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
                  <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                    <Server className="w-4 h-4" /> Base URL
                  </label>
                  <div className="flex gap-2">
                    <input
                      type="text"
                      value={localConfig.baseUrl}
                      onChange={(e) => setLocalConfig({ ...localConfig, baseUrl: e.target.value })}
                      className="flex-1 bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                      placeholder={localConfig.provider === 'ollama' ? "http://localhost:11434" : "http://localhost:1234/v1"}
                    />
                    <button 
                      onClick={() => fetchModels(true)}
                      disabled={testStatus === 'testing'}
                      className="px-4 py-2 bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-200 rounded-xl font-medium transition-colors flex items-center gap-2 whitespace-nowrap text-sm"
                    >
                      {testStatus === 'testing' ? <Loader2 className="w-4 h-4 animate-spin" /> : 'Test Connection'}
                    </button>
                  </div>
                  {testStatus === 'success' && <p className="text-emerald-600 text-xs mt-2 flex items-center gap-1"><CheckCircle2 className="w-3 h-3"/> {testMessage}</p>}
                  {testStatus === 'error' && <p className="text-red-600 text-xs mt-2 flex items-center gap-1"><XCircle className="w-3 h-3"/> {testMessage}</p>}
                </div>

                {localConfig.provider === 'openai' && (
                  <div>
                    <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                      <Key className="w-4 h-4" /> API Key (Optional for local)
                    </label>
                    <input
                      type="password"
                      value={localConfig.apiKey}
                      onChange={(e) => setLocalConfig({ ...localConfig, apiKey: e.target.value })}
                      className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
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
                      className="text-blue-500 hover:text-blue-400 flex items-center gap-1 text-xs font-medium"
                    >
                      <RefreshCw className={`w-3 h-3 ${isRefreshing ? 'animate-spin' : ''}`} /> Refresh Models
                    </button>
                  </div>
                  {models.length > 0 ? (
                    <select
                      value={localConfig.model}
                      onChange={(e) => setLocalConfig({ ...localConfig, model: e.target.value })}
                      className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all appearance-none"
                    >
                      {models.map(m => <option key={m} value={m}>{m}</option>)}
                    </select>
                  ) : (
                    <input
                      type="text"
                      value={localConfig.model}
                      onChange={(e) => setLocalConfig({ ...localConfig, model: e.target.value })}
                      className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                      placeholder="llama3, gpt-4, etc."
                    />
                  )}
                </div>

                <div>
                  <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                    <Server className="w-4 h-4" /> Documentation URL
                  </label>
                  <input
                    type="url"
                    value={localConfig.documentationUrl}
                    onChange={(e) => setLocalConfig({ ...localConfig, documentationUrl: e.target.value })}
                    className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                    placeholder="https://docs.example.com"
                  />
                  <p className="text-xs text-gray-400 dark:text-gray-500 mt-1">This link opens when the user clicks "Documentation" on the home page. Leave it empty to hide the button.</p>
                </div>

                <div>
                  <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                    <MessageSquare className="w-4 h-4" /> System Prompt
                  </label>
                  <textarea
                    value={localConfig.systemPrompt}
                    onChange={(e) => setLocalConfig({ ...localConfig, systemPrompt: e.target.value })}
                    className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all min-h-[100px] resize-none"
                    placeholder="You are a helpful assistant..."
                  />
                </div>

                <div className="rounded-2xl border border-blue-200/80 bg-blue-50/70 px-4 py-3">
                  <label className="flex items-start gap-3 cursor-pointer select-none">
                    <input
                      type="checkbox"
                      checked={localConfig.managerUseRagFunctionalContext}
                      onChange={(e) => setLocalConfig({ ...localConfig, managerUseRagFunctionalContext: e.target.checked })}
                      className="mt-0.5 w-4 h-4 rounded text-blue-500 focus:ring-blue-500"
                    />
                    <span>
                      <span className="block text-sm font-medium text-blue-900">Manager preloads functional context from RAG</span>
                      <span className="block text-xs text-blue-800/80 mt-1">
                        When enabled, Agent Manager queries the knowledge base before routing or answering so it can reuse field names, definitions, and business descriptions. When disabled, Manager behaves exactly as before.
                      </span>
                    </span>
                  </label>
                </div>
            </div>
          ) : activeTab === 'rag' ? (
            <div className="space-y-5">
              {/* OpenSearch URL */}
              <div>
                <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                  <Database className="w-4 h-4" /> OpenSearch URL
                </label>
                <div className="flex gap-2">
                  <input
                    type="text"
                    value={localConfig.elasticsearchUrl}
                    onChange={(e) => setLocalConfig({ ...localConfig, elasticsearchUrl: e.target.value })}
                    className="flex-1 bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                    placeholder="http://localhost:9200"
                  />
                  <button
                    onClick={testElasticsearchConnection}
                    disabled={esTestStatus === 'testing'}
                    className="px-4 py-2 bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-200 rounded-xl font-medium transition-colors flex items-center gap-2 whitespace-nowrap text-sm"
                  >
                    {esTestStatus === 'testing' ? <Loader2 className="w-4 h-4 animate-spin" /> : 'Test Connection'}
                  </button>
                </div>
                {esTestStatus === 'success' && <p className="text-emerald-600 text-xs mt-2 flex items-center gap-1"><CheckCircle2 className="w-3 h-3" /> {esTestMessage}</p>}
                {esTestStatus === 'error' && <p className="text-red-600 text-xs mt-2 flex items-center gap-1"><XCircle className="w-3 h-3" /> {esTestMessage}</p>}
              </div>

              {/* OpenSearch Index */}
              <div>
                <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                  <Layers className="w-4 h-4" /> OpenSearch Index
                </label>
                <div className="flex gap-2">
                  <input
                    type="text"
                    value={localConfig.elasticsearchIndex}
                    onChange={(e) => setLocalConfig({ ...localConfig, elasticsearchIndex: e.target.value })}
                    className="flex-1 bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                    placeholder="rag_documents"
                  />
                  <button
                    onClick={setupIndex}
                    disabled={setupStatus === 'running'}
                    className="px-4 py-2 bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-200 rounded-xl font-medium transition-colors flex items-center gap-2 whitespace-nowrap text-sm"
                    title="Create kNN index in OpenSearch"
                  >
                    {setupStatus === 'running' ? <Loader2 className="w-4 h-4 animate-spin" /> : 'Setup Index'}
                  </button>
                </div>
                {setupStatus === 'success' && <p className="text-emerald-600 text-xs mt-2 flex items-center gap-1"><CheckCircle2 className="w-3 h-3" /> {setupMessage}</p>}
                {setupStatus === 'error' && <p className="text-red-600 text-xs mt-2 flex items-center gap-1"><XCircle className="w-3 h-3" /> {setupMessage}</p>}
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                    <Key className="w-4 h-4" /> Username
                  </label>
                  <input
                    type="text"
                    value={localConfig.elasticsearchUsername}
                    onChange={(e) => setLocalConfig({ ...localConfig, elasticsearchUsername: e.target.value })}
                    className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                    placeholder="elastic"
                    autoComplete="username"
                  />
                </div>
                <div>
                  <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                    <Key className="w-4 h-4" /> Password
                  </label>
                  <input
                    type="password"
                    value={localConfig.elasticsearchPassword}
                    onChange={(e) => setLocalConfig({ ...localConfig, elasticsearchPassword: e.target.value })}
                    className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                    placeholder="••••••••"
                    autoComplete="current-password"
                  />
                </div>
              </div>

              <div className="pt-4 border-t border-gray-100 dark:border-gray-800">
                <h3 className="text-sm font-semibold text-gray-900 dark:text-gray-100 mb-4 flex items-center gap-2">
                  <Bot className="w-4 h-4 text-blue-500" /> Embedding Model Configuration
                </h3>
                
                <div className="space-y-4">
                  <div>
                    <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                      <Server className="w-4 h-4" /> Embedding endpoint URL
                    </label>
                    <div className="flex flex-wrap items-center gap-2 mb-2">
                      {(['http', 'https'] as const).map((scheme) => {
                        const active = embeddingUrlScheme === scheme;
                        return (
                          <button
                            key={scheme}
                            type="button"
                            onClick={() => {
                              setLocalConfig({
                                ...localConfig,
                                embeddingBaseUrl: rewriteUrlScheme(localConfig.embeddingBaseUrl, scheme),
                              });
                              setEmbeddingModels([]);
                              setEmbeddingModelsMessage('');
                            }}
                            className={`px-3 py-1.5 rounded-full text-xs font-semibold transition-all border ${
                              active
                                ? 'bg-blue-600 text-white border-blue-600 shadow-sm'
                                : 'bg-white/70 dark:bg-gray-800/70 text-gray-600 dark:text-gray-300 border-gray-200 dark:border-gray-700 hover:border-blue-300'
                            }`}
                          >
                            {scheme.toUpperCase()}
                          </button>
                        );
                      })}
                    </div>
                    <div className="flex gap-2">
                      <input
                        type="text"
                        value={localConfig.embeddingBaseUrl}
                        onChange={(e) => {
                          setLocalConfig({ ...localConfig, embeddingBaseUrl: e.target.value });
                          setEmbeddingModels([]);
                          setEmbeddingModelsMessage('');
                        }}
                        className="flex-1 bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                        placeholder="https://host.example.com/v2:embeddings"
                      />
                      <button
                        onClick={testEmbeddingConnection}
                        disabled={embedTestStatus === 'testing'}
                        className="px-4 py-2 bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-200 rounded-xl font-medium transition-colors flex items-center gap-2 whitespace-nowrap text-sm"
                      >
                        {embedTestStatus === 'testing' ? <Loader2 className="w-4 h-4 animate-spin" /> : 'Test Embeddings'}
                      </button>
                    </div>
                    <p className="text-xs text-gray-400 dark:text-gray-500 mt-1">
                      Use either an OpenAI-compatible base URL such as <code>https://host/v1</code>, or a direct endpoint URL such as <code>https://host/v2:embeddings</code> or <code>https://host/v1/embeddings</code>.
                    </p>
                    <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                      The same endpoint is used for both document vectorization and user-query vectorization before searching your configured OpenSearch index.
                    </p>
                    {isDirectEmbeddingEndpoint && (
                      <p className="text-xs text-blue-600 dark:text-blue-400 mt-1">
                        Direct embedding endpoint detected. RAGnarok will derive the sibling model discovery endpoint automatically when possible.
                      </p>
                    )}
                    <label className="flex items-center gap-2 mt-2 cursor-pointer select-none w-fit">
                      <input
                        type="checkbox"
                        checked={localConfig.embeddingVerifySsl}
                        onChange={(e) => setLocalConfig({ ...localConfig, embeddingVerifySsl: e.target.checked })}
                        className="w-4 h-4 rounded text-blue-500 focus:ring-blue-500"
                      />
                      <span className="text-xs text-gray-600 dark:text-gray-400">
                        Verify SSL certificate{localConfig.disableSslVerification ? ' (overridden globally)' : ''}
                      </span>
                    </label>
                    {embedTestStatus === 'success' && <p className="text-emerald-600 text-xs mt-2 flex items-center gap-1"><CheckCircle2 className="w-3 h-3"/> {embedTestMessage}</p>}
                    {embedTestStatus === 'error' && <p className="text-red-600 text-xs mt-2 flex items-center gap-1"><XCircle className="w-3 h-3"/> {embedTestMessage}</p>}
                  </div>

                  <div>
                    <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                      <Key className="w-4 h-4" /> API Key
                    </label>
                    <input
                      type="password"
                      value={localConfig.embeddingApiKey}
                      onChange={(e) => setLocalConfig({ ...localConfig, embeddingApiKey: e.target.value })}
                      className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                      placeholder="Bearer token or API key"
                    />
                  </div>

                  <div>
                    <div className="flex items-center justify-between mb-2">
                      <label className="flex items-center gap-2 text-sm font-medium text-gray-700">
                        <Bot className="w-4 h-4" /> Model name
                      </label>
                      <button
                        onClick={fetchEmbeddingModels}
                        className="text-blue-500 hover:text-blue-400 flex items-center gap-1 text-xs font-medium"
                      >
                        <RefreshCw className={`w-3 h-3 ${isRefreshingEmbed ? 'animate-spin' : ''}`} /> Discover models
                      </button>
                    </div>
                    {embeddingModels.length > 0 ? (
                      <select
                        value={localConfig.embeddingModel}
                        onChange={(e) => setLocalConfig({ ...localConfig, embeddingModel: e.target.value })}
                        className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all appearance-none"
                      >
                        {embeddingModels.map(m => <option key={m} value={m}>{m}</option>)}
                      </select>
                    ) : (
                      <input
                        type="text"
                        value={localConfig.embeddingModel}
                        onChange={(e) => setLocalConfig({ ...localConfig, embeddingModel: e.target.value })}
                        className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                        placeholder="nomic-embed-text"
                      />
                    )}
                    <p className="text-xs text-gray-500 mt-1">
                      Optional when your embedding provider infers the model from the endpoint. Keep it filled when your provider still expects a model in the request body.
                    </p>
                    {embeddingModelsMessage && (
                      <p className="text-xs text-gray-500 mt-1">{embeddingModelsMessage}</p>
                    )}
                  </div>
                </div>
              </div>

              <div className="pt-4 border-t border-gray-100 grid grid-cols-3 gap-4">
                <div>
                  <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                    <SlidersHorizontal className="w-4 h-4" /> Chunk Size
                  </label>
                  <input
                    type="number"
                    value={localConfig.chunkSize}
                    onChange={(e) => setLocalConfig({ ...localConfig, chunkSize: parseInt(e.target.value) || 512 })}
                    className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                  />
                </div>
                <div>
                  <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                    <SlidersHorizontal className="w-4 h-4" /> Chunk Overlap
                  </label>
                  <input
                    type="number"
                    value={localConfig.chunkOverlap}
                    onChange={(e) => setLocalConfig({ ...localConfig, chunkOverlap: parseInt(e.target.value) || 50 })}
                    className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                  />
                </div>
                <div>
                  <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                    <SlidersHorizontal className="w-4 h-4" /> KNN Neighbors
                  </label>
                  <input
                    type="number"
                    value={localConfig.knnNeighbors}
                    onChange={(e) => setLocalConfig({ ...localConfig, knnNeighbors: parseInt(e.target.value) || 50 })}
                    className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                  />
                </div>
              </div>

              {/* ── Document Ingest ── */}
              <div className="pt-4 border-t border-gray-100 dark:border-gray-800 space-y-3">
                <h3 className="text-sm font-semibold text-gray-900 dark:text-gray-100 flex items-center gap-2">
                  <UploadCloud className="w-4 h-4 text-blue-500" /> Index a Document
                </h3>
                <p className="text-xs text-gray-500 dark:text-gray-400 -mt-2">
                  Chunk, embed and push a document into OpenSearch.
                </p>

                {/* Doc name + file picker */}
                <div className="flex gap-2">
                  <input
                    type="text"
                    value={ingestDocName}
                    onChange={(e) => setIngestDocName(e.target.value)}
                    placeholder="Document name"
                    className="flex-1 bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-2.5 text-sm text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all"
                  />
                  <button
                    onClick={() => fileInputRef.current?.click()}
                    className="px-3 py-2 bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-200 rounded-xl transition-colors flex items-center gap-1.5 text-sm font-medium whitespace-nowrap"
                  >
                    <FolderOpen className="w-4 h-4" /> Load file
                  </button>
                  <input
                    ref={fileInputRef}
                    type="file"
                    accept=".txt,.md,.csv,.json"
                    className="hidden"
                    onChange={handleFileLoad}
                  />
                </div>

                {/* Text area */}
                <textarea
                  value={ingestText}
                  onChange={(e) => setIngestText(e.target.value)}
                  rows={4}
                  placeholder="Paste document text here, or load a .txt / .md / .csv file above…"
                  className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-sm text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all resize-none"
                />

                <button
                  onClick={ingestDocument}
                  disabled={ingestStatus === 'indexing' || !ingestText.trim() || !ingestDocName.trim()}
                  className="w-full py-2.5 rounded-xl bg-blue-600 hover:bg-blue-700 disabled:opacity-40 disabled:cursor-not-allowed text-white text-sm font-semibold flex items-center justify-center gap-2 transition-colors"
                >
                  {ingestStatus === 'indexing'
                    ? <><Loader2 className="w-4 h-4 animate-spin" /> Indexing…</>
                    : <><UploadCloud className="w-4 h-4" /> Index Document</>
                  }
                </button>
                {ingestStatus === 'success' && <p className="text-emerald-600 text-xs flex items-center gap-1"><CheckCircle2 className="w-3 h-3" /> {ingestMessage}</p>}
                {ingestStatus === 'error'   && <p className="text-red-600   text-xs flex items-center gap-1"><XCircle      className="w-3 h-3" /> {ingestMessage}</p>}
              </div>
            </div>
          ) : activeTab === 'clickhouse' ? (
            <div className="space-y-6">
              <div className="p-4 rounded-2xl bg-cyan-50/80 dark:bg-cyan-900/20 border border-cyan-200/70 dark:border-cyan-700/40">
                <div className="flex items-center gap-2 mb-2">
                  <Database className="w-4 h-4 text-cyan-600 dark:text-cyan-300" />
                  <h3 className="text-sm font-semibold text-cyan-900 dark:text-cyan-200">ClickHouse SQL</h3>
                </div>
                <p className="text-xs text-cyan-800/90 dark:text-cyan-300/90 leading-relaxed">
                  Configure the ClickHouse connection used by ClickHouse SQL, Data Analyst, Auto-ML, and any Manager workflow that needs ClickHouse queries. All existing tests, safety limits, and table previews stay available here.
                </p>
              </div>

              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                      <Server className="w-4 h-4" /> Host
                    </label>
                    <input
                      type="text"
                      value={localConfig.clickhouseHost}
                      onChange={(e) => setLocalConfig({ ...localConfig, clickhouseHost: e.target.value })}
                      className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-cyan-500/50 transition-all"
                      placeholder="localhost"
                    />
                  </div>
                  <div>
                    <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                      <Server className="w-4 h-4" /> Port
                    </label>
                    <input
                      type="number"
                      value={localConfig.clickhousePort}
                      onChange={(e) => setLocalConfig({ ...localConfig, clickhousePort: parseInt(e.target.value, 10) || 8123 })}
                      className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-cyan-500/50 transition-all"
                      placeholder="8123"
                    />
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                      <Database className="w-4 h-4" /> Database
                    </label>
                    <input
                      type="text"
                      value={localConfig.clickhouseDatabase}
                      onChange={(e) => setLocalConfig({ ...localConfig, clickhouseDatabase: e.target.value })}
                      className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-cyan-500/50 transition-all"
                      placeholder="default"
                    />
                  </div>
                  <div>
                    <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                      <Server className="w-4 h-4" /> HTTP Path
                    </label>
                    <input
                      type="text"
                      value={localConfig.clickhouseHttpPath}
                      onChange={(e) => setLocalConfig({ ...localConfig, clickhouseHttpPath: e.target.value })}
                      className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-cyan-500/50 transition-all"
                      placeholder="Leave empty for default root path"
                    />
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                      <Key className="w-4 h-4" /> Username
                    </label>
                    <input
                      type="text"
                      value={localConfig.clickhouseUsername}
                      onChange={(e) => setLocalConfig({ ...localConfig, clickhouseUsername: e.target.value })}
                      className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-cyan-500/50 transition-all"
                      placeholder="default"
                      autoComplete="username"
                    />
                  </div>
                  <div>
                    <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                      <Key className="w-4 h-4" /> Password
                    </label>
                    <input
                      type="password"
                      value={localConfig.clickhousePassword}
                      onChange={(e) => setLocalConfig({ ...localConfig, clickhousePassword: e.target.value })}
                      className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-cyan-500/50 transition-all"
                      placeholder="••••••••"
                      autoComplete="current-password"
                    />
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <label className="flex items-center gap-2 mt-2 cursor-pointer select-none">
                    <input
                      type="checkbox"
                      checked={localConfig.clickhouseSecure}
                      onChange={(e) => setLocalConfig({ ...localConfig, clickhouseSecure: e.target.checked })}
                      className="w-4 h-4 rounded text-cyan-500 focus:ring-cyan-500"
                    />
                    <span className="text-xs text-gray-600 dark:text-gray-400">Use HTTPS</span>
                  </label>
                  <label className="flex items-center gap-2 mt-2 cursor-pointer select-none">
                    <input
                      type="checkbox"
                      checked={localConfig.clickhouseVerifySsl}
                      onChange={(e) => setLocalConfig({ ...localConfig, clickhouseVerifySsl: e.target.checked })}
                      className="w-4 h-4 rounded text-cyan-500 focus:ring-cyan-500"
                    />
                    <span className="text-xs text-gray-600 dark:text-gray-400">
                      Verify SSL certificate{localConfig.disableSslVerification ? ' (overridden globally)' : ''}
                    </span>
                  </label>
                </div>

                <div>
                  <label className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">
                    <SlidersHorizontal className="w-4 h-4" /> Default Query Limit
                  </label>
                  <input
                    type="number"
                    value={localConfig.clickhouseQueryLimit}
                    onChange={(e) => setLocalConfig({ ...localConfig, clickhouseQueryLimit: parseInt(e.target.value, 10) || 200 })}
                    className="w-full bg-white/50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-3 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-cyan-500/50 transition-all"
                  />
                  <p className="text-xs text-gray-500 mt-1">Used by the ClickHouse agent to keep row-level queries bounded and safe.</p>
                </div>

                <div className="flex gap-2">
                  <button
                    onClick={testClickHouseConnection}
                    disabled={clickhouseTestStatus === 'testing'}
                    className="px-4 py-2 bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-200 rounded-xl font-medium transition-colors flex items-center gap-2 whitespace-nowrap text-sm"
                  >
                    {clickhouseTestStatus === 'testing' ? <Loader2 className="w-4 h-4 animate-spin" /> : 'Test ClickHouse'}
                  </button>
                </div>
                {clickhouseTestStatus === 'success' && <p className="text-emerald-600 text-xs mt-2 flex items-center gap-1"><CheckCircle2 className="w-3 h-3" /> {clickhouseTestMessage}</p>}
                {clickhouseTestStatus === 'error' && <p className="text-red-600 text-xs mt-2 flex items-center gap-1"><XCircle className="w-3 h-3" /> {clickhouseTestMessage}</p>}

                {clickhouseTablesPreview.length > 0 && (
                  <div className="space-y-2">
                    <p className="text-xs font-medium text-gray-600 dark:text-gray-300">Available tables preview</p>
                    <div className="flex flex-wrap gap-1">
                      {clickhouseTablesPreview.map((table) => (
                        <span
                          key={table}
                          className="px-2 py-0.5 bg-cyan-50 dark:bg-cyan-900/30 text-cyan-700 dark:text-cyan-300 border border-cyan-200 dark:border-cyan-700 rounded-md text-[10px] font-medium"
                        >
                          {table}
                        </span>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          ) : null}

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
