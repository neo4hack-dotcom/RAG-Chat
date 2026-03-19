/**
 * Utility function to conditionally join class names together.
 * Useful for Tailwind CSS classes.
 */
export function cn(...classes: (string | undefined | null | false)[]) {
  return classes.filter(Boolean).join(" ");
}

/**
 * Represents a single step in an Agent's thinking process.
 */
export type AgentStep = {
  id: string;
  title: string;
  status: 'pending' | 'running' | 'success' | 'error';
  details?: string;
  step?: number;
  type?: string;
  reasoning?: string;
  sql?: string;
  result_summary?: string;
  row_count?: number;
  ok?: boolean;
  retried?: boolean;
  suggested_path?: string;
};

export type ChatAction = {
  id: string;
  label: string;
  actionType:
    | 'open_planning_form'
    | 'edit_planning_plan'
    | 'refresh_planning_state'
    | 'confirm_file_action'
    | 'cancel_file_action'
    | 'export_data_quality_pdf';
  variant?: 'primary' | 'secondary';
  payload?: Record<string, unknown>;
};

/**
 * Represents a file attachment uploaded by the user.
 */
export type Attachment = {
  id: string;
  name: string;
  type: string;
  data: string; // base64 data URL
};

/**
 * Represents a single message in a conversation.
 * Can include attachments, agent steps, and RAG sources.
 */
export type Message = {
  id: string;
  role: "user" | "assistant" | "system";
  content: string;
  timestamp: number;
  steps?: AgentStep[];
  actions?: ChatAction[];
  attachments?: Attachment[];
  sources?: { id: string; docName: string; text: string; score: number }[];
  confidence?: number;
  chart?: ChartSpec;
};

export type ConversationMemoryStep = {
  role: "user" | "assistant";
  content: string;
  timestamp: number;
};

export type ConversationMemory = {
  steps: ConversationMemoryStep[];
  keptSteps: number;
  updatedAt: number;
};

export type ClickHouseSchemaColumn = {
  name: string;
  type: string;
  defaultKind?: string;
  defaultExpression?: string;
};

export type ClickHouseResultColumn = {
  name: string;
  type: string;
};

export type ChartType = 'bar' | 'line' | 'area' | 'scatter';

export type ChartPoint = {
  x: string;
  y: number;
};

export type ChartSpec = {
  type: ChartType;
  title: string;
  xField: string;
  yField: string;
  points: ChartPoint[];
};

export type PlanningTriggerKind = 'once' | 'daily' | 'weekly' | 'interval' | 'clickhouse_watch' | 'file_watch';

export type PlanningWeekday = 'mon' | 'tue' | 'wed' | 'thu' | 'fri' | 'sat' | 'sun';

export type ClickHouseWatchMode = 'returns_rows' | 'count_increases' | 'result_changes';

export type CrewPlanTrigger = {
  kind: PlanningTriggerKind;
  timezone: string;
  oneTimeAt: string;
  timeOfDay: string;
  weekdays: PlanningWeekday[];
  intervalMinutes: number;
  pollMinutes: number;
  watchSql: string;
  watchMode: ClickHouseWatchMode;
  directory: string;
  pattern: string;
  recursive: boolean;
};

export type CrewPlanDraft = {
  name: string;
  prompt: string;
  agents: AgentRole[];
  status: 'active' | 'paused';
  trigger: CrewPlanTrigger;
};

export type CrewPlan = CrewPlanDraft & {
  id: string;
  createdAt: string;
  updatedAt: string;
  nextRunAt: string | null;
  lastRunAt: string | null;
  lastStatus: 'success' | 'error' | 'running' | null;
  lastSummary: string;
  runtime?: Record<string, unknown>;
};

export type CrewPlanRunOutput = {
  agent: AgentRole;
  status: 'success' | 'error';
  content: string;
};

export type CrewPlanRun = {
  id: string;
  planId: string;
  planName: string;
  triggerKind: PlanningTriggerKind | 'manual';
  triggerLabel: string;
  startedAt: string;
  finishedAt: string | null;
  status: 'running' | 'success' | 'error';
  summary: string;
  outputs: CrewPlanRunOutput[];
};

export type PlanningBackendState = {
  plans: CrewPlan[];
  runs: CrewPlanRun[];
};

export type PlanningAgentState = {
  draft: CrewPlanDraft;
  missingFields: string[];
  lastQuestion: string;
  readyToReview: boolean;
};

export type FileManagerPendingAction = {
  toolName: string;
  toolInput: Record<string, unknown>;
  preview: string;
  summary: string;
  requestedAt: string;
};

export type FileManagerAgentState = {
  pendingConfirmation: FileManagerPendingAction | null;
  lastToolResult: string;
  lastVisitedPath: string;
};

export type PdfCreatorPendingAction = {
  preview: string;
  summary: string;
  requestedAt: string;
};

export type PdfCreatorAgentState = {
  stage: 'idle' | 'awaiting_source_choice' | 'awaiting_content';
  pendingDocument: Record<string, unknown> | null;
  pendingConfirmation: PdfCreatorPendingAction | null;
  lastOutputPath: string;
  lastTitle: string;
};

export type OracleConnectionConfig = {
  id: string;
  label: string;
  host: string;
  port: number;
  serviceName: string;
  sid: string;
  dsn: string;
  username: string;
  password: string;
};

export type OracleAnalystAgentConfig = {
  connectionId: string;
  rowLimit: number;
  maxRetries: number;
  maxIterations: number;
  toolkitId: string;
  systemPrompt: string;
};

export type OracleAnalystAgentState = {
  stage: 'idle' | 'awaiting_table' | 'ready';
  pendingRequest: string;
  availableTables: string[];
  selectedTable: string | null;
  schemaInfo: Array<{ name: string; type: string; nullable?: boolean }>;
  clarificationPrompt: string;
  clarificationOptions: string[];
  lastSql: string;
  lastResultMeta: Array<{ name: string; type: string }>;
  lastResultRows: Record<string, unknown>[];
  finalAnswer: string;
  actionLog: string[];
  lastError: string;
};

export type DataAnalystAgentState = {
  stage: 'idle' | 'awaiting_table' | 'ready';
  pendingRequest: string;
  availableTables: string[];
  selectedTable: string | null;
  tableSchema: Array<{ name: string; type: string; defaultKind?: string; defaultExpression?: string }>;
  clarificationPrompt: string;
  clarificationOptions: string[];
  lastSqls: string[];
  lastResultMeta: Array<{ name: string; type: string }>;
  lastResultRows: Record<string, unknown>[];
  finalAnswer: string;
  lastError: string;
  lastExportPath: string;
  knowledgeHits: Array<{ docName: string; text: string; score: number }>;
};

export type DataQualitySchemaColumn = {
  name: string;
  type: string;
  category: 'numeric' | 'string' | 'date' | 'other';
};

export type DataQualityState = {
  stage: string;
  table: string | null;
  columns: string[];
  sampleSize: number;
  rowFilter: string;
  timeColumn: string | null;
  dbType: 'clickhouse' | 'oracle';
  schemaInfo: DataQualitySchemaColumn[];
  columnStats: Record<string, unknown>;
  volumetricStats: Record<string, unknown> | null;
  llmAnalysis: string;
  finalAnswer: string;
  agentId: string;
  sessionId: string;
  lastError: string;
  availableTables: string[];
  availableColumns: string[];
  dateColumns: string[];
};

export type ManagerDelegateRole = 'clickhouse_query' | 'data_analyst' | 'file_management' | 'pdf_creator' | 'oracle_analyst' | 'data_quality_tables';

export type ManagerFileExportPipeline = {
  kind: 'clickhouse_to_file';
  stage: 'awaiting_clickhouse' | 'awaiting_export_details';
  nextDelegate: 'file_management';
  exportFormat: 'csv' | 'tsv' | 'xlsx' | null;
  targetPath: string;
  sourceRequest: string;
  reason: string;
};

export type ManagerPdfExportPipeline = {
  kind: 'clickhouse_to_pdf';
  stage: 'awaiting_clickhouse';
  nextDelegate: 'pdf_creator';
  targetPath: string;
  sourceRequest: string;
  reason: string;
  title: string;
};

export type ManagerPendingPipeline = ManagerFileExportPipeline | ManagerPdfExportPipeline;

export type ManagerAgentState = {
  activeDelegate: ManagerDelegateRole | null;
  lastRoutingReason: string;
  lastDelegateLabel: string;
  pendingPipeline: ManagerPendingPipeline | null;
};

export type ClickHouseAgentState = {
  stage: 'idle' | 'awaiting_table' | 'awaiting_field' | 'awaiting_date' | 'awaiting_chart_offer' | 'awaiting_chart_x' | 'awaiting_chart_y' | 'awaiting_chart_type' | 'ready';
  pendingRequest: string;
  availableTables: string[];
  selectedTable: string | null;
  schema: ClickHouseSchemaColumn[];
  candidateFields: string[];
  dateFields: string[];
  selectedField: string | null;
  selectedDateField: string | null;
  clarificationPrompt: string;
  clarificationOptions: string[];
  lastSql: string;
  lastResultMeta: ClickHouseResultColumn[];
  lastResultRows: Record<string, unknown>[];
  chartRequested: boolean;
  chartSuggested: boolean;
  chartOfferOptions: string[];
  chartXOptions: string[];
  chartYOptions: string[];
  chartTypeOptions: string[];
  selectedChartX: string | null;
  selectedChartY: string | null;
  selectedChartType: ChartType | null;
};

export type ConversationAgentState = {
  manager?: ManagerAgentState;
  clickhouse?: ClickHouseAgentState;
  dataAnalyst?: DataAnalystAgentState;
  planning?: PlanningAgentState;
  fileManager?: FileManagerAgentState;
  pdfCreator?: PdfCreatorAgentState;
  oracleAnalyst?: OracleAnalystAgentState;
  dataQuality?: DataQualityState;
};

/**
 * Represents a complete chat conversation.
 */
export type Conversation = {
  id: string;
  title: string;
  messages: Message[];
  memory?: ConversationMemory;
  updatedAt: number;
  agentState?: ConversationAgentState;
};

export type WorkflowMode = 'LLM' | 'RAG' | 'AGENT' | 'MCP' | 'CREWAI';

export type AgentRole = 'manager' | 'clickhouse_query' | 'data_analyst' | 'file_management' | 'pdf_creator' | 'oracle_analyst' | 'data_quality_tables';

export type Page = 'landing' | 'chat' | 'dataviz' | 'agents';

const VALID_AGENT_ROLES: AgentRole[] = ['manager', 'clickhouse_query', 'data_analyst', 'file_management', 'pdf_creator', 'oracle_analyst', 'data_quality_tables'];

function isValidAgentRole(value: unknown): value is AgentRole {
  return typeof value === 'string' && VALID_AGENT_ROLES.includes(value as AgentRole);
}

/**
 * Represents a single MCP tool entry configurable by the user.
 */
export type McpTool = {
  id: string;
  label: string;
  url: string;
};

export type PortalApp = {
  id: string;
  name: string;
  url: string;
  description: string;
};

/**
 * Global application configuration settings.
 */
export type AppConfig = {
  provider: 'openai' | 'ollama';
  baseUrl: string;
  apiKey: string;
  model: string;
  systemPrompt: string;
  disableSslVerification: boolean;
  elasticsearchUrl: string;
  elasticsearchIndex: string;
  elasticsearchUsername: string;
  elasticsearchPassword: string;
  embeddingBaseUrl: string;
  embeddingApiKey: string;
  embeddingModel: string;
  embeddingVerifySsl: boolean;
  chunkSize: number;
  chunkOverlap: number;
  knnNeighbors: number;
  mcpTools: McpTool[];
  documentationUrl: string;
  portalApps: PortalApp[];
  settingsAccessPassword: string;
  clickhouseHost: string;
  clickhousePort: number;
  clickhouseDatabase: string;
  clickhouseUsername: string;
  clickhousePassword: string;
  clickhouseSecure: boolean;
  clickhouseVerifySsl: boolean;
  clickhouseHttpPath: string;
  clickhouseQueryLimit: number;
  oracleConnections: OracleConnectionConfig[];
  oracleAnalystConfig: OracleAnalystAgentConfig;
  fileManagerConfig: FileManagerAgentConfig;
};

export type FileManagerAgentConfig = {
  basePath: string;
  maxIterations: number;
  systemPrompt: string;
};

export type AppPreferences = {
  darkMode: boolean;
  currentConversationId: string | null;
  workflow: WorkflowMode;
  agentRole: AgentRole;
  selectedMcpToolId: string;
  page: Page;
};

export type PersistedAppState = {
  config: AppConfig;
  conversations: Conversation[];
  preferences: AppPreferences;
  updatedAt?: string;
};

/**
 * Default configuration values used when the app first loads.
 */
export const DEFAULT_CONFIG: AppConfig = {
  provider: 'ollama',
  baseUrl: "http://localhost:11434",
  apiKey: "",
  model: "llama3",
  systemPrompt: "You are a helpful, smart, and concise AI assistant. Present non-JSON answers in polished Markdown with clear sections, concise bullets, tasteful **bold** emphasis, and tables when they help. Safe semantic HTML fragments such as <section>, <article>, <details>, <summary>, <table>, <ul>, <ol>, and <blockquote> are allowed when they genuinely improve the layout. Never return a full HTML document, CSS, or JavaScript. When offering choices or clarification options, always use markdown task lists (- [ ] Option) so the UI can present clickable replies.",
  disableSslVerification: false,
  elasticsearchUrl: "http://localhost:9200",
  elasticsearchIndex: "rag_documents",
  elasticsearchUsername: "",
  elasticsearchPassword: "",
  embeddingBaseUrl: "http://localhost:11434/v1",
  embeddingApiKey: "",
  embeddingModel: "nomic-embed-text",
  embeddingVerifySsl: true,
  chunkSize: 512,
  chunkOverlap: 50,
  knnNeighbors: 50,
  mcpTools: [
    { id: 'mcp_1', label: 'MCP Tool 1', url: '' },
    { id: 'mcp_2', label: 'MCP Tool 2', url: '' },
  ],
  documentationUrl: '',
  portalApps: [],
  settingsAccessPassword: 'MM@2026',
  clickhouseHost: 'localhost',
  clickhousePort: 8123,
  clickhouseDatabase: 'default',
  clickhouseUsername: 'default',
  clickhousePassword: '',
  clickhouseSecure: false,
  clickhouseVerifySsl: true,
  clickhouseHttpPath: '',
  clickhouseQueryLimit: 200,
  oracleConnections: [
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
    connectionId: 'oracle_default',
    rowLimit: 1000,
    maxRetries: 3,
    maxIterations: 8,
    toolkitId: '',
    systemPrompt:
      'You are the Oracle SQL agent. Reply in English. Use the Oracle tools before making assumptions, generate optimized Oracle SQL with explicit columns, and present final user-facing answers in polished Markdown with clear sections, concise bullets, and tasteful emphasis. Safe semantic HTML fragments are allowed when they improve readability.',
  },
  fileManagerConfig: {
    basePath: '',
    maxIterations: 10,
    systemPrompt:
      'You are the File Management agent. Reply in English by default. Use filesystem tools instead of guessing, keep answers short and factual, ask for confirmation before destructive or overwrite actions, and present final user-facing answers in polished Markdown with concise structure and tasteful emphasis.',
  },
};

export const DEFAULT_PREFERENCES: AppPreferences = {
  darkMode: false,
  currentConversationId: null,
  workflow: 'LLM',
  agentRole: 'manager',
  selectedMcpToolId: '',
  page: 'landing',
};

export const DEFAULT_PERSISTED_STATE: PersistedAppState = {
  config: DEFAULT_CONFIG,
  conversations: [],
  preferences: DEFAULT_PREFERENCES,
};

export const CHAT_MEMORY_MIN_STEPS = 5;
export const CHAT_MEMORY_MAX_STEPS = 10;

export function buildConversationMemory(messages: Message[], keptSteps = CHAT_MEMORY_MAX_STEPS): ConversationMemory {
  const safeKeptSteps = Math.max(CHAT_MEMORY_MIN_STEPS, keptSteps);
  const steps = messages
    .filter((message): message is Message & { role: 'user' | 'assistant' } => (
      message.role === 'user' || message.role === 'assistant'
    ))
    .slice(-safeKeptSteps)
    .map((message) => ({
      role: message.role,
      content: message.content,
      timestamp: message.timestamp,
    }));

  return {
    steps,
    keptSteps: safeKeptSteps,
    updatedAt: Date.now(),
  };
}

export function createEmptyCrewPlanDraft(timezone = 'UTC'): CrewPlanDraft {
  return {
    name: '',
    prompt: '',
    agents: [],
    status: 'active',
    trigger: {
      kind: 'daily',
      timezone,
      oneTimeAt: '',
      timeOfDay: '09:00',
      weekdays: ['mon'],
      intervalMinutes: 60,
      pollMinutes: 5,
      watchSql: '',
      watchMode: 'result_changes',
      directory: '',
      pattern: '*',
      recursive: false,
    },
  };
}

export function normalizeCrewPlanDraft(
  draft?: Partial<CrewPlanDraft> | null,
  timezone = 'UTC'
): CrewPlanDraft {
  const base = createEmptyCrewPlanDraft(timezone);
  const incomingTrigger: Partial<CrewPlanTrigger> = draft?.trigger ?? {};
  return {
    ...base,
    ...(draft ?? {}),
    agents: Array.isArray(draft?.agents) ? draft!.agents.filter(isValidAgentRole) : [],
    trigger: {
      ...base.trigger,
      ...incomingTrigger,
      weekdays: Array.isArray(incomingTrigger.weekdays)
        ? incomingTrigger.weekdays.filter(Boolean) as PlanningWeekday[]
        : base.trigger.weekdays,
    },
  };
}

export function normalizePlanningAgentState(
  state?: Partial<PlanningAgentState> | null,
  timezone = 'UTC'
): PlanningAgentState {
  const draft = (state as any)?.draft ?? (state as any)?.draft;
  const missingFields = (state as any)?.missingFields ?? (state as any)?.missing_fields;
  const lastQuestion = (state as any)?.lastQuestion ?? (state as any)?.last_question;
  const readyToReview = (state as any)?.readyToReview ?? (state as any)?.ready_to_review;
  return {
    draft: normalizeCrewPlanDraft(draft, timezone),
    missingFields: Array.isArray(missingFields) ? missingFields.filter(Boolean) : [],
    lastQuestion: lastQuestion || '',
    readyToReview: Boolean(readyToReview),
  };
}

export function normalizePlanningBackendState(
  state?: Partial<PlanningBackendState> | null,
  timezone = 'UTC'
): PlanningBackendState {
  const plans = Array.isArray(state?.plans) ? state!.plans : [];
  const runs = Array.isArray(state?.runs) ? state!.runs : [];

  return {
    plans: plans.map((plan) => ({
      ...normalizeCrewPlanDraft(plan, timezone),
      id: plan.id || `plan_${Date.now()}`,
      createdAt: plan.createdAt || '',
      updatedAt: plan.updatedAt || '',
      nextRunAt: plan.nextRunAt ?? null,
      lastRunAt: plan.lastRunAt ?? null,
      lastStatus: plan.lastStatus ?? null,
      lastSummary: plan.lastSummary || '',
      runtime: plan.runtime ?? {},
    })),
    runs: runs.map((run) => ({
      id: run.id || `run_${Date.now()}`,
      planId: run.planId || '',
      planName: run.planName || 'Unnamed plan',
      triggerKind: run.triggerKind || 'manual',
      triggerLabel: run.triggerLabel || '',
      startedAt: run.startedAt || '',
      finishedAt: run.finishedAt ?? null,
      status: run.status || 'running',
      summary: run.summary || '',
      outputs: Array.isArray(run.outputs)
        ? run.outputs.filter((output): output is CrewPlanRunOutput => (
            Boolean(output)
            && isValidAgentRole((output as CrewPlanRunOutput).agent)
            && ((output as CrewPlanRunOutput).status === 'success' || (output as CrewPlanRunOutput).status === 'error')
          ))
        : [],
    })),
  };
}

export function normalizeFileManagerAgentState(
  state?: Partial<FileManagerAgentState> | null
): FileManagerAgentState {
  const pending = (state as any)?.pendingConfirmation ?? (state as any)?.pending_confirmation;
  return {
    pendingConfirmation: pending && typeof pending === 'object'
      ? {
          toolName: (pending as any).toolName ?? (pending as any).tool_name ?? '',
          toolInput: ((pending as any).toolInput ?? (pending as any).tool_input ?? {}) as Record<string, unknown>,
          preview: (pending as any).preview ?? '',
          summary: (pending as any).summary ?? '',
          requestedAt: (pending as any).requestedAt ?? (pending as any).requested_at ?? '',
        }
      : null,
    lastToolResult: (state as any)?.lastToolResult ?? (state as any)?.last_tool_result ?? '',
    lastVisitedPath: (state as any)?.lastVisitedPath ?? (state as any)?.last_visited_path ?? '',
  };
}

export function normalizePdfCreatorAgentState(
  state?: Partial<PdfCreatorAgentState> | null
): PdfCreatorAgentState {
  const pendingDocument = (state as any)?.pendingDocument ?? (state as any)?.pending_document;
  const pendingConfirmation = (state as any)?.pendingConfirmation ?? (state as any)?.pending_confirmation;
  return {
    stage:
      (state as any)?.stage === 'awaiting_source_choice' || (state as any)?.stage === 'awaiting_content'
        ? (state as any).stage
        : 'idle',
    pendingDocument: pendingDocument && typeof pendingDocument === 'object'
      ? (pendingDocument as Record<string, unknown>)
      : null,
    pendingConfirmation: pendingConfirmation && typeof pendingConfirmation === 'object'
      ? {
          preview: (pendingConfirmation as any).preview ?? '',
          summary: (pendingConfirmation as any).summary ?? '',
          requestedAt: (pendingConfirmation as any).requestedAt ?? (pendingConfirmation as any).requested_at ?? '',
        }
      : null,
    lastOutputPath: (state as any)?.lastOutputPath ?? (state as any)?.last_output_path ?? '',
    lastTitle: (state as any)?.lastTitle ?? (state as any)?.last_title ?? '',
  };
}

export function normalizeOracleAnalystAgentState(
  state?: Partial<OracleAnalystAgentState> | null
): OracleAnalystAgentState {
  const schemaInfo = (state as any)?.schemaInfo ?? (state as any)?.schema_info;
  const lastResultMeta = (state as any)?.lastResultMeta ?? (state as any)?.last_result_meta;
  const lastResultRows = (state as any)?.lastResultRows ?? (state as any)?.last_result_rows;
  const actionLog = (state as any)?.actionLog ?? (state as any)?.action_log;
  return {
    stage:
      (state as any)?.stage === 'awaiting_table' || (state as any)?.stage === 'ready'
        ? (state as any).stage
        : 'idle',
    pendingRequest: String((state as any)?.pendingRequest ?? (state as any)?.pending_request ?? ''),
    availableTables: Array.isArray((state as any)?.availableTables ?? (state as any)?.available_tables)
      ? ((state as any)?.availableTables ?? (state as any)?.available_tables).filter(Boolean)
      : [],
    selectedTable: (state as any)?.selectedTable ?? (state as any)?.selected_table ?? null,
    schemaInfo: Array.isArray(schemaInfo)
      ? schemaInfo
          .filter(Boolean)
          .map((column: any) => ({
            name: String(column?.name ?? ''),
            type: String(column?.type ?? ''),
            nullable: Boolean(column?.nullable),
          }))
          .filter((column: { name: string }) => Boolean(column.name))
      : [],
    clarificationPrompt: String((state as any)?.clarificationPrompt ?? (state as any)?.clarification_prompt ?? ''),
    clarificationOptions: Array.isArray((state as any)?.clarificationOptions ?? (state as any)?.clarification_options)
      ? ((state as any)?.clarificationOptions ?? (state as any)?.clarification_options).filter(Boolean)
      : [],
    lastSql: String((state as any)?.lastSql ?? (state as any)?.last_sql ?? ''),
    lastResultMeta: Array.isArray(lastResultMeta)
      ? lastResultMeta
          .filter(Boolean)
          .map((column: any) => ({
            name: String(column?.name ?? ''),
            type: String(column?.type ?? ''),
          }))
          .filter((column: { name: string }) => Boolean(column.name))
      : [],
    lastResultRows: Array.isArray(lastResultRows) ? lastResultRows.filter((row: unknown) => Boolean(row)) as Record<string, unknown>[] : [],
    finalAnswer: String((state as any)?.finalAnswer ?? (state as any)?.final_answer ?? ''),
    actionLog: Array.isArray(actionLog) ? actionLog.filter(Boolean).map(String) : [],
    lastError: String((state as any)?.lastError ?? (state as any)?.last_error ?? ''),
  };
}

export function normalizeDataAnalystAgentState(
  state?: Partial<DataAnalystAgentState> | null
): DataAnalystAgentState {
  const tableSchema = (state as any)?.tableSchema ?? (state as any)?.table_schema ?? (state as any)?.schema;
  const lastResultMeta = (state as any)?.lastResultMeta ?? (state as any)?.last_result_meta;
  const lastResultRows = (state as any)?.lastResultRows ?? (state as any)?.last_result_rows;
  const knowledgeHits = (state as any)?.knowledgeHits ?? (state as any)?.knowledge_hits;
  return {
    stage:
      (state as any)?.stage === 'awaiting_table' || (state as any)?.stage === 'ready'
        ? (state as any).stage
        : 'idle',
    pendingRequest: String((state as any)?.pendingRequest ?? (state as any)?.pending_request ?? ''),
    availableTables: Array.isArray((state as any)?.availableTables ?? (state as any)?.available_tables)
      ? ((state as any)?.availableTables ?? (state as any)?.available_tables).filter(Boolean)
      : [],
    selectedTable: (state as any)?.selectedTable ?? (state as any)?.selected_table ?? null,
    tableSchema: Array.isArray(tableSchema)
      ? tableSchema
          .filter(Boolean)
          .map((column: any) => ({
            name: String(column?.name ?? ''),
            type: String(column?.type ?? ''),
            defaultKind: String(column?.defaultKind ?? column?.default_kind ?? ''),
            defaultExpression: String(column?.defaultExpression ?? column?.default_expression ?? ''),
          }))
          .filter((column: { name: string }) => Boolean(column.name))
      : [],
    clarificationPrompt: String((state as any)?.clarificationPrompt ?? (state as any)?.clarification_prompt ?? ''),
    clarificationOptions: Array.isArray((state as any)?.clarificationOptions ?? (state as any)?.clarification_options)
      ? ((state as any)?.clarificationOptions ?? (state as any)?.clarification_options).filter(Boolean)
      : [],
    lastSqls: Array.isArray((state as any)?.lastSqls ?? (state as any)?.last_sqls)
      ? ((state as any)?.lastSqls ?? (state as any)?.last_sqls).filter(Boolean).map(String)
      : [],
    lastResultMeta: Array.isArray(lastResultMeta)
      ? lastResultMeta
          .filter(Boolean)
          .map((column: any) => ({
            name: String(column?.name ?? ''),
            type: String(column?.type ?? ''),
          }))
          .filter((column: { name: string }) => Boolean(column.name))
      : [],
    lastResultRows: Array.isArray(lastResultRows) ? lastResultRows.filter((row: unknown) => Boolean(row)) as Record<string, unknown>[] : [],
    finalAnswer: String((state as any)?.finalAnswer ?? (state as any)?.final_answer ?? ''),
    lastError: String((state as any)?.lastError ?? (state as any)?.last_error ?? ''),
    lastExportPath: String((state as any)?.lastExportPath ?? (state as any)?.last_export_path ?? ''),
    knowledgeHits: Array.isArray(knowledgeHits)
      ? knowledgeHits
          .filter(Boolean)
          .map((item: any) => ({
            docName: String(item?.docName ?? item?.doc_name ?? ''),
            text: String(item?.text ?? ''),
            score: Number(item?.score ?? 0),
          }))
      : [],
  };
}

export function normalizeManagerAgentState(
  state?: Partial<ManagerAgentState> | null
): ManagerAgentState {
  const activeDelegate = (state as any)?.activeDelegate ?? (state as any)?.active_delegate;
  const pendingPipeline = (state as any)?.pendingPipeline ?? (state as any)?.pending_pipeline;
  const exportFormat = pendingPipeline?.exportFormat ?? pendingPipeline?.export_format ?? null;
  const normalizedFormat = exportFormat === 'csv' || exportFormat === 'tsv' || exportFormat === 'xlsx'
    ? exportFormat
    : null;
  const stage = pendingPipeline?.stage;
  return {
    activeDelegate: activeDelegate === 'clickhouse_query' || activeDelegate === 'data_analyst' || activeDelegate === 'file_management' || activeDelegate === 'data_quality_tables' || activeDelegate === 'pdf_creator' || activeDelegate === 'oracle_analyst'
      ? activeDelegate
      : null,
    lastRoutingReason: (state as any)?.lastRoutingReason ?? (state as any)?.last_routing_reason ?? '',
    lastDelegateLabel: (state as any)?.lastDelegateLabel ?? (state as any)?.last_delegate_label ?? '',
    pendingPipeline: pendingPipeline && pendingPipeline.kind === 'clickhouse_to_file' && pendingPipeline.nextDelegate === 'file_management'
      ? {
          kind: 'clickhouse_to_file',
          stage: stage === 'awaiting_export_details' ? 'awaiting_export_details' : 'awaiting_clickhouse',
          nextDelegate: 'file_management',
          exportFormat: normalizedFormat,
          targetPath: String(pendingPipeline?.targetPath ?? pendingPipeline?.target_path ?? ''),
          sourceRequest: String(pendingPipeline?.sourceRequest ?? pendingPipeline?.source_request ?? ''),
          reason: String(pendingPipeline?.reason ?? ''),
        }
      : pendingPipeline && pendingPipeline.kind === 'clickhouse_to_pdf' && pendingPipeline.nextDelegate === 'pdf_creator'
        ? {
            kind: 'clickhouse_to_pdf',
            stage: 'awaiting_clickhouse',
            nextDelegate: 'pdf_creator',
            targetPath: String(pendingPipeline?.targetPath ?? pendingPipeline?.target_path ?? ''),
            sourceRequest: String(pendingPipeline?.sourceRequest ?? pendingPipeline?.source_request ?? ''),
            reason: String(pendingPipeline?.reason ?? ''),
            title: String(pendingPipeline?.title ?? ''),
          }
      : null,
  };
}

export function normalizeDataQualityState(
  state?: Partial<DataQualityState> | null
): DataQualityState {
  const schemaInfoRaw = (state as any)?.schemaInfo ?? (state as any)?.schema_info;
  const rawSampleSize = (state as any)?.sampleSize ?? (state as any)?.sample_size;
  return {
    stage: (state as any)?.stage ?? 'idle',
    table: (state as any)?.table ?? null,
    columns: Array.isArray((state as any)?.columns) ? (state as any).columns.filter(Boolean) : [],
    sampleSize: rawSampleSize === 0 ? 0 : (Number(rawSampleSize ?? 50000) || 50000),
    rowFilter: (state as any)?.rowFilter ?? (state as any)?.row_filter ?? '',
    timeColumn: (state as any)?.timeColumn ?? (state as any)?.time_column ?? null,
    dbType: (state as any)?.dbType === 'oracle' || (state as any)?.db_type === 'oracle' ? 'oracle' : 'clickhouse',
    schemaInfo: Array.isArray(schemaInfoRaw)
      ? schemaInfoRaw
          .filter(Boolean)
          .map((column: any) => ({
            name: column?.name ?? '',
            type: column?.type ?? '',
            category: column?.category === 'numeric' || column?.category === 'string' || column?.category === 'date' || column?.category === 'other'
              ? column.category
              : 'other',
          }))
          .filter((column: DataQualitySchemaColumn) => Boolean(column.name))
      : [],
    columnStats: ((state as any)?.columnStats ?? (state as any)?.column_stats ?? {}) as Record<string, unknown>,
    volumetricStats: ((state as any)?.volumetricStats ?? (state as any)?.volumetric_stats ?? null) as Record<string, unknown> | null,
    llmAnalysis: (state as any)?.llmAnalysis ?? (state as any)?.llm_analysis ?? '',
    finalAnswer: (state as any)?.finalAnswer ?? (state as any)?.final_answer ?? '',
    agentId: (state as any)?.agentId ?? (state as any)?.agent_id ?? 'data_quality_tables',
    sessionId: (state as any)?.sessionId ?? (state as any)?.session_id ?? '',
    lastError: (state as any)?.lastError ?? (state as any)?.last_error ?? '',
    availableTables: Array.isArray((state as any)?.availableTables ?? (state as any)?.available_tables)
      ? ((state as any)?.availableTables ?? (state as any)?.available_tables).filter(Boolean)
      : [],
    availableColumns: Array.isArray((state as any)?.availableColumns ?? (state as any)?.available_columns)
      ? ((state as any)?.availableColumns ?? (state as any)?.available_columns).filter(Boolean)
      : [],
    dateColumns: Array.isArray((state as any)?.dateColumns ?? (state as any)?.date_columns)
      ? ((state as any)?.dateColumns ?? (state as any)?.date_columns).filter(Boolean)
      : [],
  };
}

export function normalizeAppConfig(config?: Partial<AppConfig> | null): AppConfig {
  const incomingFileManager = config?.fileManagerConfig;
  const incomingOracleAnalyst = config?.oracleAnalystConfig;
  const incomingOracleConnections = Array.isArray(config?.oracleConnections) ? config!.oracleConnections : [];
  const incomingPortalApps = Array.isArray(config?.portalApps) ? config!.portalApps : [];
  const normalizedOracleConnections = incomingOracleConnections.length > 0
    ? incomingOracleConnections.map((connection, index) => ({
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
    : DEFAULT_CONFIG.oracleConnections.map((connection) => ({ ...connection }));
  const hasOracleConnection = normalizedOracleConnections.some((connection) => connection.id === (incomingOracleAnalyst?.connectionId || DEFAULT_CONFIG.oracleAnalystConfig.connectionId));
  return {
    ...DEFAULT_CONFIG,
    ...(config ?? {}),
    disableSslVerification: config?.disableSslVerification ?? DEFAULT_CONFIG.disableSslVerification,
    settingsAccessPassword: config?.settingsAccessPassword || DEFAULT_CONFIG.settingsAccessPassword,
    portalApps: incomingPortalApps
      .filter((app): app is PortalApp => Boolean(app))
      .map((app, index) => ({
        id: app.id || `portal_app_${index + 1}`,
        name: app.name || '',
        url: app.url || '',
        description: app.description || '',
      })),
    oracleConnections: normalizedOracleConnections,
    oracleAnalystConfig: {
      ...DEFAULT_CONFIG.oracleAnalystConfig,
      ...(incomingOracleAnalyst ?? {}),
      connectionId: hasOracleConnection
        ? (incomingOracleAnalyst?.connectionId || DEFAULT_CONFIG.oracleAnalystConfig.connectionId)
        : (normalizedOracleConnections[0]?.id ?? DEFAULT_CONFIG.oracleAnalystConfig.connectionId),
      rowLimit: Math.min(50000, Math.max(1, Number(incomingOracleAnalyst?.rowLimit ?? DEFAULT_CONFIG.oracleAnalystConfig.rowLimit) || DEFAULT_CONFIG.oracleAnalystConfig.rowLimit)),
      maxRetries: Math.min(10, Math.max(1, Number(incomingOracleAnalyst?.maxRetries ?? DEFAULT_CONFIG.oracleAnalystConfig.maxRetries) || DEFAULT_CONFIG.oracleAnalystConfig.maxRetries)),
      maxIterations: Math.min(20, Math.max(1, Number(incomingOracleAnalyst?.maxIterations ?? DEFAULT_CONFIG.oracleAnalystConfig.maxIterations) || DEFAULT_CONFIG.oracleAnalystConfig.maxIterations)),
      toolkitId: incomingOracleAnalyst?.toolkitId ?? DEFAULT_CONFIG.oracleAnalystConfig.toolkitId,
      systemPrompt: incomingOracleAnalyst?.systemPrompt ?? DEFAULT_CONFIG.oracleAnalystConfig.systemPrompt,
    },
    fileManagerConfig: {
      ...DEFAULT_CONFIG.fileManagerConfig,
      ...(incomingFileManager ?? {}),
      basePath: incomingFileManager?.basePath ?? DEFAULT_CONFIG.fileManagerConfig.basePath,
      maxIterations: Math.min(15, Math.max(1, incomingFileManager?.maxIterations ?? DEFAULT_CONFIG.fileManagerConfig.maxIterations)),
      systemPrompt: incomingFileManager?.systemPrompt ?? DEFAULT_CONFIG.fileManagerConfig.systemPrompt,
    },
    mcpTools: Array.isArray(config?.mcpTools)
      ? config!.mcpTools.map((tool) => ({
          id: tool.id || `mcp_${Date.now()}`,
          label: tool.label || 'New Tool',
          url: tool.url || '',
        }))
      : DEFAULT_CONFIG.mcpTools.map((tool) => ({ ...tool })),
  };
}

export function normalizeAppPreferences(preferences?: Partial<AppPreferences> | null): AppPreferences {
  const nextAgentRole =
    preferences?.agentRole === 'clickhouse_query' || preferences?.agentRole === 'data_analyst' || preferences?.agentRole === 'file_management' || preferences?.agentRole === 'pdf_creator' || preferences?.agentRole === 'oracle_analyst' || preferences?.agentRole === 'data_quality_tables'
      ? preferences.agentRole
      : 'manager';
  return {
    ...DEFAULT_PREFERENCES,
    ...(preferences ?? {}),
    agentRole: nextAgentRole,
  };
}

export function normalizePersistedAppState(
  state?: Partial<PersistedAppState> | null
): PersistedAppState {
  const config = normalizeAppConfig(state?.config);
  const conversations = Array.isArray(state?.conversations) ? state!.conversations : [];
  const preferences = normalizeAppPreferences(state?.preferences);
  const wantsFreshChat = preferences.currentConversationId === null;
  const hasCurrentConversation = conversations.some((conversation) => conversation.id === preferences.currentConversationId);
  const selectedMcpToolId = config.mcpTools.some((tool) => tool.id === preferences.selectedMcpToolId)
    ? preferences.selectedMcpToolId
    : (config.mcpTools[0]?.id ?? '');

  return {
    config,
    conversations,
    preferences: {
      ...preferences,
      currentConversationId: wantsFreshChat
        ? null
        : hasCurrentConversation
        ? preferences.currentConversationId
        : (conversations[0]?.id ?? null),
      selectedMcpToolId,
    },
    updatedAt: state?.updatedAt,
  };
}

export function hasMeaningfulPersistedAppState(state: PersistedAppState): boolean {
  if (state.conversations.length > 0) return true;
  if (JSON.stringify(state.config) !== JSON.stringify(DEFAULT_CONFIG)) return true;
  return JSON.stringify(state.preferences) !== JSON.stringify(DEFAULT_PREFERENCES);
}

/**
 * Preprocesses markdown text before it is rendered by ReactMarkdown.
 * This function fixes common formatting issues, such as inline task lists
 * generated by some LLMs that don't include proper line breaks.
 */
export function preprocessMarkdown(md: string): string {
  if (!md) return '';
  let text = md;
  // Fix inline task list items generated by some LLMs
  text = text.replace(/([^\n])\s+([-*]\s+\[[ xX]\]\s+)/g, '$1\n$2');
  // Fix inline normal list items (only if preceded by punctuation to avoid matching "A - B")
  text = text.replace(/([.!?])\s+([-*]\s+[A-Z0-9])/g, '$1\n$2');
  // Fix inline numbered list items
  text = text.replace(/([.!?])\s+(\d+\.\s+[A-Z0-9])/g, '$1\n$2');
  text = promoteChoiceListsToTaskLists(text);
  return text;
}

const CHOICE_CONTEXT_PATTERN =
  /(?:choose|pick|select|which(?:\s+one|\s+option|\s+table|\s+field|\s+date|\s+format)?|would you like|do you want|can you confirm|confirm|prefer|option(?:s)?|one of the following|clarif(?:y|ication)|specif(?:y|ication)|tell me which|choose from|pick one|reply with|answer with|yes or no|launch analysis|edit table|edit columns|edit sample size|edit row filter|edit time column|start over|choisis|selectionne|sélectionne|lequel|laquelle|quelle option|précis(?:e|ion)|réponds avec|confirme)/i;

function promoteChoiceListsToTaskLists(md: string): string {
  const lines = md.split('\n');
  let inCodeFence = false;

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    if (/^\s*```/.test(line)) {
      inCodeFence = !inCodeFence;
      continue;
    }
    if (inCodeFence) continue;
    if (!/^\s*(?:[-*+]|\d+\.)\s+/.test(line)) continue;
    if (/^\s*[-*+]\s+\[[ xX]\]\s+/.test(line)) continue;

    const blockItems: string[] = [];
    let cursor = index;
    let containsTaskList = false;

    while (cursor < lines.length) {
      const current = lines[cursor];
      if (/^\s*$/.test(current) || /^\s*```/.test(current)) break;
      if (/^\s*[-*+]\s+\[[ xX]\]\s+/.test(current)) {
        containsTaskList = true;
        break;
      }
      const match = current.match(/^\s*(?:[-*+]|\d+\.)\s+(.+?)\s*$/);
      if (!match) break;
      blockItems.push(match[1].trim());
      cursor += 1;
    }

    if (containsTaskList || blockItems.length < 2 || blockItems.length > 8) {
      index = Math.max(index, cursor - 1);
      continue;
    }

    if (blockItems.some((item) => item.length > 120 || item.startsWith('|') || item.startsWith('`'))) {
      index = Math.max(index, cursor - 1);
      continue;
    }

    const contextLines: string[] = [];
    let lookback = index - 1;
    while (lookback >= 0 && contextLines.length < 4) {
      const previous = lines[lookback].trim();
      if (!previous) break;
      if (/^\s*```/.test(previous)) break;
      contextLines.unshift(previous);
      lookback -= 1;
    }

    if (!CHOICE_CONTEXT_PATTERN.test(contextLines.join(' '))) {
      index = Math.max(index, cursor - 1);
      continue;
    }

    const replacement = blockItems.map((item) => `- [ ] ${item}`);
    lines.splice(index, blockItems.length, ...replacement);
    index += replacement.length - 1;
  }

  return lines.join('\n');
}

/**
 * Legacy function to parse markdown to HTML.
 * Note: This is largely superseded by ReactMarkdown in the ChatMessage component,
 * but may still be used in older parts of the codebase if any remain.
 */
export function parseMarkdownToHTML(md: string): string {
  if (!md) return '';
  
  let html = md;
  
  // Fix inline task list items generated by some LLMs
  html = html.replace(/([^\n])\s+([-*]\s+\[[ xX]\]\s+)/g, '$1\n$2');
  // Fix inline normal list items (only if preceded by punctuation to avoid matching "A - B")
  html = html.replace(/([.!?])\s+([-*]\s+[A-Z0-9])/g, '$1\n$2');
  // Fix inline numbered list items
  html = html.replace(/([.!?])\s+(\d+\.\s+[A-Z0-9])/g, '$1\n$2');

  // Escape HTML to prevent XSS, but allow <u> and <ins>
  html = html.replace(/</g, '&lt;').replace(/>/g, '&gt;');
  html = html.replace(/&lt;u&gt;(.*?)&lt;\/u&gt;/g, '<u>$1</u>');
  html = html.replace(/&lt;ins&gt;(.*?)&lt;\/ins&gt;/g, '<ins>$1</ins>');

  // Code blocks
  html = html.replace(/```[\s\S]*?\n([\s\S]*?)```/g, '<pre><code>$1</code></pre>');
  
  // Inline code
  html = html.replace(/`([^`]+)`/g, '<code>$1</code>');

  // Bold & Italic
  html = html.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
  html = html.replace(/\*(.*?)\*/g, '<em>$1</em>');

  // Links
  html = html.replace(/\[(.*?)\]\((.*?)\)/g, '<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>');

  // Process blocks
  const blocks = html.split(/\n\n+/);
  const processedBlocks = blocks.map(block => {
    if (block.match(/^### (.*$)/im)) return block.replace(/^### (.*$)/gim, '<h3>$1</h3>');
    if (block.match(/^## (.*$)/im)) return block.replace(/^## (.*$)/gim, '<h2>$1</h2>');
    if (block.match(/^# (.*$)/im)) return block.replace(/^# (.*$)/gim, '<h1>$1</h1>');
    if (block.match(/^\> /im)) return block.replace(/^\> (.*$)/gim, '<blockquote>$1</blockquote>');

    if (block.match(/^([-*]|\d+\.)\s+/im)) {
      const lines = block.split('\n');
      let resultHtml = '';
      let listType = ''; // 'ul' or 'ol'

      for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        if (!line.trim()) continue;

        const isTaskEmpty = line.match(/^[-*]\s+\[ \]\s+(.*$)/);
        const isTaskChecked = line.match(/^[-*]\s+\[[xX]\]\s+(.*$)/);
        const isNormalList = line.match(/^[-*]\s+(.*$)/);
        const isNumberedList = line.match(/^(\d+)\.\s+(.*$)/);

        if (isTaskEmpty || isTaskChecked || isNormalList || isNumberedList) {
          const currentType = isNumberedList ? 'ol' : 'ul';
          if (listType !== currentType) {
            if (listType) resultHtml += `</${listType}>`;
            resultHtml += `<${currentType}>`;
            listType = currentType;
          }
          
          if (isTaskEmpty) {
            resultHtml += `<li class="task-list-item" data-task="${isTaskEmpty[1]}"><input type="checkbox" /> <span>${isTaskEmpty[1]}</span></li>`;
          } else if (isTaskChecked) {
            resultHtml += `<li class="task-list-item" data-task="${isTaskChecked[1]}"><input type="checkbox" checked /> <span>${isTaskChecked[1]}</span></li>`;
          } else if (isNormalList) {
            resultHtml += `<li>${isNormalList[1]}</li>`;
          } else if (isNumberedList) {
            resultHtml += `<li>${isNumberedList[2]}</li>`;
          }
        } else {
          if (listType) {
            resultHtml += `</${listType}>`;
            listType = '';
          }
          resultHtml += `<p>${line}</p>`;
        }
      }
      if (listType) {
        resultHtml += `</${listType}>`;
      }
      return resultHtml;
    }

    if (block.match(/^\|.*\|$/m)) {
      const lines = block.split('\n').filter(line => line.trim().startsWith('|'));
      if (lines.length >= 2) {
        let tableHtml = '<div class="overflow-x-auto my-4"><table class="min-w-full divide-y divide-gray-200 border border-gray-200 rounded-lg shadow-sm">';
        
        // Parse header
        const headerCells = lines[0].split('|').slice(1, -1).map(c => c.trim());
        tableHtml += '<thead class="bg-gray-50/80"><tr>';
        headerCells.forEach(cell => {
          tableHtml += `<th class="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider border-b border-gray-200">${cell}</th>`;
        });
        tableHtml += '</tr></thead>';

        // Parse body
        tableHtml += '<tbody class="bg-white divide-y divide-gray-100">';
        for (let i = 2; i < lines.length; i++) {
          const cells = lines[i].split('|').slice(1, -1).map(c => c.trim());
          tableHtml += '<tr class="hover:bg-gray-50/50 transition-colors">';
          cells.forEach(cell => {
            tableHtml += `<td class="px-4 py-3 text-sm text-gray-700">${cell}</td>`;
          });
          tableHtml += '</tr>';
        }
        tableHtml += '</tbody></table></div>';
        return tableHtml;
      }
    }

    if (block.startsWith('<pre>')) return block;

    return `<p>${block.replace(/\n/g, '<br/>')}</p>`;
  });

  return processedBlocks.join('');
}
