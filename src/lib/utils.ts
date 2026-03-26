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
    | 'export_data_quality_pdf'
    | 'run_mcp_preset';
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

export type PlanningExportPostAction = {
  enabled: boolean;
  format: 'csv' | 'tsv' | 'xlsx';
  path: string;
};

export type PlanningEmailPostAction = {
  enabled: boolean;
  to: string[];
  cc: string[];
  bcc: string[];
  subject: string;
  bodyTemplate: string;
  attachExportedFile: boolean;
};

export type PlanningPostActions = {
  exportFile: PlanningExportPostAction;
  sendEmail: PlanningEmailPostAction;
};

export type CrewPlanDraft = {
  name: string;
  prompt: string;
  agents: AgentRole[];
  mcpToolIds: string[];
  useMcpOrchestrator: boolean;
  status: 'active' | 'paused';
  trigger: CrewPlanTrigger;
  postActions: PlanningPostActions;
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
  agent: string;
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

export type AutoMlAgentState = {
  stage: 'idle' | 'awaiting_table' | 'awaiting_target' | 'ready';
  pendingRequest: string;
  availableTables: string[];
  selectedTable: string | null;
  schemaInfo: Array<{ name: string; type: string }>;
  targetColumn: string | null;
  rowFilter: string;
  sampleRowLimit: number;
  featureColumns: string[];
  clarificationPrompt: string;
  clarificationOptions: string[];
  comparisonRows: Array<Record<string, unknown>>;
  problemType: 'classification' | 'regression' | '';
  recommendedModel: string;
  finalAnswer: string;
  lastError: string;
};

export type DataCleanerFinding = {
  level: 'critical' | 'warning' | 'info';
  title: string;
  detail: string;
};

export type DataCleanerScript = {
  title: string;
  sql: string;
};

export type DataCleanerAgentState = {
  stage: 'idle' | 'awaiting_table' | 'ready';
  pendingRequest: string;
  availableTables: string[];
  selectedTable: string | null;
  schemaInfo: Array<{ name: string; type: string }>;
  rowFilter: string;
  clarificationPrompt: string;
  clarificationOptions: string[];
  findings: DataCleanerFinding[];
  correctionScripts: DataCleanerScript[];
  finalAnswer: string;
  lastError: string;
};

export type AnonymizerFinding = {
  column: string;
  piiType: string;
  risk: 'high' | 'medium' | 'low';
  recommendation: string;
  evidence: string;
};

export type AnonymizerScript = {
  title: string;
  sql: string;
};

export type AnonymizerAgentState = {
  stage: 'idle' | 'awaiting_table' | 'ready';
  pendingRequest: string;
  availableTables: string[];
  selectedTable: string | null;
  schemaInfo: Array<{ name: string; type: string }>;
  rowFilter: string;
  clarificationPrompt: string;
  clarificationOptions: string[];
  piiFindings: AnonymizerFinding[];
  maskingScripts: AnonymizerScript[];
  finalAnswer: string;
  lastError: string;
};

export type EmailSenderPendingEmail = {
  to: string[];
  cc: string[];
  bcc: string[];
  subject: string;
  body: string;
  attachmentPaths: string[];
};

export type EmailSenderAgentState = {
  stage: 'idle' | 'awaiting_details' | 'ready';
  pendingRequest: string;
  pendingEmail: EmailSenderPendingEmail | null;
  clarificationPrompt: string;
  clarificationOptions: string[];
  lastRecipients: string[];
  lastSubject: string;
  lastAttachmentPaths: string[];
  finalAnswer: string;
  lastError: string;
};

export type CustomAgentRuntimeState = {
  selectedAgentId: string | null;
  finalAnswer: string;
  lastError: string;
};

export type ManagerDelegateRole = 'clickhouse_query' | 'data_analyst' | 'file_management' | 'pdf_creator' | 'oracle_analyst' | 'auto_ml' | 'data_cleaner' | 'anonymizer' | 'email_sender' | 'custom_agent';

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
  autoMl?: AutoMlAgentState;
  dataCleaner?: DataCleanerAgentState;
  anonymizer?: AnonymizerAgentState;
  emailSender?: EmailSenderAgentState;
  customAgent?: CustomAgentRuntimeState;
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

export type AgentRole = 'manager' | 'clickhouse_query' | 'data_analyst' | 'file_management' | 'pdf_creator' | 'oracle_analyst' | 'auto_ml' | 'data_cleaner' | 'anonymizer' | 'email_sender' | 'custom_agent';
export type BuiltInAgentRole = Exclude<AgentRole, 'custom_agent'>;

export type Page = 'landing' | 'chat' | 'dataviz' | 'agents' | 'admin';

const VALID_AGENT_ROLES: AgentRole[] = ['manager', 'clickhouse_query', 'data_analyst', 'file_management', 'pdf_creator', 'oracle_analyst', 'auto_ml', 'data_cleaner', 'anonymizer', 'email_sender', 'custom_agent'];
export const BUILT_IN_AGENT_ROLES: BuiltInAgentRole[] = ['manager', 'clickhouse_query', 'data_analyst', 'file_management', 'pdf_creator', 'oracle_analyst', 'auto_ml', 'data_cleaner', 'anonymizer', 'email_sender'];

function isValidAgentRole(value: unknown): value is AgentRole {
  return typeof value === 'string' && VALID_AGENT_ROLES.includes(value as AgentRole);
}

export type AgentVisibilityConfig = Record<BuiltInAgentRole, boolean>;

/**
 * Represents a single MCP tool entry configurable by the user.
 */
export type McpPresetQuestion = {
  id: string;
  label: string;
  prompt: string;
  preferredTool: string;
};

export type McpTool = {
  id: string;
  label: string;
  url: string;
  description: string;
  presetQuestions: McpPresetQuestion[];
};

export const MCP_ORCHESTRATOR_ID = '__mcp_orchestrator__';

export type PortalApp = {
  id: string;
  name: string;
  url: string;
  description: string;
};

export type CustomAgentConfig = {
  id: string;
  title: string;
  description: string;
  pythonCode: string;
  systemPrompt: string;
  managerRoutingHint: string;
  status: 'draft' | 'ready' | 'error';
  statusMessage: string;
  enabled: boolean;
  badgeColor: string;
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
  managerUseRagFunctionalContext: boolean;
  agentVisibility: AgentVisibilityConfig;
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
  agenticDataVizUrl: string;
  portalApps: PortalApp[];
  customAgents: CustomAgentConfig[];
  settingsAccessPassword: string;
  ssoConfig: SsoConfig;
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
  emailSenderConfig: EmailSenderAgentConfig;
};

export type FileManagerAgentConfig = {
  basePath: string;
  maxIterations: number;
  systemPrompt: string;
};

export type EmailSenderAgentConfig = {
  host: string;
  port: number;
  secure: boolean;
  startTls: boolean;
  username: string;
  password: string;
  fromEmail: string;
  fromName: string;
  replyTo: string;
  allowedRecipients: string[];
  systemPrompt: string;
};

export type SsoConfig = {
  enabled: boolean;
  providerType: 'oidc' | 'saml' | 'generic';
  providerLabel: string;
  issuerUrl: string;
  authorizationUrl: string;
  tokenUrl: string;
  userInfoUrl: string;
  jwksUrl: string;
  clientId: string;
  clientSecret: string;
  redirectUri: string;
  scopes: string;
  logoutUrl: string;
  allowedDomains: string[];
  roleClaim: string;
  emailClaim: string;
  nameClaim: string;
  allowAdminPasswordFallback: boolean;
};

export type AppPreferences = {
  darkMode: boolean;
  currentConversationId: string | null;
  workflow: WorkflowMode;
  agentRole: AgentRole;
  selectedMcpToolId: string;
  selectedCustomAgentId: string;
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
  systemPrompt: "You are a helpful, smart, and concise AI assistant. Present non-JSON answers in polished Markdown with clear sections, concise bullets, tasteful **bold** emphasis, and tables when they help. Safe semantic HTML fragments such as <section>, <article>, <details>, <summary>, <table>, <ul>, <ol>, and <blockquote> are allowed when they genuinely improve the layout. Never return a full HTML document, CSS, or JavaScript. When offering choices or clarification options, always use markdown task lists (- [ ] Option) so the UI can present clickable replies. If the user explicitly asks for a table, rows/columns, a matrix, a grid, a schema list, or a tabular preview, return the relevant structured result as a valid Markdown table whenever the data is naturally tabular.",
  managerUseRagFunctionalContext: false,
  agentVisibility: {
    manager: true,
    clickhouse_query: true,
    data_analyst: true,
    file_management: true,
    pdf_creator: true,
    oracle_analyst: true,
    auto_ml: true,
    data_cleaner: true,
    anonymizer: true,
    email_sender: true,
  },
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
    { id: 'mcp_1', label: 'MCP Tool 1', url: '', description: '', presetQuestions: [] },
    { id: 'mcp_2', label: 'MCP Tool 2', url: '', description: '', presetQuestions: [] },
  ],
  documentationUrl: '',
  agenticDataVizUrl: '',
  portalApps: [],
  customAgents: [],
  settingsAccessPassword: 'MM@2026',
  ssoConfig: {
    enabled: false,
    providerType: 'oidc',
    providerLabel: 'Corporate SSO',
    issuerUrl: '',
    authorizationUrl: '',
    tokenUrl: '',
    userInfoUrl: '',
    jwksUrl: '',
    clientId: '',
    clientSecret: '',
    redirectUri: '',
    scopes: 'openid profile email',
    logoutUrl: '',
    allowedDomains: [],
    roleClaim: 'roles',
    emailClaim: 'email',
    nameClaim: 'name',
    allowAdminPasswordFallback: true,
  },
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
      'You are the Oracle SQL agent. Reply in English. Use the Oracle tools before making assumptions, generate optimized Oracle SQL with explicit columns, and present final user-facing answers in polished Markdown with clear sections, concise bullets, and tasteful emphasis. Safe semantic HTML fragments are allowed when they improve readability. When the user explicitly asks for a table or schema list, include a readable Markdown table.',
  },
  fileManagerConfig: {
    basePath: '',
    maxIterations: 10,
    systemPrompt:
      'You are the File Management agent. Reply in English by default. Use filesystem tools instead of guessing, keep answers short and factual, ask for confirmation before destructive or overwrite actions, and present final user-facing answers in polished Markdown with concise structure and tasteful emphasis. When the user explicitly asks for tabular output, include a Markdown table whenever the result is naturally tabular.',
  },
  emailSenderConfig: {
    host: '',
    port: 587,
    secure: false,
    startTls: true,
    username: '',
    password: '',
    fromEmail: '',
    fromName: 'RAGnarok',
    replyTo: '',
    allowedRecipients: [],
    systemPrompt:
      'You are the Email Sender agent. Reply in English. Help the user prepare and send an email with text and optional file attachments. Ask only for the missing delivery details, never send to recipients outside the configured allowlist, and present final user-facing answers in polished Markdown with concise structure and tasteful emphasis.',
  },
};

export const DEFAULT_PREFERENCES: AppPreferences = {
  darkMode: false,
  currentConversationId: null,
  workflow: 'LLM',
  agentRole: 'manager',
  selectedMcpToolId: '',
  selectedCustomAgentId: '',
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
    mcpToolIds: [],
    useMcpOrchestrator: false,
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
    postActions: {
      exportFile: {
        enabled: false,
        format: 'csv',
        path: '',
      },
      sendEmail: {
        enabled: false,
        to: [],
        cc: [],
        bcc: [],
        subject: '',
        bodyTemplate:
          "Hello,\n\nHere is the latest MCP automation result for {plan_name}.\n\nTrigger: {trigger_label}\n\nSummary:\n{summary}\n\nDetailed outputs:\n{outputs_markdown}",
        attachExportedFile: false,
      },
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
    mcpToolIds: Array.isArray((draft as any)?.mcpToolIds)
      ? ((draft as any).mcpToolIds as unknown[]).filter((value): value is string => typeof value === 'string' && value.trim().length > 0)
      : [],
    useMcpOrchestrator: Boolean((draft as any)?.useMcpOrchestrator),
    trigger: {
      ...base.trigger,
      ...incomingTrigger,
      weekdays: Array.isArray(incomingTrigger.weekdays)
        ? incomingTrigger.weekdays.filter(Boolean) as PlanningWeekday[]
        : base.trigger.weekdays,
    },
    postActions: {
      exportFile: {
        ...base.postActions.exportFile,
        ...(((draft as any)?.postActions?.exportFile ?? {}) as Partial<PlanningExportPostAction>),
        format:
          ((draft as any)?.postActions?.exportFile?.format === 'tsv' || (draft as any)?.postActions?.exportFile?.format === 'xlsx')
            ? (draft as any).postActions.exportFile.format
            : 'csv',
        path: String((draft as any)?.postActions?.exportFile?.path ?? '').trim(),
        enabled: Boolean((draft as any)?.postActions?.exportFile?.enabled),
      },
      sendEmail: {
        ...base.postActions.sendEmail,
        ...(((draft as any)?.postActions?.sendEmail ?? {}) as Partial<PlanningEmailPostAction>),
        to: Array.isArray((draft as any)?.postActions?.sendEmail?.to)
          ? (draft as any).postActions.sendEmail.to.filter((value: unknown): value is string => typeof value === 'string' && value.trim().length > 0)
          : base.postActions.sendEmail.to,
        cc: Array.isArray((draft as any)?.postActions?.sendEmail?.cc)
          ? (draft as any).postActions.sendEmail.cc.filter((value: unknown): value is string => typeof value === 'string' && value.trim().length > 0)
          : base.postActions.sendEmail.cc,
        bcc: Array.isArray((draft as any)?.postActions?.sendEmail?.bcc)
          ? (draft as any).postActions.sendEmail.bcc.filter((value: unknown): value is string => typeof value === 'string' && value.trim().length > 0)
          : base.postActions.sendEmail.bcc,
        subject: String((draft as any)?.postActions?.sendEmail?.subject ?? '').trim(),
        bodyTemplate: String((draft as any)?.postActions?.sendEmail?.bodyTemplate ?? base.postActions.sendEmail.bodyTemplate),
        attachExportedFile: Boolean((draft as any)?.postActions?.sendEmail?.attachExportedFile),
        enabled: Boolean((draft as any)?.postActions?.sendEmail?.enabled),
      },
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
            && typeof (output as CrewPlanRunOutput).agent === 'string'
            && (output as CrewPlanRunOutput).agent.trim().length > 0
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

export function normalizeCustomAgentRuntimeState(
  state?: Partial<CustomAgentRuntimeState> | null
): CustomAgentRuntimeState {
  return {
    selectedAgentId: (state as any)?.selectedAgentId ?? (state as any)?.selected_agent_id ?? null,
    finalAnswer: String((state as any)?.finalAnswer ?? (state as any)?.final_answer ?? ''),
    lastError: String((state as any)?.lastError ?? (state as any)?.last_error ?? ''),
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
    activeDelegate: activeDelegate === 'clickhouse_query' || activeDelegate === 'data_analyst' || activeDelegate === 'auto_ml' || activeDelegate === 'file_management' || activeDelegate === 'pdf_creator' || activeDelegate === 'oracle_analyst' || activeDelegate === 'data_cleaner' || activeDelegate === 'anonymizer' || activeDelegate === 'email_sender'
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

export function normalizeAutoMlAgentState(
  state?: Partial<AutoMlAgentState> | null
): AutoMlAgentState {
  const schemaInfo = (state as any)?.schemaInfo ?? (state as any)?.schema_info;
  const comparisonRows = (state as any)?.comparisonRows ?? (state as any)?.comparison_rows;
  const rawProblemType = String((state as any)?.problemType ?? (state as any)?.problem_type ?? '');
  return {
    stage:
      (state as any)?.stage === 'awaiting_table' || (state as any)?.stage === 'awaiting_target' || (state as any)?.stage === 'ready'
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
          }))
          .filter((column: { name: string }) => Boolean(column.name))
      : [],
    targetColumn: (state as any)?.targetColumn ?? (state as any)?.target_column ?? null,
    rowFilter: String((state as any)?.rowFilter ?? (state as any)?.row_filter ?? ''),
    sampleRowLimit: Math.max(100, Math.min(10000, Number((state as any)?.sampleRowLimit ?? (state as any)?.sample_row_limit ?? 1000) || 1000)),
    featureColumns: Array.isArray((state as any)?.featureColumns ?? (state as any)?.feature_columns)
      ? ((state as any)?.featureColumns ?? (state as any)?.feature_columns).filter(Boolean).map(String)
      : [],
    clarificationPrompt: String((state as any)?.clarificationPrompt ?? (state as any)?.clarification_prompt ?? ''),
    clarificationOptions: Array.isArray((state as any)?.clarificationOptions ?? (state as any)?.clarification_options)
      ? ((state as any)?.clarificationOptions ?? (state as any)?.clarification_options).filter(Boolean)
      : [],
    comparisonRows: Array.isArray(comparisonRows)
      ? comparisonRows.filter((row: unknown) => Boolean(row)) as Record<string, unknown>[]
      : [],
    problemType: rawProblemType === 'classification' || rawProblemType === 'regression' ? rawProblemType : '',
    recommendedModel: String((state as any)?.recommendedModel ?? (state as any)?.recommended_model ?? ''),
    finalAnswer: String((state as any)?.finalAnswer ?? (state as any)?.final_answer ?? ''),
    lastError: String((state as any)?.lastError ?? (state as any)?.last_error ?? ''),
  };
}

export function normalizeDataCleanerAgentState(
  state?: Partial<DataCleanerAgentState> | null
): DataCleanerAgentState {
  const findings = (state as any)?.findings;
  const correctionScripts = (state as any)?.correctionScripts ?? (state as any)?.correction_scripts;
  return {
    stage:
      (state as any)?.stage === 'awaiting_table' || (state as any)?.stage === 'ready'
        ? (state as any).stage
        : 'idle',
    pendingRequest: String((state as any)?.pendingRequest ?? (state as any)?.pending_request ?? ''),
    availableTables: Array.isArray((state as any)?.availableTables ?? (state as any)?.available_tables)
      ? ((state as any)?.availableTables ?? (state as any)?.available_tables).filter(Boolean).map(String)
      : [],
    selectedTable: (state as any)?.selectedTable ?? (state as any)?.selected_table ?? null,
    schemaInfo: Array.isArray((state as any)?.schemaInfo ?? (state as any)?.schema_info)
      ? ((state as any)?.schemaInfo ?? (state as any)?.schema_info)
          .filter(Boolean)
          .map((column: any) => ({
            name: String(column?.name ?? ''),
            type: String(column?.type ?? ''),
          }))
          .filter((column: { name: string }) => Boolean(column.name))
      : [],
    rowFilter: String((state as any)?.rowFilter ?? (state as any)?.row_filter ?? ''),
    clarificationPrompt: String((state as any)?.clarificationPrompt ?? (state as any)?.clarification_prompt ?? ''),
    clarificationOptions: Array.isArray((state as any)?.clarificationOptions ?? (state as any)?.clarification_options)
      ? ((state as any)?.clarificationOptions ?? (state as any)?.clarification_options).filter(Boolean).map(String)
      : [],
    findings: Array.isArray(findings)
      ? findings
          .filter(Boolean)
          .map((item: any) => ({
            level: item?.level === 'critical' || item?.level === 'warning' ? item.level : 'info',
            title: String(item?.title ?? ''),
            detail: String(item?.detail ?? ''),
          }))
      : [],
    correctionScripts: Array.isArray(correctionScripts)
      ? correctionScripts
          .filter(Boolean)
          .map((item: any) => ({
            title: String(item?.title ?? ''),
            sql: String(item?.sql ?? ''),
          }))
      : [],
    finalAnswer: String((state as any)?.finalAnswer ?? (state as any)?.final_answer ?? ''),
    lastError: String((state as any)?.lastError ?? (state as any)?.last_error ?? ''),
  };
}

export function normalizeAnonymizerAgentState(
  state?: Partial<AnonymizerAgentState> | null
): AnonymizerAgentState {
  const piiFindings = (state as any)?.piiFindings ?? (state as any)?.pii_findings;
  const maskingScripts = (state as any)?.maskingScripts ?? (state as any)?.masking_scripts;
  return {
    stage:
      (state as any)?.stage === 'awaiting_table' || (state as any)?.stage === 'ready'
        ? (state as any).stage
        : 'idle',
    pendingRequest: String((state as any)?.pendingRequest ?? (state as any)?.pending_request ?? ''),
    availableTables: Array.isArray((state as any)?.availableTables ?? (state as any)?.available_tables)
      ? ((state as any)?.availableTables ?? (state as any)?.available_tables).filter(Boolean).map(String)
      : [],
    selectedTable: (state as any)?.selectedTable ?? (state as any)?.selected_table ?? null,
    schemaInfo: Array.isArray((state as any)?.schemaInfo ?? (state as any)?.schema_info)
      ? ((state as any)?.schemaInfo ?? (state as any)?.schema_info)
          .filter(Boolean)
          .map((column: any) => ({
            name: String(column?.name ?? ''),
            type: String(column?.type ?? ''),
          }))
          .filter((column: { name: string }) => Boolean(column.name))
      : [],
    rowFilter: String((state as any)?.rowFilter ?? (state as any)?.row_filter ?? ''),
    clarificationPrompt: String((state as any)?.clarificationPrompt ?? (state as any)?.clarification_prompt ?? ''),
    clarificationOptions: Array.isArray((state as any)?.clarificationOptions ?? (state as any)?.clarification_options)
      ? ((state as any)?.clarificationOptions ?? (state as any)?.clarification_options).filter(Boolean).map(String)
      : [],
    piiFindings: Array.isArray(piiFindings)
      ? piiFindings
          .filter(Boolean)
          .map((item: any) => ({
            column: String(item?.column ?? ''),
            piiType: String(item?.piiType ?? item?.pii_type ?? ''),
            risk: item?.risk === 'high' || item?.risk === 'low' ? item.risk : 'medium',
            recommendation: String(item?.recommendation ?? ''),
            evidence: String(item?.evidence ?? ''),
          }))
      : [],
    maskingScripts: Array.isArray(maskingScripts)
      ? maskingScripts
          .filter(Boolean)
          .map((item: any) => ({
            title: String(item?.title ?? ''),
            sql: String(item?.sql ?? ''),
          }))
      : [],
    finalAnswer: String((state as any)?.finalAnswer ?? (state as any)?.final_answer ?? ''),
    lastError: String((state as any)?.lastError ?? (state as any)?.last_error ?? ''),
  };
}

export function normalizeEmailSenderAgentState(
  state?: Partial<EmailSenderAgentState> | null
): EmailSenderAgentState {
  const pendingEmail = (state as any)?.pendingEmail ?? (state as any)?.pending_email;
  return {
    stage:
      (state as any)?.stage === 'awaiting_details' || (state as any)?.stage === 'ready'
        ? (state as any).stage
        : 'idle',
    pendingRequest: String((state as any)?.pendingRequest ?? (state as any)?.pending_request ?? ''),
    pendingEmail: pendingEmail && typeof pendingEmail === 'object'
      ? {
          to: Array.isArray((pendingEmail as any)?.to) ? (pendingEmail as any).to.filter(Boolean).map(String) : [],
          cc: Array.isArray((pendingEmail as any)?.cc) ? (pendingEmail as any).cc.filter(Boolean).map(String) : [],
          bcc: Array.isArray((pendingEmail as any)?.bcc) ? (pendingEmail as any).bcc.filter(Boolean).map(String) : [],
          subject: String((pendingEmail as any)?.subject ?? ''),
          body: String((pendingEmail as any)?.body ?? ''),
          attachmentPaths: Array.isArray((pendingEmail as any)?.attachmentPaths ?? (pendingEmail as any)?.attachment_paths)
            ? ((pendingEmail as any)?.attachmentPaths ?? (pendingEmail as any)?.attachment_paths).filter(Boolean).map(String)
            : [],
        }
      : null,
    clarificationPrompt: String((state as any)?.clarificationPrompt ?? (state as any)?.clarification_prompt ?? ''),
    clarificationOptions: Array.isArray((state as any)?.clarificationOptions ?? (state as any)?.clarification_options)
      ? ((state as any)?.clarificationOptions ?? (state as any)?.clarification_options).filter(Boolean).map(String)
      : [],
    lastRecipients: Array.isArray((state as any)?.lastRecipients ?? (state as any)?.last_recipients)
      ? ((state as any)?.lastRecipients ?? (state as any)?.last_recipients).filter(Boolean).map(String)
      : [],
    lastSubject: String((state as any)?.lastSubject ?? (state as any)?.last_subject ?? ''),
    lastAttachmentPaths: Array.isArray((state as any)?.lastAttachmentPaths ?? (state as any)?.last_attachment_paths)
      ? ((state as any)?.lastAttachmentPaths ?? (state as any)?.last_attachment_paths).filter(Boolean).map(String)
      : [],
    finalAnswer: String((state as any)?.finalAnswer ?? (state as any)?.final_answer ?? ''),
    lastError: String((state as any)?.lastError ?? (state as any)?.last_error ?? ''),
  };
}

export function normalizeAppConfig(config?: Partial<AppConfig> | null): AppConfig {
  const incomingFileManager = config?.fileManagerConfig;
  const incomingEmailSender = config?.emailSenderConfig;
  const incomingSso = config?.ssoConfig;
  const incomingOracleAnalyst = config?.oracleAnalystConfig;
  const incomingOracleConnections = Array.isArray(config?.oracleConnections) ? config!.oracleConnections : [];
  const incomingPortalApps = Array.isArray(config?.portalApps) ? config!.portalApps : [];
  const incomingCustomAgents = Array.isArray(config?.customAgents) ? config!.customAgents : [];
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
    managerUseRagFunctionalContext: config?.managerUseRagFunctionalContext ?? DEFAULT_CONFIG.managerUseRagFunctionalContext,
    agentVisibility: {
      ...DEFAULT_CONFIG.agentVisibility,
      ...(config?.agentVisibility ?? {}),
    },
    settingsAccessPassword: config?.settingsAccessPassword || DEFAULT_CONFIG.settingsAccessPassword,
    ssoConfig: {
      ...DEFAULT_CONFIG.ssoConfig,
      ...(incomingSso ?? {}),
      enabled: incomingSso?.enabled ?? DEFAULT_CONFIG.ssoConfig.enabled,
      providerType:
        incomingSso?.providerType === 'saml' || incomingSso?.providerType === 'generic'
          ? incomingSso.providerType
          : 'oidc',
      providerLabel: String(incomingSso?.providerLabel ?? DEFAULT_CONFIG.ssoConfig.providerLabel),
      issuerUrl: String(incomingSso?.issuerUrl ?? DEFAULT_CONFIG.ssoConfig.issuerUrl),
      authorizationUrl: String(incomingSso?.authorizationUrl ?? DEFAULT_CONFIG.ssoConfig.authorizationUrl),
      tokenUrl: String(incomingSso?.tokenUrl ?? DEFAULT_CONFIG.ssoConfig.tokenUrl),
      userInfoUrl: String(incomingSso?.userInfoUrl ?? DEFAULT_CONFIG.ssoConfig.userInfoUrl),
      jwksUrl: String(incomingSso?.jwksUrl ?? DEFAULT_CONFIG.ssoConfig.jwksUrl),
      clientId: String(incomingSso?.clientId ?? DEFAULT_CONFIG.ssoConfig.clientId),
      clientSecret: String(incomingSso?.clientSecret ?? DEFAULT_CONFIG.ssoConfig.clientSecret),
      redirectUri: String(incomingSso?.redirectUri ?? DEFAULT_CONFIG.ssoConfig.redirectUri),
      scopes: String(incomingSso?.scopes ?? DEFAULT_CONFIG.ssoConfig.scopes),
      logoutUrl: String(incomingSso?.logoutUrl ?? DEFAULT_CONFIG.ssoConfig.logoutUrl),
      allowedDomains: Array.isArray(incomingSso?.allowedDomains)
        ? incomingSso!.allowedDomains.filter(Boolean).map(String)
        : DEFAULT_CONFIG.ssoConfig.allowedDomains,
      roleClaim: String(incomingSso?.roleClaim ?? DEFAULT_CONFIG.ssoConfig.roleClaim),
      emailClaim: String(incomingSso?.emailClaim ?? DEFAULT_CONFIG.ssoConfig.emailClaim),
      nameClaim: String(incomingSso?.nameClaim ?? DEFAULT_CONFIG.ssoConfig.nameClaim),
      allowAdminPasswordFallback: incomingSso?.allowAdminPasswordFallback ?? DEFAULT_CONFIG.ssoConfig.allowAdminPasswordFallback,
    },
    agenticDataVizUrl: config?.agenticDataVizUrl ?? DEFAULT_CONFIG.agenticDataVizUrl,
    portalApps: incomingPortalApps
      .filter((app): app is PortalApp => Boolean(app))
      .map((app, index) => ({
        id: app.id || `portal_app_${index + 1}`,
        name: app.name || '',
        url: app.url || '',
        description: app.description || '',
      })),
    customAgents: incomingCustomAgents
      .filter((agent): agent is CustomAgentConfig => Boolean(agent))
      .map((agent, index) => ({
        id: agent.id || `custom_agent_${index + 1}`,
        title: agent.title || `Custom Agent ${index + 1}`,
        description: agent.description || '',
        pythonCode: agent.pythonCode || '',
        systemPrompt: agent.systemPrompt || '',
        managerRoutingHint: agent.managerRoutingHint || '',
        status: agent.status === 'ready' || agent.status === 'error' ? agent.status : 'draft',
        statusMessage: agent.statusMessage || '',
        enabled: agent.enabled ?? false,
        badgeColor: agent.badgeColor || 'zinc',
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
    emailSenderConfig: {
      ...DEFAULT_CONFIG.emailSenderConfig,
      ...(incomingEmailSender ?? {}),
      host: String(incomingEmailSender?.host ?? DEFAULT_CONFIG.emailSenderConfig.host),
      port: Math.min(65535, Math.max(1, Number(incomingEmailSender?.port ?? DEFAULT_CONFIG.emailSenderConfig.port) || DEFAULT_CONFIG.emailSenderConfig.port)),
      secure: incomingEmailSender?.secure ?? DEFAULT_CONFIG.emailSenderConfig.secure,
      startTls: incomingEmailSender?.startTls ?? DEFAULT_CONFIG.emailSenderConfig.startTls,
      username: String(incomingEmailSender?.username ?? DEFAULT_CONFIG.emailSenderConfig.username),
      password: String(incomingEmailSender?.password ?? DEFAULT_CONFIG.emailSenderConfig.password),
      fromEmail: String(incomingEmailSender?.fromEmail ?? DEFAULT_CONFIG.emailSenderConfig.fromEmail),
      fromName: String(incomingEmailSender?.fromName ?? DEFAULT_CONFIG.emailSenderConfig.fromName),
      replyTo: String(incomingEmailSender?.replyTo ?? DEFAULT_CONFIG.emailSenderConfig.replyTo),
      allowedRecipients: Array.isArray(incomingEmailSender?.allowedRecipients)
        ? incomingEmailSender!.allowedRecipients.filter(Boolean).map(String)
        : DEFAULT_CONFIG.emailSenderConfig.allowedRecipients,
      systemPrompt: String(incomingEmailSender?.systemPrompt ?? DEFAULT_CONFIG.emailSenderConfig.systemPrompt),
    },
    mcpTools: Array.isArray(config?.mcpTools)
      ? config!.mcpTools.map((tool) => ({
          id: tool.id || `mcp_${Date.now()}`,
          label: tool.label || 'New Tool',
          url: tool.url || '',
          description: tool.description || '',
          presetQuestions: Array.isArray(tool.presetQuestions)
            ? tool.presetQuestions.map((preset, presetIndex) => ({
                id: preset?.id || `${tool.id || 'mcp'}_preset_${presetIndex + 1}`,
                label: String(preset?.label || '').trim(),
                prompt: String(preset?.prompt || '').trim(),
                preferredTool: String(preset?.preferredTool || '').trim(),
              }))
            : [],
        }))
      : DEFAULT_CONFIG.mcpTools.map((tool) => ({ ...tool })),
  };
}

export function normalizeAppPreferences(preferences?: Partial<AppPreferences> | null): AppPreferences {
  const nextAgentRole =
    preferences?.agentRole === 'clickhouse_query' || preferences?.agentRole === 'data_analyst' || preferences?.agentRole === 'auto_ml' || preferences?.agentRole === 'file_management' || preferences?.agentRole === 'pdf_creator' || preferences?.agentRole === 'oracle_analyst' || preferences?.agentRole === 'data_cleaner' || preferences?.agentRole === 'anonymizer' || preferences?.agentRole === 'email_sender' || preferences?.agentRole === 'custom_agent'
      ? preferences.agentRole
      : 'manager';
  return {
    ...DEFAULT_PREFERENCES,
    ...(preferences ?? {}),
    agentRole: nextAgentRole,
    selectedCustomAgentId: String(preferences?.selectedCustomAgentId ?? ''),
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
  const selectedCustomAgentId = config.customAgents.some((agent) => agent.id === preferences.selectedCustomAgentId && agent.enabled)
    ? preferences.selectedCustomAgentId
    : (config.customAgents.find((agent) => agent.enabled)?.id ?? '');
  const isRoleVisible = (role: AgentRole): boolean =>
    role === 'custom_agent'
      ? config.customAgents.some((agent) => agent.enabled)
      : config.agentVisibility[role as BuiltInAgentRole] !== false;
  const fallbackBuiltInRole = BUILT_IN_AGENT_ROLES.find((role) => config.agentVisibility[role] !== false) ?? 'manager';
  const agentRole = isRoleVisible(preferences.agentRole)
    ? preferences.agentRole
    : (config.customAgents.some((agent) => agent.enabled) ? 'custom_agent' : fallbackBuiltInRole);

  return {
    config,
    conversations,
    preferences: {
      ...preferences,
      agentRole,
      currentConversationId: wantsFreshChat
        ? null
        : hasCurrentConversation
        ? preferences.currentConversationId
        : (conversations[0]?.id ?? null),
      selectedMcpToolId,
      selectedCustomAgentId,
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
  text = text.replace(/<button\b[^>]*>([\s\S]*?)<\/button>/gi, (_, label: string) => {
    const normalized = label.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
    return normalized ? `\n- [ ] ${normalized}\n` : '';
  });
  // Fix inline task list items generated by some LLMs
  text = text.replace(/([^\n])\s+([-*]\s+\[[ xX]\]\s+)/g, '$1\n$2');
  // Fix inline normal list items (only if preceded by punctuation to avoid matching "A - B")
  text = text.replace(/([.!?])\s+([-*]\s+[A-Z0-9])/g, '$1\n$2');
  // Fix inline numbered list items
  text = text.replace(/([.!?])\s+(\d+\.\s+[A-Z0-9])/g, '$1\n$2');
  text = promoteChoiceListsToTaskLists(text);
  return text;
}

const COLLAPSIBLE_MARKDOWN_SECTION_RULES: Array<{ pattern: RegExp; summary: string }> = [
  { pattern: /^(executed sql|sql used|sql queries?|queries executed|query log)$/i, summary: 'Expand SQL' },
  { pattern: /^(technical details?|technical notes?|implementation notes?|appendix|appendices)$/i, summary: 'Expand details' },
  { pattern: /^(reasoning|reasoning summary|analysis reasoning|method|approach)$/i, summary: 'Expand reasoning' },
  { pattern: /^(data preview|result preview|sample rows?|raw results?)$/i, summary: 'Expand data preview' },
  { pattern: /^(knowledge signals|sources?|reference notes?|evidence trail|confidence|action log)$/i, summary: 'Expand details' },
];

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function collapsibleSummaryForHeading(title: string): string | null {
  const normalized = title.trim();
  if (!normalized) return null;
  for (const rule of COLLAPSIBLE_MARKDOWN_SECTION_RULES) {
    if (rule.pattern.test(normalized)) {
      return rule.summary;
    }
  }
  return null;
}

function collapseTechnicalSections(md: string): string {
  const lines = md.split('\n');
  const renderedSections: string[] = [];
  let currentSection: string[] = [];
  let inCodeFence = false;

  const flushSection = () => {
    if (currentSection.length === 0) return;
    const headingMatch = currentSection[0].match(/^(#{2,4})\s+(.+?)\s*$/);
    const summaryLabel = headingMatch ? collapsibleSummaryForHeading(headingMatch[2]) : null;
    const sectionBody = currentSection.slice(1).join('\n').trim();

    if (
      headingMatch &&
      summaryLabel &&
      sectionBody &&
      !sectionBody.includes('<details')
    ) {
      renderedSections.push(
        `<details>\n<summary>${escapeHtml(summaryLabel)}</summary>\n\n${sectionBody}\n</details>`
      );
    } else {
      renderedSections.push(currentSection.join('\n'));
    }
    currentSection = [];
  };

  for (const line of lines) {
    if (/^\s*```/.test(line)) {
      inCodeFence = !inCodeFence;
    }
    const isHeading = !inCodeFence && /^(#{2,4})\s+.+$/.test(line);
    if (isHeading) {
      flushSection();
    }
    currentSection.push(line);
  }

  flushSection();
  return renderedSections.join('\n');
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
