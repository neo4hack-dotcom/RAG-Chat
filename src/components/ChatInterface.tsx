import React, { useState, useRef, useEffect, useLayoutEffect, useMemo } from "react";
import { Send, Settings, Hammer, Loader2, Bot, Plus, MessageSquare, Trash2, Database, Network, Cpu, PanelLeftClose, PanelLeftOpen, Star, Paperclip, X, File, Moon, Sun, Home, CalendarDays, ChevronDown, ChevronRight, FolderOpen, BarChart3, Minus, RotateCcw, ZoomIn, Copy, Check, Terminal, BrainCircuit, FilePenLine, Gauge } from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { Message, AppConfig, Conversation, Attachment, McpTool, WorkflowMode, AgentRole, ChatAction, CrewPlan, CrewPlanDraft, PlanningBackendState, FileManagerAgentConfig, DataQualitySchemaColumn, DataQualityState, AgentStep, buildConversationMemory, createEmptyCrewPlanDraft, normalizeCrewPlanDraft, normalizePlanningAgentState, normalizePlanningBackendState, normalizeFileManagerAgentState, normalizePdfCreatorAgentState, normalizeOracleAnalystAgentState, normalizeDataAnalystAgentState, normalizeManagerAgentState, normalizeDataQualityState, normalizeAppConfig } from "../lib/utils";
import { ChatMessage } from "./ChatMessage";
import { PlanningModal } from "./PlanningModal";
import { PlanningMonitorModal } from "./PlanningMonitorModal";
import { AgentConsoleModal } from "./AgentConsoleModal";
import { FileManagerConfigModal } from "./FileManagerConfigModal";
import { DataQualityModal } from "./DataQualityModal";
import { SqlDraftModal } from "./SqlDraftModal";
import { downloadMarkdownPdf } from "../lib/pdf";

interface ChatInterfaceProps {
  config: AppConfig;
  conversations: Conversation[];
  currentId: string | null;
  workflow: WorkflowMode;
  agentRole: AgentRole;
  mcpToolId: string;
  onConversationsChange: React.Dispatch<React.SetStateAction<Conversation[]>>;
  onCurrentIdChange: (id: string | null) => void;
  onWorkflowChange: (workflow: WorkflowMode) => void;
  onAgentRoleChange: (role: AgentRole) => void;
  onMcpToolIdChange: (id: string) => void;
  onConfigChange: (config: AppConfig) => void;
  isDark: boolean;
  onToggleDark: () => void;
  onGoHome?: () => void;
}

const AGENT_INTRO_MARKERS = {
  manager: "<!-- agent-intro:manager -->",
  clickhouse_query: "<!-- agent-intro:clickhouse_query -->",
  data_analyst: "<!-- agent-intro:data_analyst -->",
  file_management: "<!-- agent-intro:file_management -->",
  pdf_creator: "<!-- agent-intro:pdf_creator -->",
  oracle_analyst: "<!-- agent-intro:oracle_analyst -->",
  data_quality_tables: "<!-- agent-intro:data_quality_tables -->",
} as const;

function getAgentIntroContent(agentRole: AgentRole): string | null {
  if (agentRole === 'manager') {
    return `${AGENT_INTRO_MARKERS.manager}
## Agent Manager

This manager works in English and can orchestrate the available specialist agents.

- It routes requests to Clickhouse SQL, Data analyst, File management, Oracle SQL, or Data quality - Tables when needed.
- It can also route export-ready results to PDF creator when you want a polished document.
- It keeps the conversation context while delegated agents ask follow-up questions.
- It answers directly when no specialist tool is required.`;
  }

  if (agentRole === 'clickhouse_query') {
    return `${AGENT_INTRO_MARKERS.clickhouse_query}
## Clickhouse SQL Agent

This agent works in English and guides the analysis safely before running SQL.

- It tries to infer the best table automatically from your question whenever the intent is clear.
- It only asks you to choose a table, field, or date column when the request stays ambiguous.
- It can generate charts on demand and also suggests a visualization when the result deserves one.
- It returns a short final answer, the executed SQL, and a concise reasoning summary.`;
  }

  if (agentRole === 'data_analyst') {
    return `${AGENT_INTRO_MARKERS.data_analyst}
## Data Analyst Agent

This agent works in English and handles deeper ClickHouse investigations through a multi-step loop.

- It can run several targeted queries before answering, instead of stopping after a single SQL execution.
- It keeps one primary table in focus, asks you to choose only when the table stays ambiguous, and preserves the latest analytical context in the same conversation.
- It can look into the knowledge base when configured, repair failed SQL automatically with a simpler fallback, and export the latest dataset to CSV when you explicitly ask for it.
- It returns a business-facing markdown answer while keeping analytical steps available from the thinking panel.`;
  }

  if (agentRole === 'file_management') {
    return `${AGENT_INTRO_MARKERS.file_management}
## File Management Agent

This agent works in English and uses backend Python tools to inspect and manage files safely.

- Use it to browse folders, read files, summarize CSV or Excel data, and create or edit supported files.
- Overwrite, move, and delete operations always require an explicit confirmation step before execution.
- Use the Configure button, or double-click the agent chip below, to configure the access root, iteration limit, or custom system prompt.`;
  }

  if (agentRole === 'pdf_creator') {
    return `${AGENT_INTRO_MARKERS.pdf_creator}
## PDF Creator Agent

This agent works in English and turns the latest analysis or pasted content into a polished PDF.

- It creates a clean export with a professional layout that matches the app's visual tone.
- It can reuse the latest useful assistant result in the chat, or ask you to paste content explicitly.
- Existing PDF files always require confirmation before overwrite.`;
  }

  if (agentRole === 'oracle_analyst') {
    return `${AGENT_INTRO_MARKERS.oracle_analyst}
## Oracle SQL Agent

This agent works in English and queries Oracle safely from natural language.

- It inspects accessible tables and schema before generating Oracle SQL.
- It validates read-only SQL before execution and can recover from query errors automatically.
- It returns an executive summary, key metrics, the SQL used, a preview data table, insights, and a confidence score.
- Configure one or more Oracle connections in Settings, then ask a business question directly.`;
  }

  if (agentRole === 'data_quality_tables') {
    return `${AGENT_INTRO_MARKERS.data_quality_tables}
## Data Quality - Tables Agent

This agent works in English and runs structured profiling through a dedicated form.

- Use the form to select the table, columns, sample size, row filter, and optional time column.
- It computes SQL statistics first, then asks the local LLM to score quality issues and recommend fixes.
- The final report includes an executive summary, per-column findings, and optional volumetric analysis.`;
  }

  return null;
}

function hasAgentIntroMessage(messages: Message[], agentRole: AgentRole): boolean {
  const marker =
    agentRole === 'manager'
      ? AGENT_INTRO_MARKERS.manager
      : agentRole === 'clickhouse_query'
        ? AGENT_INTRO_MARKERS.clickhouse_query
        : agentRole === 'data_analyst'
          ? AGENT_INTRO_MARKERS.data_analyst
        : agentRole === 'file_management'
          ? AGENT_INTRO_MARKERS.file_management
          : agentRole === 'pdf_creator'
            ? AGENT_INTRO_MARKERS.pdf_creator
          : agentRole === 'oracle_analyst'
            ? AGENT_INTRO_MARKERS.oracle_analyst
          : agentRole === 'data_quality_tables'
            ? AGENT_INTRO_MARKERS.data_quality_tables
          : null;
  if (!marker) return false;
  return messages.some((message) => message.role === 'assistant' && message.content.includes(marker));
}

function createAgentIntroMessage(agentRole: AgentRole): Message | null {
  const content = getAgentIntroContent(agentRole);
  if (!content) return null;
  return {
    id: `agent-intro-${agentRole}-${Date.now()}`,
    role: "assistant",
    content,
    timestamp: Date.now(),
  };
}

type DataQualityFormState = {
  table: string;
  columns: string[];
  sampleSize: number;
  rowFilter: string;
  timeColumn: string | null;
};

type MentionTargetDefinition = {
  id: string;
  label: string;
  shortLabel: string;
  aliases: string[];
  role?: AgentRole;
  icon: LucideIcon;
  tone: string;
};

type BreadcrumbNode = {
  id: string;
  label: string;
  icon: LucideIcon;
  toneClass: string;
};

type DraftArtifact = {
  id: string;
  title: string;
  kind: "sql" | "code" | "note" | "chart";
  kindLabel: string;
  preview: string;
  content: string;
  timestamp: number;
  engineHint?: "clickhouse" | "oracle";
};

type SqlDraftPreviewResult = {
  engine: "clickhouse" | "oracle";
  executedSql: string;
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
  shownRows: number;
  rowLimit: number;
};

const CHAT_ZOOM_MIN = 0.85;
const CHAT_ZOOM_MAX = 1.4;
const CHAT_ZOOM_STEP = 0.05;
const CHAT_ZOOM_DEFAULT = 1;
const BEAUTIFUL_RESPONSE_PROMPT = "[SYSTEM: For every non-JSON answer, produce a polished, presentation-ready result. Prefer elegant Markdown with short section headings, compact bullet lists, tasteful **bold** emphasis, comparison tables when useful, and blockquotes for notes. If the user explicitly asks for a table, rows/columns, a schema list, a matrix, a grid, or a tabular preview, return the relevant structured result as a valid compact Markdown table whenever the data is naturally tabular. Put the main answer first. If you include technical details, SQL, raw previews, reasoning notes, or appendices, place them after the main answer and preferably inside `<details><summary>Expand details</summary>...</details>` blocks. You may use safe semantic HTML fragments such as <section>, <article>, <details>, <summary>, <table>, <ul>, <ol>, and <blockquote> when they genuinely improve the layout. Never output a full HTML document, CSS, JavaScript, <head>, <body>, <iframe>, or inline event handlers. Keep the result clean, premium, readable, and visually impressive in the chat UI.]";
const CLICKABLE_CHOICES_PROMPT = '[SYSTEM: If you need clarification and you offer explicit choices, format every selectable option as its own markdown task list item using exactly "- [ ] Option". Keep option labels short so the UI can turn them into clickable replies.]';
const WELCOME_MESSAGE_CONTENT = "# Welcome to RAGnarok ⚡️\n\nI'm your AI assistant, ready to connect to your LLMs, RAG system, or agents.";

function isDefaultWelcomeMessage(message: Message): boolean {
  return message.role === 'assistant' && message.content.trim() === WELCOME_MESSAGE_CONTENT;
}

function isAgentIntroMessage(message: Message): boolean {
  if (message.role !== 'assistant') return false;
  return Object.values(AGENT_INTRO_MARKERS).some((marker) => message.content.includes(marker));
}

function isIntroductoryAssistantMessage(message: Message): boolean {
  return isDefaultWelcomeMessage(message) || isAgentIntroMessage(message);
}

function hasMeaningfulConversationMessages(messages: Message[]): boolean {
  return messages.some((message) => !isIntroductoryAssistantMessage(message));
}

function stepBadgeLabel(step: AgentStep): string {
  const rawType = ((step as any).type ?? '').toString().trim();
  return rawType ? rawType.replace(/_/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase()) : '';
}

function stepReasoning(step: AgentStep): string {
  return ((step as any).reasoning ?? '').toString().trim();
}

function stepResultSummary(step: AgentStep): string {
  return (((step as any).result_summary ?? (step as any).resultSummary) ?? '').toString().trim();
}

function stepSql(step: AgentStep): string {
  return ((step as any).sql ?? '').toString().trim();
}

function stepRowCount(step: AgentStep): number | null {
  const raw = (step as any).row_count ?? (step as any).rowCount;
  return typeof raw === 'number' && Number.isFinite(raw) ? raw : null;
}

function stepSuggestedPath(step: AgentStep): string {
  return (((step as any).suggested_path ?? (step as any).suggestedPath) ?? '').toString().trim();
}

function compactMessagePreview(content: string, maxLength = 132): string {
  const plain = String(content || '')
    .replace(/<!--[\s\S]*?-->/g, ' ')
    .replace(/[`#>*_-]/g, ' ')
    .replace(/\[(.*?)\]\(.*?\)/g, '$1')
    .replace(/\s+/g, ' ')
    .trim();
  if (!plain) return 'Assistant response';
  return plain.length > maxLength ? `${plain.slice(0, maxLength - 1)}…` : plain;
}

function inferSqlDraftEngine(workflow: WorkflowMode, agentRole: AgentRole, sql: string): "clickhouse" | "oracle" {
  const cleaned = String(sql || "").toLowerCase();
  if (
    agentRole === "oracle_analyst"
    || /\b(fetch first\s+\d+\s+rows\s+only|rownum\s*<=|nvl\(|decode\(|sysdate\b|to_char\()/i.test(cleaned)
  ) {
    return "oracle";
  }
  if (workflow === "AGENT" && (agentRole === "clickhouse_query" || agentRole === "data_analyst" || agentRole === "data_quality_tables")) {
    return "clickhouse";
  }
  return "clickhouse";
}

function clampChatZoom(value: number): number {
  return Math.min(CHAT_ZOOM_MAX, Math.max(CHAT_ZOOM_MIN, Number(value) || CHAT_ZOOM_DEFAULT));
}

function buildDataQualityFormState(state?: Partial<DataQualityState> | null): DataQualityFormState {
  return {
    table: state?.table ?? "",
    columns: Array.isArray(state?.columns) ? state.columns.filter(Boolean) : [],
    sampleSize: state?.sampleSize === 0 ? 0 : (state?.sampleSize ?? 50_000),
    rowFilter: state?.rowFilter ?? "",
    timeColumn: state?.timeColumn ?? null,
  };
}

function keepColumnsInSchema(columns: string[], schema: DataQualitySchemaColumn[]): string[] {
  const available = new Set(schema.map((column) => column.name));
  return columns.filter((column) => available.has(column));
}

function summarizeDataQualityRun(form: DataQualityFormState): string {
  const lines = [
    `Run data quality analysis on table \`${form.table}\`.`,
    `Columns selected: ${form.columns.length}.`,
  ];
  if (form.rowFilter.trim()) {
    lines.push(`Row filter: \`${form.rowFilter.trim()}\`.`);
  }
  if (form.timeColumn) {
    lines.push(`Volumetric analysis on \`${form.timeColumn}\`.`);
  }
  lines.push(
    form.sampleSize === 0
      ? "Sample size: full scan capped at 2,000,000 rows."
      : `Sample size: ${form.sampleSize.toLocaleString()} rows.`
  );
  return lines.join("\n");
}

const AGENT_ROLE_LABELS: Record<AgentRole, string> = {
  manager: 'Agent Manager',
  clickhouse_query: 'Clickhouse SQL',
  data_analyst: 'Data Analyst',
  file_management: 'File management',
  pdf_creator: 'PDF creator',
  oracle_analyst: 'Oracle SQL',
  data_quality_tables: 'Data quality - Tables',
};

const AGENT_ROLE_SHORT_LABELS: Record<AgentRole, string> = {
  manager: 'MGR',
  clickhouse_query: 'CLICK',
  data_analyst: 'ANALYST',
  file_management: 'FILES',
  pdf_creator: 'PDF',
  oracle_analyst: 'ORACLE',
  data_quality_tables: 'QUALITY',
};

const AGENT_MENTION_TARGETS: MentionTargetDefinition[] = [
  { id: 'manager', label: 'Agent Manager', shortLabel: 'MGR', aliases: ['manager', 'planner', 'planificateur'], role: 'manager', icon: Star, tone: 'from-amber-500 to-orange-500' },
  { id: 'clickhouse', label: 'Clickhouse SQL', shortLabel: 'CLICK', aliases: ['clickhouse', 'clickhousesql'], role: 'clickhouse_query', icon: Database, tone: 'from-cyan-500 to-sky-500' },
  { id: 'data_analyst', label: 'Data Analyst', shortLabel: 'ANALYST', aliases: ['analyst', 'dataanalyst'], role: 'data_analyst', icon: Cpu, tone: 'from-violet-500 to-fuchsia-500' },
  { id: 'oracle', label: 'Oracle SQL', shortLabel: 'ORACLE', aliases: ['oracle', 'oraclesql'], role: 'oracle_analyst', icon: Database, tone: 'from-orange-500 to-amber-500' },
  { id: 'files', label: 'File management', shortLabel: 'FILES', aliases: ['file', 'files', 'filemanager'], role: 'file_management', icon: FolderOpen, tone: 'from-emerald-500 to-teal-500' },
  { id: 'pdf', label: 'PDF creator', shortLabel: 'PDF', aliases: ['pdf', 'pdfcreator'], role: 'pdf_creator', icon: File, tone: 'from-slate-600 to-slate-800' },
  { id: 'quality', label: 'Data quality - Tables', shortLabel: 'QUALITY', aliases: ['quality', 'dataquality'], role: 'data_quality_tables', icon: BarChart3, tone: 'from-fuchsia-500 to-pink-500' },
  { id: 'designer', label: 'Designer', shortLabel: 'DESIGN', aliases: ['designer'], icon: FilePenLine, tone: 'from-pink-500 to-rose-500' },
  { id: 'writer', label: 'Writer', shortLabel: 'WRITER', aliases: ['writer', 'redacteur', 'rédacteur'], icon: FilePenLine, tone: 'from-indigo-500 to-blue-500' },
  { id: 'strategist', label: 'Strategist', shortLabel: 'STRAT', aliases: ['strategist', 'stratege', 'stratège'], icon: BrainCircuit, tone: 'from-purple-500 to-indigo-500' },
  { id: 'executor', label: 'Executor', shortLabel: 'EXEC', aliases: ['executor', 'executeur', 'exécuteur'], icon: Hammer, tone: 'from-slate-700 to-slate-900' },
];

function normalizeMentionToken(value: string): string {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, '');
}

function extractMentionTargets(text: string): MentionTargetDefinition[] {
  const matches = String(text || '').match(/@([A-Za-zÀ-ÖØ-öø-ÿ0-9_-]+)/g) ?? [];
  const seen = new Set<string>();
  const resolved: MentionTargetDefinition[] = [];

  for (const rawMatch of matches) {
    const token = normalizeMentionToken(rawMatch.slice(1));
    const target = AGENT_MENTION_TARGETS.find((entry) => entry.aliases.some((alias) => normalizeMentionToken(alias) === token));
    if (!target || seen.has(target.id)) continue;
    seen.add(target.id);
    resolved.push(target);
  }

  return resolved;
}

function getMentionQuery(text: string, cursor: number): string | null {
  const safeCursor = Number.isFinite(cursor) ? Math.max(0, cursor) : text.length;
  const beforeCursor = text.slice(0, safeCursor);
  const match = beforeCursor.match(/(?:^|\s)@([A-Za-zÀ-ÖØ-öø-ÿ0-9_-]*)$/);
  return match ? match[1] ?? '' : null;
}

function replaceMentionToken(text: string, cursor: number, replacement: string): string {
  const safeCursor = Number.isFinite(cursor) ? Math.max(0, cursor) : text.length;
  const beforeCursor = text.slice(0, safeCursor);
  const afterCursor = text.slice(safeCursor);
  const match = beforeCursor.match(/(?:^|\s)@([A-Za-zÀ-ÖØ-öø-ÿ0-9_-]*)$/);
  if (!match || match.index === undefined) {
    return `${text.trimEnd()} ${replacement} `.trimStart();
  }
  const prefix = beforeCursor.slice(0, match.index);
  const leadingSpace = beforeCursor.charAt(match.index) === ' ' ? ' ' : '';
  return `${prefix}${leadingSpace}${replacement} ${afterCursor}`.replace(/\s{2,}/g, ' ');
}

function buildDraftArtifacts(messages: Message[], workflow: WorkflowMode, agentRole: AgentRole): DraftArtifact[] {
  const artifacts: DraftArtifact[] = [];

  [...messages]
    .filter((message) => message.role === 'assistant' && !isIntroductoryAssistantMessage(message))
    .reverse()
    .forEach((message, messageIndex) => {
      const content = String(message.content || '').trim();
      if (!content) return;

      const codeBlocks = [...content.matchAll(/```([\w-]+)?\n([\s\S]*?)```/g)];
      codeBlocks.slice(0, 2).forEach((block, blockIndex) => {
        const language = block[1] ? block[1].toUpperCase() : 'CODE';
        const snippet = (block[2] || '').trim();
        if (!snippet) return;
        const normalizedLanguage = language.toLowerCase();
        const isSqlDraft = normalizedLanguage === "sql" || normalizedLanguage === "clickhouse" || normalizedLanguage === "oracle";
        artifacts.push({
          id: `${message.id}-code-${blockIndex}`,
          title: isSqlDraft ? "SQL draft" : `${language} draft`,
          kind: isSqlDraft ? "sql" : "code",
          kindLabel: isSqlDraft ? "SQL" : language,
          preview: compactMessagePreview(snippet, 180),
          content: snippet,
          timestamp: message.timestamp - blockIndex,
          engineHint: isSqlDraft ? inferSqlDraftEngine(workflow, agentRole, snippet) : undefined,
        });
      });

      if (message.chart) {
        artifacts.push({
          id: `${message.id}-chart`,
          title: message.chart.title || 'Chart draft',
          kind: "chart",
          kindLabel: message.chart.type.toUpperCase(),
          preview: `${message.chart.points.length} point(s) prepared for ${message.chart.xField} × ${message.chart.yField}.`,
          content: content,
          timestamp: message.timestamp + 1,
        });
      }

      if (content.length > 320 || /\|.+\|/.test(content)) {
        const headingMatch = content.match(/^##?\s+(.+)$/m);
        artifacts.push({
          id: `${message.id}-note-${messageIndex}`,
          title: headingMatch?.[1]?.trim() || 'Analysis draft',
          kind: "note",
          kindLabel: 'NOTE',
          preview: compactMessagePreview(content, 220),
          content,
          timestamp: message.timestamp + 2,
        });
      }
    });

  return artifacts
    .sort((a, b) => b.timestamp - a.timestamp)
    .slice(0, 8);
}

function isFrenchCapabilitiesQuery(text: string): boolean {
  return /(?:que peux?-?tu faire(?: pour moi)?|qu['’]est-ce que tu peux faire(?: pour moi)?|que peut faire (?:l['’])?(?:app|application|outil|ragnarok)|comment .*servir|comment .*utiliser|aide-moi à comprendre|capacités|fonctionnalités|quel agent utiliser)/i.test(text);
}

function isEnglishCapabilitiesQuery(text: string): boolean {
  return /(?:what can you do(?: for me)?|what can this app do|how can you help|what is this app able to do|how do i use|how can i use|capabilities|features|which agent should i use)/i.test(text);
}

function isAppCapabilitiesQuery(text: string): boolean {
  const normalized = text.trim();
  if (!normalized) return false;
  return isFrenchCapabilitiesQuery(normalized) || isEnglishCapabilitiesQuery(normalized);
}

function withBeautifulResponsePrompt(basePrompt: string): string {
  const normalized = String(basePrompt || '').trim();
  return normalized
    ? `${normalized}\n\n${BEAUTIFUL_RESPONSE_PROMPT}`
    : BEAUTIFUL_RESPONSE_PROMPT;
}

function buildAppCapabilitiesReply(text: string): string {
  const replyInFrench = isFrenchCapabilitiesQuery(text);

  if (replyInFrench) {
    return `## Ce que RAGnarok peut faire pour toi

RAGnarok peut fonctionner de plusieurs façons selon ton besoin, mais ses **agents spécialisés** sont le meilleur point de départ si tu veux obtenir une action concrète plutôt qu'une simple réponse.

### Agent Manager

- C'est l'agent **chef d'orchestre**.
- Tu peux lui décrire directement un objectif métier, par exemple : **"analyse les ventes puis exporte le résultat en CSV"**.
- Il peut déléguer à d'autres agents comme **Clickhouse SQL**, **Data Analyst**, **Oracle SQL**, **File management**, **PDF creator** ou **Data quality - Tables** quand c'est pertinent.
- C'est le bon choix si tu ne sais pas encore quel agent utiliser.

### Clickhouse SQL

- Cet agent sert à **interroger une base ClickHouse** en langage naturel.
- Il peut choisir la bonne table automatiquement si la demande est claire, sinon il te propose des choix cliquables.
- Il peut aussi proposer ou générer un **graphique** quand le résultat s'y prête.
- Utilise-le pour des demandes comme : **"show me the revenue by month"**, **"find the latest failed jobs"**, ou **"compare sales by region"**.

### Data Analyst

- Cet agent sert à mener une **analyse plus approfondie** sur ClickHouse quand une seule requête ne suffit pas.
- Il peut enchaîner plusieurs requêtes, garder le fil de l'enquête dans la conversation, réparer automatiquement une requête SQL trop complexe ou invalide, et exporter le dernier dataset en CSV si tu le demandes explicitement.
- Utilise-le pour des demandes comme : **"explain the sales drop by week, country, and product"**, **"investigate why failed jobs increased"**, ou **"compare retention patterns and summarize the drivers"**.

### Oracle SQL

- Cet agent sert à **interroger une base Oracle** en langage naturel.
- Il explore les tables accessibles, inspecte le schéma, génère du **SQL Oracle optimisé**, puis répond avec une **synthèse narrative en Markdown**.
- Il valide la requête avant exécution et peut corriger automatiquement le SQL si Oracle renvoie une erreur exploitable.
- Utilise-le pour des demandes comme : **"summarize overdue invoices from Oracle"**, **"show top customers by revenue in Oracle"**, ou **"compare monthly orders in the Oracle ERP schema"**.

### File management

- Cet agent sert à **lire, créer, modifier, déplacer et supprimer des fichiers**.
- Il peut travailler sur des fichiers texte, CSV et Excel, avec confirmation avant les actions sensibles.
- Utilise-le pour : **créer un fichier**, **générer un export CSV/XLSX**, **lire un dossier**, **résumer un fichier**, ou **mettre à jour un document**.
- Il est particulièrement utile après une requête de données quand tu veux **sauvegarder le résultat dans un fichier**.

### PDF creator

- Cet agent sert à **transformer une analyse, une synthèse ou un résultat en PDF professionnel**.
- Il peut reprendre le **dernier résultat utile du chat** ou un contenu que tu colles manuellement.
- Utilise-le quand tu veux un **livrable PDF clair et partageable**, avec une mise en page cohérente avec l'app.

### Data quality - Tables

- Cet agent sert à faire de la **data quality** et du **profiling statistique** sur une table.
- Il s'utilise via un **formulaire guidé** où tu choisis la table, les colonnes, le sample size, le filtre éventuel et la colonne temporelle.
- Il calcule des statistiques, détecte les anomalies, puis produit une **synthèse structurée** avec recommandations.
- Utilise-le pour des demandes comme : **"check data quality on customers"** ou **"profile the sales table"**.

## Autres modes utiles

- **RAG Knowledge** : interroger ta base documentaire avec recherche sémantique.
- **MCP** : appeler un outil externe compatible MCP.
- **LangGraph Planning** : programmer des agents à heure fixe ou sur événement, puis suivre les exécutions.
- **Pure LLM** : discuter librement avec le modèle sans workflow spécialisé.

## Comment bien t'en servir

- Si tu veux un résultat métier sans te poser de question, commence par **Agent Manager**.
- Si tu veux interroger des données, choisis **Clickhouse SQL**.
- Si tu veux une enquête plus poussée en plusieurs étapes, choisis **Data Analyst**.
- Si tu veux analyser des données Oracle, choisis **Oracle SQL**.
- Si tu veux produire ou manipuler un fichier, choisis **File management**.
- Si tu veux transformer un résultat en document prêt à partager, choisis **PDF creator**.
- Si tu veux auditer la qualité d'une table, choisis **Data quality - Tables** puis **Open form**.
- Si tu veux automatiser un scénario récurrent, utilise **LangGraph Planning**.

Si tu veux, je peux aussi te proposer **quel agent utiliser selon ton besoin exact**.`;
  }

  return `## What RAGnarok can do for you

RAGnarok supports several workflows, but its **specialist agents** are the most useful entry point when you want the app to actually perform a task instead of only chatting.

### Agent Manager

- This is the **orchestrator** agent.
- You can give it a business outcome such as **"analyze sales and export the result to CSV"**.
- It can delegate to **Clickhouse SQL**, **Data Analyst**, **Oracle SQL**, **File management**, **PDF creator**, or **Data quality - Tables** when needed.
- Use it when you want the app to decide the best path for you.

### Clickhouse SQL

- This agent is designed to **query a ClickHouse database from natural language**.
- It tries to infer the right table automatically, and only asks for clarification when the request stays ambiguous.
- It can also suggest or generate a **chart** when the result is easier to understand visually.
- Use it for requests like **"show revenue by month"**, **"find the latest failed jobs"**, or **"compare sales by region"**.

### Data Analyst

- This agent is designed for **deeper ClickHouse investigations** when a single SQL query is not enough.
- It can chain multiple analytical queries, keep the investigation context alive across the conversation, repair invalid SQL automatically with a simpler fallback, and export the latest dataset to CSV when you explicitly ask for it.
- Use it for requests like **"explain the sales drop by week, country, and product"**, **"investigate why failed jobs increased"**, or **"compare retention patterns and summarize the drivers"**.

### Oracle SQL

- This agent is designed to **query an Oracle database from natural language**.
- It discovers accessible tables, inspects schema, generates **optimized Oracle SQL**, and returns a **narrative Markdown answer**.
- It validates the query before execution and can automatically repair the SQL when Oracle returns a useful error message.
- Use it for requests like **"summarize overdue invoices from Oracle"**, **"show top customers by revenue in Oracle"**, or **"compare monthly orders in the Oracle ERP schema"**.

### File management

- This agent is built to **read, create, edit, move, and delete files**.
- It supports text files, CSV, and Excel workflows, with confirmation before destructive actions.
- Use it to **create a file**, **export data to CSV/XLSX**, **inspect folders**, **summarize a file**, or **update existing content**.
- It is especially useful after a data query when you want to **save the result as a file**.

### PDF creator

- This agent is built to **turn an analysis, summary, or result into a professional PDF**.
- It can reuse the **latest useful assistant result in the chat** or work from content you paste manually.
- Use it when you want a **shareable PDF deliverable** with a clean layout aligned with the app.

### Data quality - Tables

- This agent runs **data-quality profiling and statistical diagnostics** on a table.
- It opens a **guided form** where you choose the table, columns, sample size, optional row filter, and optional time column.
- It computes metrics, detects anomalies, and returns a **structured quality summary** with recommendations.
- Use it for requests like **"profile the customers table"** or **"check the data quality of sales records"**.

## Other useful modes

- **RAG Knowledge**: query your document base with semantic retrieval.
- **MCP**: call an external MCP-compatible tool.
- **LangGraph Planning**: schedule one or more agents to run on a fixed schedule or on events, then monitor their execution history.
- **Pure LLM**: free-form conversation with the model without a specialized workflow.

## How to use the app effectively

- Start with **Agent Manager** if you want the app to figure out the best execution path.
- Pick **Clickhouse SQL** when your goal is to analyze data from ClickHouse.
- Pick **Data Analyst** when your goal is to run a deeper multi-step investigation on ClickHouse.
- Pick **Oracle SQL** when your goal is to analyze data from Oracle.
- Pick **File management** when your goal is to create, inspect, or export files.
- Pick **PDF creator** when your goal is to turn an analysis into a polished PDF deliverable.
- Pick **Data quality - Tables** and click **Open form** when you want a guided table-quality analysis.
- Use **LangGraph Planning** when you want an automated recurring workflow.

If you want, I can also tell you **which mode or agent to use for your exact task**.`;
}

export function ChatInterface({
  config,
  conversations,
  currentId,
  workflow,
  agentRole,
  mcpToolId,
  onConversationsChange,
  onCurrentIdChange,
  onWorkflowChange,
  onAgentRoleChange,
  onMcpToolIdChange,
  onConfigChange,
  isDark,
  onToggleDark,
  onGoHome,
}: ChatInterfaceProps) {
  // --- STATE MANAGEMENT ---

  // UI and Interaction states
  const [input, setInput] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [isConnected, setIsConnected] = useState(true);
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);
  const [attachments, setAttachments] = useState<Attachment[]>([]);
  const [chatZoom, setChatZoom] = useState(CHAT_ZOOM_DEFAULT);
  const [isZoomControlOpen, setIsZoomControlOpen] = useState(false);
  const [isThinkingPanelOpen, setIsThinkingPanelOpen] = useState(false);
  const [isInputCopied, setIsInputCopied] = useState(false);
  const [isDraftPanelOpen, setIsDraftPanelOpen] = useState(false);
  const [isAgentStatePanelOpen, setIsAgentStatePanelOpen] = useState(false);
  const [inputCursor, setInputCursor] = useState(0);
  
  // Refs for DOM elements
  const fileInputRef = useRef<HTMLInputElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const chatScrollContainerRef = useRef<HTMLElement>(null);
  const lastAssistantMessageRef = useRef<HTMLDivElement>(null);
  const zoomControlRef = useRef<HTMLDivElement>(null);
  const agentIntroBootstrapRef = useRef<string | null>(null);
  const dataQualityAutoOpenRef = useRef(false);
  const dataQualityMetadataRequestRef = useRef(0);
  const toolsIslandRef = useRef<HTMLDivElement>(null);
  const activeRequestControllerRef = useRef<AbortController | null>(null);
  const browserTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';

  const currentConversation = conversations.find(c => c.id === currentId);
  const managerAgentState = normalizeManagerAgentState((currentConversation?.agentState as any)?.manager);
  const clickhouseAgentState = currentConversation?.agentState?.clickhouse;
  const dataAnalystAgentState = normalizeDataAnalystAgentState((currentConversation?.agentState as any)?.dataAnalyst);
  const planningAgentState = normalizePlanningAgentState((currentConversation?.agentState as any)?.planning, browserTimeZone);
  const fileManagerAgentState = normalizeFileManagerAgentState((currentConversation?.agentState as any)?.fileManager);
  const pdfCreatorAgentState = normalizePdfCreatorAgentState((currentConversation?.agentState as any)?.pdfCreator);
  const oracleAnalystAgentState = normalizeOracleAnalystAgentState((currentConversation?.agentState as any)?.oracleAnalyst);
  const dataQualityAgentState = normalizeDataQualityState((currentConversation?.agentState as any)?.dataQuality);
  const fallbackMessages = currentConversation?.messages || [
    {
      id: "1",
      role: "assistant",
      content: WELCOME_MESSAGE_CONTENT,
      timestamp: Date.now(),
      steps: [
        { id: 'init-1', title: 'System Initialization', status: 'success', details: 'Loaded configuration and connected to local environment.' },
        { id: 'init-2', title: 'Ready for Instructions', status: 'success', details: 'Awaiting your commands to orchestrate sub-agents or query the RAG database.' }
      ]
    },
  ];
  const pendingAgentIntro =
    !currentConversation && workflow === 'AGENT'
      ? createAgentIntroMessage(agentRole)
      : null;
  const messages = pendingAgentIntro && !hasAgentIntroMessage(fallbackMessages, agentRole)
    ? [...fallbackMessages, pendingAgentIntro]
    : fallbackMessages;
  const shouldHideIntroductoryMessages = input.trim().length > 0 && !messages.some((message) => message.role === 'user');
  const visibleMessages = shouldHideIntroductoryMessages
    ? messages.filter((message) => !isIntroductoryAssistantMessage(message))
    : messages;
  const thinkingMessages = messages.filter(
    (message) =>
      message.role === 'assistant' &&
      !isDefaultWelcomeMessage(message) &&
      !isAgentIntroMessage(message) &&
      Array.isArray(message.steps) &&
      message.steps.length > 0
  );
  const lastAssistantMessage = [...visibleMessages]
    .reverse()
    .find((message) => message.role === 'assistant') ?? null;
  const lastAssistantMessageId = lastAssistantMessage?.id ?? null;
  const lastAssistantMessageAnchorKey = lastAssistantMessage
    ? `${lastAssistantMessage.id}:${lastAssistantMessage.timestamp}`
    : "";
  const latestUserMessage = [...messages].reverse().find((message) => message.role === 'user') ?? null;
  const latestMentionTargets = useMemo(
    () => extractMentionTargets(latestUserMessage?.content ?? ''),
    [latestUserMessage?.content]
  );
  const [isPlanningModalOpen, setIsPlanningModalOpen] = useState(false);
  const [isPlanningMonitorOpen, setIsPlanningMonitorOpen] = useState(false);
  const [planningState, setPlanningState] = useState<PlanningBackendState>(() => normalizePlanningBackendState(undefined, browserTimeZone));
  const [plannerDraft, setPlannerDraft] = useState<CrewPlanDraft>(() => normalizeCrewPlanDraft(planningAgentState.draft, browserTimeZone));
  const [editingPlanningPlanId, setEditingPlanningPlanId] = useState<string | null>(null);
  const [isPlanningDraftDirty, setIsPlanningDraftDirty] = useState(false);
  const [planningBusy, setPlanningBusy] = useState(false);
  const [isSqlDraftModalOpen, setIsSqlDraftModalOpen] = useState(false);
  const [selectedSqlDraft, setSelectedSqlDraft] = useState<DraftArtifact | null>(null);
  const [sqlDraftText, setSqlDraftText] = useState("");
  const [sqlDraftEngine, setSqlDraftEngine] = useState<"clickhouse" | "oracle">("clickhouse");
  const [sqlDraftRowLimit, setSqlDraftRowLimit] = useState(1000);
  const [sqlDraftResult, setSqlDraftResult] = useState<SqlDraftPreviewResult | null>(null);
  const [sqlDraftError, setSqlDraftError] = useState("");
  const [isSqlDraftRunning, setIsSqlDraftRunning] = useState(false);
  const [planningError, setPlanningError] = useState<string | null>(null);
  const [isToolsIslandOpen, setIsToolsIslandOpen] = useState(false);
  const [isAgentMenuExpanded, setIsAgentMenuExpanded] = useState(workflow === 'AGENT');
  const [isMcpMenuExpanded, setIsMcpMenuExpanded] = useState(workflow === 'MCP');
  const [isOtherAgentsOpen, setIsOtherAgentsOpen] = useState(agentRole === 'clickhouse_query' || agentRole === 'data_analyst' || agentRole === 'file_management' || agentRole === 'pdf_creator' || agentRole === 'oracle_analyst' || agentRole === 'data_quality_tables');
  const [isFileManagerConfigOpen, setIsFileManagerConfigOpen] = useState(false);
  const [isDataQualityModalOpen, setIsDataQualityModalOpen] = useState(false);
  const [isDataQualityMetadataLoading, setIsDataQualityMetadataLoading] = useState(false);
  const [dataQualityFormError, setDataQualityFormError] = useState<string | null>(null);
  const [dataQualityForm, setDataQualityForm] = useState<DataQualityFormState>(() => buildDataQualityFormState(dataQualityAgentState));
  const [dataQualityTables, setDataQualityTables] = useState<string[]>(dataQualityAgentState.availableTables);
  const [dataQualitySchema, setDataQualitySchema] = useState<DataQualitySchemaColumn[]>(dataQualityAgentState.schemaInfo);
  const [isConsoleOpen, setIsConsoleOpen] = useState(false);
  const persistedPlanningDraftKey = JSON.stringify((currentConversation?.agentState as any)?.planning?.draft ?? null);

  // --- ACTIONS ---

  const collapseToolsIsland = () => {
    setIsToolsIslandOpen(false);
    setIsAgentMenuExpanded(false);
    setIsMcpMenuExpanded(false);
    setIsOtherAgentsOpen(false);
  };

  const resetChatShell = () => {
    activeRequestControllerRef.current?.abort();
    activeRequestControllerRef.current = null;
    agentIntroBootstrapRef.current = null;
    dataQualityAutoOpenRef.current = false;
    onCurrentIdChange(null);
    setInput("");
    setAttachments([]);
    setIsLoading(false);
    setIsThinkingPanelOpen(false);
    setIsPlanningModalOpen(false);
    setIsPlanningMonitorOpen(false);
    setIsFileManagerConfigOpen(false);
    setIsDataQualityModalOpen(false);
    setDataQualityFormError(null);
    setIsConsoleOpen(false);
    setIsAgentStatePanelOpen(false);
  };

  // Start a completely new chat session
  const createNewChat = () => {
    resetChatShell();
  };

  const handleWorkflowSelection = (nextWorkflow: WorkflowMode) => {
    if (workflow === nextWorkflow) {
      setIsToolsIslandOpen(true);
      if (nextWorkflow === 'AGENT') {
        const nextExpanded = !isAgentMenuExpanded;
        setIsAgentMenuExpanded(nextExpanded);
        setIsMcpMenuExpanded(false);
        if (!nextExpanded) {
          setIsOtherAgentsOpen(false);
        } else if (agentRole !== 'manager') {
          setIsOtherAgentsOpen(true);
        }
      } else if (nextWorkflow === 'MCP') {
        setIsMcpMenuExpanded((open) => !open);
        setIsAgentMenuExpanded(false);
        setIsOtherAgentsOpen(false);
      } else {
        setIsAgentMenuExpanded(false);
        setIsMcpMenuExpanded(false);
        setIsOtherAgentsOpen(false);
      }
      return;
    }
    resetChatShell();
    setIsToolsIslandOpen(true);
    if (nextWorkflow === 'AGENT') {
      setIsAgentMenuExpanded(true);
      setIsMcpMenuExpanded(false);
      setIsOtherAgentsOpen(agentRole !== 'manager');
    } else if (nextWorkflow === 'MCP') {
      setIsAgentMenuExpanded(false);
      setIsMcpMenuExpanded(true);
      setIsOtherAgentsOpen(false);
    } else {
      setIsAgentMenuExpanded(false);
      setIsMcpMenuExpanded(false);
      setIsOtherAgentsOpen(false);
    }
    onWorkflowChange(nextWorkflow);
  };

  const handleAgentRoleSelection = (nextRole: AgentRole) => {
    if (workflow === 'AGENT' && agentRole === nextRole) {
      setIsToolsIslandOpen(true);
      setIsAgentMenuExpanded(true);
      setIsOtherAgentsOpen(nextRole !== 'manager');
      return;
    }
    resetChatShell();
    setIsToolsIslandOpen(true);
    setIsAgentMenuExpanded(true);
    setIsMcpMenuExpanded(false);
    setIsOtherAgentsOpen(nextRole !== 'manager');
    onAgentRoleChange(nextRole);
  };

  const handleMcpToolSelection = (nextToolId: string) => {
    if (mcpToolId === nextToolId) {
      setIsToolsIslandOpen(true);
      setIsMcpMenuExpanded(true);
      return;
    }
    resetChatShell();
    setIsToolsIslandOpen(true);
    setIsAgentMenuExpanded(false);
    setIsMcpMenuExpanded(true);
    setIsOtherAgentsOpen(false);
    onMcpToolIdChange(nextToolId);
  };

  // Reset the current chat to its initial state (welcome message only)
  const clearCurrentChat = () => {
    if (!currentId) return;
    onConversationsChange(prev => prev.map(c => 
      c.id === currentId 
        ? { ...c, messages: [{
            id: Date.now().toString(),
            role: "assistant",
            content: WELCOME_MESSAGE_CONTENT,
            timestamp: Date.now(),
            steps: [
              { id: 'init-1', title: 'System Initialization', status: 'success', details: 'Loaded configuration and connected to local environment.' },
              { id: 'init-2', title: 'Ready for Instructions', status: 'success', details: 'Awaiting your commands to orchestrate sub-agents or query the RAG database.' }
            ]
          }], memory: buildConversationMemory([{
            id: Date.now().toString(),
            role: "assistant",
            content: WELCOME_MESSAGE_CONTENT,
            timestamp: Date.now(),
          }]), updatedAt: Date.now(), agentState: undefined } 
        : c
    ));
  };

  // Delete a specific conversation from history
  const deleteConversation = (e: React.MouseEvent, id: string) => {
    e.stopPropagation();
    const updated = conversations.filter(c => c.id !== id);
    onConversationsChange(updated);
    if (currentId === id) {
      onCurrentIdChange(updated.length > 0 ? updated[0].id : null);
    }
  };

  useLayoutEffect(() => {
    if (isLoading || !lastAssistantMessageId) return;
    const frame = window.requestAnimationFrame(() => {
      const container = chatScrollContainerRef.current;
      const target = lastAssistantMessageRef.current;
      if (!container || !target) return;

      const containerRect = container.getBoundingClientRect();
      const targetRect = target.getBoundingClientRect();
      const scale = chatZoom || 1;
      const nextTop = container.scrollTop + ((targetRect.top - containerRect.top) / scale) - 8;

      container.scrollTo({
        top: Math.max(0, nextTop),
        behavior: "smooth",
      });
    });
    return () => window.cancelAnimationFrame(frame);
  }, [chatZoom, lastAssistantMessageAnchorKey, isLoading]);

  useEffect(() => {
    if (!isZoomControlOpen && !isThinkingPanelOpen) return;

    const handlePointerDown = (event: MouseEvent) => {
      if (!zoomControlRef.current?.contains(event.target as Node)) {
        setIsZoomControlOpen(false);
        setIsThinkingPanelOpen(false);
      }
    };

    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setIsZoomControlOpen(false);
        setIsThinkingPanelOpen(false);
      }
    };

    document.addEventListener('mousedown', handlePointerDown);
    document.addEventListener('keydown', handleEscape);
    return () => {
      document.removeEventListener('mousedown', handlePointerDown);
      document.removeEventListener('keydown', handleEscape);
    };
  }, [isThinkingPanelOpen, isZoomControlOpen]);

  useEffect(() => {
    if ((config.mcpTools ?? []).length === 0) {
      if (mcpToolId) onMcpToolIdChange('');
      return;
    }

    const currentToolExists = (config.mcpTools ?? []).some((tool: McpTool) => tool.id === mcpToolId);
    if (!currentToolExists) {
      onMcpToolIdChange(config.mcpTools[0]?.id ?? '');
    }
  }, [config.mcpTools, mcpToolId, onMcpToolIdChange]);

  useEffect(() => {
    if (editingPlanningPlanId) return;
    if (isPlanningModalOpen && isPlanningDraftDirty) return;
    setPlannerDraft(normalizeCrewPlanDraft(planningAgentState.draft, browserTimeZone));
  }, [persistedPlanningDraftKey, browserTimeZone, editingPlanningPlanId, isPlanningDraftDirty, isPlanningModalOpen]);

  const updatePlanningConversationState = (nextPlanningState: unknown) => {
    const normalizedState = normalizePlanningAgentState(nextPlanningState as any, browserTimeZone);
    const nextTitle = normalizedState.draft.name.trim() || 'LangGraph Planning';
    if (!currentId) {
      const conversationId = Date.now().toString();
      const newConversation: Conversation = {
        id: conversationId,
        title: nextTitle,
        messages: [],
        memory: buildConversationMemory([]),
        updatedAt: Date.now(),
        agentState: {
          planning: normalizedState,
        },
      };
      onConversationsChange((prev) => [newConversation, ...prev]);
      onCurrentIdChange(conversationId);
      return;
    }

    onConversationsChange(prev => prev.map((conversation) =>
      conversation.id === currentId
        ? {
            ...conversation,
            title: nextTitle || conversation.title,
            agentState: {
              ...(conversation.agentState ?? {}),
              planning: normalizedState,
            },
            updatedAt: Date.now(),
          }
        : conversation
    ));
  };

  useEffect(() => {
    if (!isPlanningModalOpen || !isPlanningDraftDirty) return;

    const timeoutId = window.setTimeout(() => {
      updatePlanningConversationState({
        draft: plannerDraft,
        missing_fields: planningAgentState.missingFields,
        last_question: planningAgentState.lastQuestion,
        ready_to_review: planningAgentState.readyToReview,
      });
    }, 220);

    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [
    isPlanningModalOpen,
    isPlanningDraftDirty,
    plannerDraft,
    planningAgentState.lastQuestion,
    planningAgentState.readyToReview,
    planningAgentState.missingFields,
  ]);

  const loadPlanningState = async () => {
    setPlanningBusy(true);
    try {
      const response = await fetch('/api/planning/state');
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `Planning state error: ${response.status}`);
      }
      const data = await response.json();
      setPlanningState(normalizePlanningBackendState(data, browserTimeZone));
      setPlanningError(null);
    } catch (error) {
      setPlanningError(error instanceof Error ? error.message : 'Unable to load planning state.');
    } finally {
      setPlanningBusy(false);
    }
  };

  useEffect(() => {
    if (workflow === 'CREWAI' || isPlanningModalOpen || isPlanningMonitorOpen) {
      void loadPlanningState();
    }
  }, [workflow, isPlanningModalOpen, isPlanningMonitorOpen]);

  useEffect(() => {
    if (workflow === 'AGENT' && isAgentMenuExpanded && (agentRole === 'clickhouse_query' || agentRole === 'data_analyst' || agentRole === 'file_management' || agentRole === 'pdf_creator' || agentRole === 'oracle_analyst' || agentRole === 'data_quality_tables')) {
      setIsOtherAgentsOpen(true);
    }
  }, [workflow, agentRole, isAgentMenuExpanded]);

  useEffect(() => {
    if (workflow !== 'AGENT') {
      setIsAgentMenuExpanded(false);
      setIsOtherAgentsOpen(false);
    }
    if (workflow !== 'MCP') {
      setIsMcpMenuExpanded(false);
    }
    if (workflow !== 'AGENT' && workflow !== 'MCP' && workflow !== 'LLM' && workflow !== 'RAG' && workflow !== 'CREWAI') {
      setIsToolsIslandOpen(false);
    }
  }, [workflow]);

  useEffect(() => {
    if (workflow !== 'AGENT' || !currentConversation || isLoading) return;
    if (agentRole !== 'manager' && agentRole !== 'clickhouse_query' && agentRole !== 'data_analyst' && agentRole !== 'file_management' && agentRole !== 'pdf_creator' && agentRole !== 'oracle_analyst' && agentRole !== 'data_quality_tables') return;
    if (hasMeaningfulConversationMessages(currentConversation.messages)) return;
    if (hasAgentIntroMessage(currentConversation.messages, agentRole)) return;

    const bootstrapKey = `${currentConversation.id}:${agentRole}:${currentConversation.updatedAt}`;
    if (agentIntroBootstrapRef.current === bootstrapKey) return;
    agentIntroBootstrapRef.current = bootstrapKey;

    const introMessage = createAgentIntroMessage(agentRole);
    if (!introMessage) return;

    onConversationsChange((prev) =>
      prev.map((conversation) =>
        conversation.id === currentConversation.id
          ? {
              ...conversation,
              messages: [...conversation.messages, introMessage],
              memory: buildConversationMemory([...conversation.messages, introMessage]),
              updatedAt: Date.now(),
            }
          : conversation
      )
    );
  }, [workflow, agentRole, currentConversation, isLoading, onConversationsChange]);

  useEffect(() => {
    if (isDataQualityModalOpen) {
      return;
    }
    setDataQualityForm(buildDataQualityFormState(dataQualityAgentState));
    setDataQualityTables(dataQualityAgentState.availableTables);
    setDataQualitySchema(dataQualityAgentState.schemaInfo);
  }, [dataQualityAgentState, isDataQualityModalOpen]);

  const loadDataQualityMetadata = async (nextTable?: string, preferredColumns?: string[]) => {
    const requestId = ++dataQualityMetadataRequestRef.current;
    const normalizedTable = String(nextTable ?? '').trim();
    setIsDataQualityMetadataLoading(true);
    setDataQualityFormError(null);
    try {
      const response = await fetch('/api/data-quality/options', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          table: normalizedTable || undefined,
          clickhouse: {
            host: config.clickhouseHost,
            port: config.clickhousePort,
            database: config.clickhouseDatabase,
            username: config.clickhouseUsername,
            password: config.clickhousePassword,
            secure: config.clickhouseSecure,
            verify_ssl: config.disableSslVerification ? false : (config.clickhouseVerifySsl ?? true),
            http_path: config.clickhouseHttpPath,
            query_limit: config.clickhouseQueryLimit,
          },
        }),
      });

      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `Data quality metadata error: ${response.status}`);
      }

      const data = await response.json();
      if (requestId !== dataQualityMetadataRequestRef.current) return;

      const normalizedState = normalizeDataQualityState({
        table: normalizedTable || null,
        schemaInfo: data.schema_info,
        availableTables: data.available_tables,
        availableColumns: data.available_columns,
        dateColumns: data.date_columns,
      });

      setDataQualityTables(normalizedState.availableTables);
      setDataQualitySchema(normalizedState.schemaInfo);
      setDataQualityForm((prev) => {
        const explicitColumnsProvided = Array.isArray(preferredColumns);
        const preservedColumns = keepColumnsInSchema(preferredColumns ?? prev.columns, normalizedState.schemaInfo);
        const nextColumns = normalizedTable
          ? (
              preservedColumns.length > 0
                ? preservedColumns
                : explicitColumnsProvided
                  ? []
                  : normalizedState.schemaInfo.map((column) => column.name)
            )
          : [];
        const nextTimeColumn =
          prev.timeColumn && normalizedState.dateColumns.includes(prev.timeColumn)
            ? prev.timeColumn
            : null;

        return {
          ...prev,
          table: normalizedTable,
          columns: nextColumns,
          timeColumn: nextTimeColumn,
        };
      });
    } catch (error) {
      if (requestId !== dataQualityMetadataRequestRef.current) return;
      setDataQualityTables([]);
      setDataQualitySchema([]);
      setDataQualityForm((prev) => ({
        ...prev,
        table: normalizedTable,
        columns: [],
        timeColumn: null,
      }));
      setDataQualityFormError(error instanceof Error ? error.message : 'Unable to load data-quality metadata.');
    } finally {
      if (requestId === dataQualityMetadataRequestRef.current) {
        setIsDataQualityMetadataLoading(false);
      }
    }
  };

  const openDataQualityModal = async () => {
    setIsDataQualityModalOpen(true);
    setDataQualityFormError(null);
    await loadDataQualityMetadata(dataQualityForm.table, dataQualityForm.columns);
  };

  useEffect(() => {
    if (workflow === 'AGENT' && agentRole === 'data_quality_tables') {
      if (isLoading) {
        return;
      }
      if (!dataQualityAutoOpenRef.current) {
        dataQualityAutoOpenRef.current = true;
        void openDataQualityModal();
      }
      return;
    }

    dataQualityAutoOpenRef.current = false;
    setIsDataQualityModalOpen(false);
    setDataQualityFormError(null);
  }, [
    workflow,
    agentRole,
    isLoading,
  ]);

  const handleDataQualityRun = async () => {
    const table = dataQualityForm.table.trim();
    const columns = keepColumnsInSchema(dataQualityForm.columns, dataQualitySchema);
    if (!table) {
      setDataQualityFormError('Choose a table before running the analysis.');
      return;
    }
    if (columns.length === 0) {
      setDataQualityFormError('Select at least one column to profile.');
      return;
    }

    const selectedTimeColumn =
      dataQualityForm.timeColumn && dataQualitySchema.some((column) => column.name === dataQualityForm.timeColumn && column.category === 'date')
        ? dataQualityForm.timeColumn
        : null;
    const payload = {
      __dq__: true,
      table,
      columns,
      sample_size: dataQualityForm.sampleSize,
      ...(dataQualityForm.rowFilter.trim() ? { row_filter: dataQualityForm.rowFilter.trim() } : {}),
      ...(selectedTimeColumn ? { time_column: selectedTimeColumn } : {}),
    };
    const userMsg: Message = {
      id: Date.now().toString(),
      role: "user",
      content: summarizeDataQualityRun({
        ...dataQualityForm,
        table,
        columns,
        timeColumn: selectedTimeColumn,
      }),
      timestamp: Date.now(),
    };
    const baseMessages = currentConversation?.messages ?? messages;
    const nextMessages = [...baseMessages, userMsg];

    setDataQualityFormError(null);
    setIsLoading(true);
    const controller = new AbortController();
    activeRequestControllerRef.current = controller;

    try {
      const response = await fetch('/api/chat/data-quality-agent', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        signal: controller.signal,
        body: JSON.stringify({
          message: JSON.stringify(payload, null, 2),
          history: buildConversationMemory(nextMessages).steps.map((step) => ({
            role: step.role,
            content: step.content,
          })),
          agent_state: dataQualityAgentState ?? undefined,
          clickhouse: {
            host: config.clickhouseHost,
            port: config.clickhousePort,
            database: config.clickhouseDatabase,
            username: config.clickhouseUsername,
            password: config.clickhousePassword,
            secure: config.clickhouseSecure,
            verify_ssl: config.disableSslVerification ? false : (config.clickhouseVerifySsl ?? true),
            http_path: config.clickhouseHttpPath,
            query_limit: config.clickhouseQueryLimit,
          },
          llm_base_url: config.baseUrl,
          llm_model: config.model,
          llm_api_key: config.apiKey || undefined,
          llm_provider: config.provider,
          disable_ssl_verification: config.disableSslVerification ?? false,
        }),
      });

      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `Data Quality Agent error: ${response.status}`);
      }

      const data = await response.json();
      const nextState = normalizeDataQualityState(data.agent_state);
      const assistantDataQualityMsg: Message = {
        id: (Date.now() + 1).toString(),
        role: "assistant",
        content: data.answer,
        timestamp: Date.now(),
        steps: data.steps,
        actions: data.actions,
      };
      const conversationMessages = [...nextMessages, assistantDataQualityMsg];

      if (!currentId) {
        const conversationId = Date.now().toString();
        const newConversation: Conversation = {
          id: conversationId,
          title: `Data quality · ${table}`,
          messages: conversationMessages,
          memory: buildConversationMemory(conversationMessages),
          updatedAt: Date.now(),
          agentState: {
            dataQuality: nextState,
          },
        };
        onConversationsChange((prev) => [newConversation, ...prev]);
        onCurrentIdChange(conversationId);
      } else {
        onConversationsChange((prev) => {
          const idx = prev.findIndex((conversation) => conversation.id === currentId);
          if (idx === -1) return prev;
          const updated = [...prev];
          updated[idx] = {
            ...updated[idx],
            messages: conversationMessages,
            memory: buildConversationMemory(conversationMessages),
            updatedAt: Date.now(),
            agentState: {
              ...(updated[idx].agentState ?? {}),
              dataQuality: nextState,
            },
          };
          const [conversation] = updated.splice(idx, 1);
          updated.unshift(conversation);
          return updated;
        });
      }

      setIsConnected(true);
      setDataQualityForm(buildDataQualityFormState(nextState));
      setDataQualityTables(nextState.availableTables);
      setDataQualitySchema(nextState.schemaInfo);
      setIsDataQualityModalOpen(false);
    } catch (error) {
      if ((error as any)?.name === 'AbortError') {
        setIsConnected(true);
        setDataQualityFormError('Run stopped.');
        return;
      }
      setIsConnected(false);
      setDataQualityFormError(error instanceof Error ? error.message : 'Unable to run the data-quality analysis.');
    } finally {
      if (activeRequestControllerRef.current === controller) {
        activeRequestControllerRef.current = null;
      }
      setIsLoading(false);
    }
  };

  const openPlanningModal = (nextDraft?: Partial<CrewPlanDraft> | null) => {
    if (nextDraft) {
      setPlannerDraft(normalizeCrewPlanDraft(nextDraft, browserTimeZone));
      setEditingPlanningPlanId(null);
    }
    setIsPlanningDraftDirty(false);
    setPlanningError(null);
    setIsPlanningModalOpen(true);
    void loadPlanningState();
  };

  const openPlanningMonitor = () => {
    setPlanningError(null);
    setIsPlanningMonitorOpen(true);
    void loadPlanningState();
  };

  const openSqlDraftPreview = (artifact: DraftArtifact) => {
    const defaultEngine = artifact.engineHint ?? (agentRole === 'oracle_analyst' ? 'oracle' : 'clickhouse');
    setSelectedSqlDraft(artifact);
    setSqlDraftText(artifact.content);
    setSqlDraftEngine(defaultEngine);
    setSqlDraftRowLimit(1000);
    setSqlDraftResult(null);
    setSqlDraftError('');
    setIsSqlDraftModalOpen(true);
  };

  const closeSqlDraftPreview = () => {
    setIsSqlDraftModalOpen(false);
    setSelectedSqlDraft(null);
    setSqlDraftText('');
    setSqlDraftResult(null);
    setSqlDraftError('');
    setIsSqlDraftRunning(false);
  };

  const runSqlDraftPreview = async () => {
    if (!sqlDraftText.trim()) {
      setSqlDraftError('Enter or confirm a SQL draft before generating the table.');
      return;
    }

    const disableSslVerification = config.disableSslVerification ?? false;
    const effectiveClickhouseVerifySsl = disableSslVerification ? false : (config.clickhouseVerifySsl ?? true);
    setIsSqlDraftRunning(true);
    setSqlDraftError('');

    try {
      const response = await fetch('/api/sql-draft/preview', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: sqlDraftText,
          engine: sqlDraftEngine,
          row_limit: sqlDraftRowLimit,
          clickhouse: {
            host: config.clickhouseHost,
            port: config.clickhousePort,
            database: config.clickhouseDatabase,
            username: config.clickhouseUsername,
            password: config.clickhousePassword,
            secure: config.clickhouseSecure,
            verify_ssl: effectiveClickhouseVerifySsl,
            http_path: config.clickhouseHttpPath,
            query_limit: Math.max(sqlDraftRowLimit, config.clickhouseQueryLimit),
          },
          oracle_connections: (config.oracleConnections ?? []).map((connection) => ({
            id: connection.id,
            label: connection.label,
            host: connection.host,
            port: connection.port,
            service_name: connection.serviceName,
            sid: connection.sid,
            dsn: connection.dsn,
            username: connection.username,
            password: connection.password,
          })),
          oracle_analyst_config: {
            connection_id: config.oracleAnalystConfig.connectionId,
            row_limit: Math.max(sqlDraftRowLimit, config.oracleAnalystConfig.rowLimit),
            max_retries: config.oracleAnalystConfig.maxRetries,
            max_iterations: config.oracleAnalystConfig.maxIterations,
            toolkit_id: config.oracleAnalystConfig.toolkitId,
            system_prompt: config.oracleAnalystConfig.systemPrompt,
          },
        }),
      });

      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `SQL preview error: ${response.status}`);
      }

      const data = await response.json();
      setSqlDraftResult({
        engine: data.engine === 'oracle' ? 'oracle' : 'clickhouse',
        executedSql: String(data.executed_sql || sqlDraftText),
        columns: Array.isArray(data.columns) ? data.columns.map((column: unknown) => String(column)) : [],
        rows: Array.isArray(data.rows) ? data.rows : [],
        rowCount: Number(data.row_count || 0),
        shownRows: Number(data.shown_rows || 0),
        rowLimit: Number(data.row_limit || sqlDraftRowLimit),
      });
    } catch (error) {
      setSqlDraftError(error instanceof Error ? error.message : 'Unable to generate the SQL preview.');
    } finally {
      setIsSqlDraftRunning(false);
    }
  };

  const startNewPlanningDraft = () => {
    const emptyDraft = createEmptyCrewPlanDraft(browserTimeZone);
    setEditingPlanningPlanId(null);
    setPlannerDraft(emptyDraft);
    setIsPlanningDraftDirty(false);
    updatePlanningConversationState({
      draft: emptyDraft,
      missing_fields: [],
      last_question: '',
      ready_to_review: false,
    });
  };

  const savePlanningPlan = async (draft: CrewPlanDraft, editingPlanId: string | null) => {
    setPlanningBusy(true);
    try {
      const response = await fetch('/api/planning/plans', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          plan: {
            ...draft,
            id: editingPlanId || undefined,
          },
        }),
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `Planning save error: ${response.status}`);
      }
      const data = await response.json();
      setPlanningState(normalizePlanningBackendState(data, browserTimeZone));
      setPlanningError(null);
      setEditingPlanningPlanId(null);
      setIsPlanningDraftDirty(false);
      const emptyDraft = createEmptyCrewPlanDraft(browserTimeZone);
      setPlannerDraft(emptyDraft);
      updatePlanningConversationState({
        draft: emptyDraft,
        missing_fields: [],
        last_question: '',
        ready_to_review: false,
      });
    } catch (error) {
      setPlanningError(error instanceof Error ? error.message : 'Unable to save the planning job.');
      throw error;
    } finally {
      setPlanningBusy(false);
    }
  };

  const editPlanningPlan = (plan: CrewPlan) => {
    setEditingPlanningPlanId(plan.id);
    setPlannerDraft(normalizeCrewPlanDraft(plan, browserTimeZone));
    setIsPlanningDraftDirty(false);
    setPlanningError(null);
    setIsPlanningModalOpen(true);
  };

  const togglePlanningPlanStatus = async (plan: CrewPlan) => {
    setPlanningBusy(true);
    try {
      const response = await fetch(`/api/planning/plans/${plan.id}/status`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          status: plan.status === 'active' ? 'paused' : 'active',
        }),
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `Planning status error: ${response.status}`);
      }
      const data = await response.json();
      setPlanningState(normalizePlanningBackendState(data, browserTimeZone));
      setPlanningError(null);
    } catch (error) {
      setPlanningError(error instanceof Error ? error.message : 'Unable to update the planning status.');
    } finally {
      setPlanningBusy(false);
    }
  };

  const deletePlanningPlan = async (planId: string) => {
    setPlanningBusy(true);
    try {
      const response = await fetch(`/api/planning/plans/${planId}`, {
        method: 'DELETE',
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `Planning delete error: ${response.status}`);
      }
      const data = await response.json();
      setPlanningState(normalizePlanningBackendState(data, browserTimeZone));
      setPlanningError(null);
      if (editingPlanningPlanId === planId) {
        setEditingPlanningPlanId(null);
        setPlannerDraft(createEmptyCrewPlanDraft(browserTimeZone));
      }
    } catch (error) {
      setPlanningError(error instanceof Error ? error.message : 'Unable to delete the planning job.');
    } finally {
      setPlanningBusy(false);
    }
  };

  const runPlanningPlan = async (planId: string) => {
    setPlanningBusy(true);
    try {
      const response = await fetch(`/api/planning/plans/${planId}/run`, {
        method: 'POST',
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `Planning run error: ${response.status}`);
      }
      const data = await response.json();
      setPlanningState(normalizePlanningBackendState(data, browserTimeZone));
      setPlanningError(null);
    } catch (error) {
      setPlanningError(error instanceof Error ? error.message : 'Unable to run the planning job.');
    } finally {
      setPlanningBusy(false);
    }
  };

  const handleChatAction = (action: ChatAction, message: Message) => {
    if (action.actionType === 'open_planning_form') {
      openPlanningModal(planningAgentState.draft);
      return;
    }
    if (action.actionType === 'edit_planning_plan') {
      const planId = String(action.payload?.planId || '');
      const existingPlan = planningState.plans.find((plan) => plan.id === planId);
      if (existingPlan) {
        editPlanningPlan(existingPlan);
      } else {
        openPlanningModal(planningAgentState.draft);
      }
      return;
    }
    if (action.actionType === 'refresh_planning_state') {
      void loadPlanningState();
      return;
    }
    if (action.actionType === 'confirm_file_action') {
      void handleSend('confirm');
      return;
    }
    if (action.actionType === 'cancel_file_action') {
      void handleSend('cancel');
      return;
    }
    if (action.actionType === 'export_data_quality_pdf') {
      const fileName = String(action.payload?.fileName || 'data-quality-summary.pdf');
      const title = String(action.payload?.title || 'Data Quality Summary');
      downloadMarkdownPdf(message.content, { fileName, title });
    }
  };

  // Handle file selection for attachments
  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || []);
    files.forEach(file => {
      const reader = new FileReader();
      reader.onload = (event) => {
        const base64 = event.target?.result as string;
        setAttachments(prev => [...prev, {
          id: Math.random().toString(36).substring(7),
          name: file.name,
          type: file.type,
          data: base64
        }]);
      };
      reader.readAsDataURL(file);
    });
    if (fileInputRef.current) fileInputRef.current.value = '';
  };

  // Remove an attachment before sending
  const removeAttachment = (id: string) => {
    setAttachments(prev => prev.filter(a => a.id !== id));
  };

  const handleCopyInput = () => {
    if (!input.trim()) return;
    navigator.clipboard.writeText(input).then(() => {
      setIsInputCopied(true);
      window.setTimeout(() => setIsInputCopied(false), 1800);
    });
  };

  const stopCurrentExecution = () => {
    activeRequestControllerRef.current?.abort();
    activeRequestControllerRef.current = null;
    setIsLoading(false);
  };

  const handleMentionSelect = (target: MentionTargetDefinition) => {
    const cursor = textareaRef.current?.selectionStart ?? inputCursor ?? input.length;
    const nextValue = replaceMentionToken(input, cursor, `@${target.aliases[0]}`);
    setInput(nextValue);
    window.requestAnimationFrame(() => {
      if (!textareaRef.current) return;
      const nextCursor = nextValue.lastIndexOf(`@${target.aliases[0]}`) + target.aliases[0].length + 2;
      textareaRef.current.focus();
      textareaRef.current.selectionStart = nextCursor;
      textareaRef.current.selectionEnd = nextCursor;
      textareaRef.current.style.height = 'auto';
      textareaRef.current.style.height = `${Math.min(textareaRef.current.scrollHeight, 200)}px`;
      setInputCursor(nextCursor);
    });
  };

  // Auto-resize the textarea based on content
  const handleInputChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setInput(e.target.value);
    setInputCursor(e.target.selectionStart ?? e.target.value.length);
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
      textareaRef.current.style.height = `${Math.min(textareaRef.current.scrollHeight, 200)}px`;
    }
  };

  // Main function to handle sending a message
  const handleSend = async (text: string = input) => {
    const mentionedTargets = extractMentionTargets(text);
    const mentionedRoles = Array.from(
      new Set(
        mentionedTargets
          .map((target) => target.role)
          .filter((role): role is AgentRole => Boolean(role))
      )
    );
    const resolvedWorkflow: WorkflowMode = mentionedTargets.length > 0 ? 'AGENT' : workflow;
    const resolvedAgentRole: AgentRole =
      mentionedRoles.length === 1 && mentionedTargets.length === 1
        ? mentionedRoles[0]
        : mentionedTargets.length > 0
          ? 'manager'
          : agentRole;

    if (resolvedWorkflow === 'AGENT' && resolvedAgentRole === 'data_quality_tables') {
      void openDataQualityModal();
      return;
    }
    if ((!text.trim() && attachments.length === 0) || isLoading) return;

    // Construct the user message object
    const userMsg: Message = {
      id: Date.now().toString(),
      role: "user",
      content: text,
      timestamp: Date.now(),
      attachments: attachments.length > 0 ? attachments : undefined
    };

    let activeConvId = currentId;
    const persistedMessages = messages.filter((message) => !isIntroductoryAssistantMessage(message));
    let activeMessages = [...persistedMessages, userMsg];

    // Create a new conversation if one doesn't exist
    if (!activeConvId) {
      activeConvId = Date.now().toString();
      const newConv: Conversation = {
        id: activeConvId,
        title: text.slice(0, 30) + (text.length > 30 ? '...' : ''),
        messages: activeMessages,
        memory: buildConversationMemory(activeMessages),
        updatedAt: Date.now()
      };
      onConversationsChange(prev => [newConv, ...prev]);
      onCurrentIdChange(activeConvId);
    } else {
      // Update the existing conversation and move it to the top of the list
      onConversationsChange(prev => {
        const idx = prev.findIndex(c => c.id === activeConvId);
        if (idx === -1) return prev;
        const updated = [...prev];
        updated[idx] = {
          ...updated[idx],
          messages: activeMessages,
          memory: buildConversationMemory(activeMessages),
          updatedAt: Date.now()
        };
        const [conv] = updated.splice(idx, 1);
        updated.unshift(conv);
        return updated;
      });
    }

    // Reset input fields and UI state
    setInput("");
    setInputCursor(0);
    setAttachments([]);
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
    }
    setIsLoading(true);
    const controller = new AbortController();
    activeRequestControllerRef.current = controller;
    if (mentionedTargets.length > 0) {
      onWorkflowChange('AGENT');
      onAgentRoleChange(resolvedAgentRole);
    }

    try {
      const memoryHistory = buildConversationMemory(activeMessages).steps.map((step) => ({
        role: step.role,
        content: step.content,
      }));
      let reply = "";
      let sources = undefined;
      let confidence = undefined;
      let nextManagerAgentState = managerAgentState;
      let nextClickhouseAgentState = clickhouseAgentState;
      let nextDataAnalystAgentState = dataAnalystAgentState;
      let nextPlanningAgentState = planningAgentState;
      let nextFileManagerAgentState = fileManagerAgentState;
      let nextPdfCreatorAgentState = pdfCreatorAgentState;
      let nextOracleAnalystAgentState = oracleAnalystAgentState;
      let nextDataQualityAgentState = dataQualityAgentState;
      const disableSslVerification = config.disableSslVerification ?? false;
      const effectiveEmbeddingVerifySsl = disableSslVerification ? false : (config.embeddingVerifySsl ?? true);
      const effectiveClickhouseVerifySsl = disableSslVerification ? false : (config.clickhouseVerifySsl ?? true);
      const serializedDataAnalystState = {
        stage: dataAnalystAgentState.stage,
        pending_request: dataAnalystAgentState.pendingRequest,
        available_tables: dataAnalystAgentState.availableTables,
        selected_table: dataAnalystAgentState.selectedTable,
        table_schema: dataAnalystAgentState.tableSchema,
        clarification_prompt: dataAnalystAgentState.clarificationPrompt,
        clarification_options: dataAnalystAgentState.clarificationOptions,
        last_sqls: dataAnalystAgentState.lastSqls,
        last_result_meta: dataAnalystAgentState.lastResultMeta,
        last_result_rows: dataAnalystAgentState.lastResultRows,
        final_answer: dataAnalystAgentState.finalAnswer,
        last_error: dataAnalystAgentState.lastError,
        last_export_path: dataAnalystAgentState.lastExportPath,
        knowledge_hits: (dataAnalystAgentState.knowledgeHits ?? []).map((hit) => ({
          doc_name: hit.docName,
          text: hit.text,
          score: hit.score,
        })),
      };

      // Route the request based on the selected workflow
      if (resolvedWorkflow === 'LLM' && isAppCapabilitiesQuery(text)) {
        reply = buildAppCapabilitiesReply(text);
        setIsConnected(true);
      } else if (resolvedWorkflow === 'RAG') {
        // Call our full-stack RAG backend
        const response = await fetch('/api/chat/rag', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          signal: controller.signal,
          body: JSON.stringify({
            message: text,
            history: memoryHistory,
            opensearch: {
              url:      config.elasticsearchUrl,
              index:    config.elasticsearchIndex,
              username: config.elasticsearchUsername || undefined,
              password: config.elasticsearchPassword || undefined,
            },
            embedding_base_url:   config.embeddingBaseUrl,
            embedding_api_key:    config.embeddingApiKey  || undefined,
            embedding_model:      config.embeddingModel,
            embedding_verify_ssl: effectiveEmbeddingVerifySsl,
            knn_neighbors:        config.knnNeighbors,
            llm_base_url:       config.baseUrl,
            llm_model:          config.model,
            llm_api_key:        config.apiKey || undefined,
            llm_provider:       config.provider,
            disable_ssl_verification: disableSslVerification,
          })
        });

        if (!response.ok) throw new Error(`RAG Backend error! status: ${response.status}`);
        const data = await response.json();
        reply = data.answer;
        sources = data.sources;
        confidence = data.confidence;
        setIsConnected(true);
      } else if (resolvedWorkflow === 'MCP') {
        const activeTool = (config.mcpTools ?? []).find((t: McpTool) => t.id === mcpToolId);
        if (!activeTool?.url) {
          throw new Error("No MCP tool is selected or its URL is missing. Configure an MCP tool in Settings.");
        }
        const response = await fetch('/api/chat/mcp', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          signal: controller.signal,
          body: JSON.stringify({
            message: text,
            history: memoryHistory,
            mcp_url: activeTool.url,
            llm_base_url: config.baseUrl,
            llm_model: config.model,
            llm_api_key: config.apiKey || undefined,
            llm_provider: config.provider,
            system_prompt: formattedSystemPrompt,
            disable_ssl_verification: disableSslVerification,
          }),
        });
        if (!response.ok) {
          const err = await response.json().catch(() => ({}));
          throw new Error(err.detail || `MCP Backend error: ${response.status}`);
        }
        const data = await response.json();
        reply = data.answer;
        setIsConnected(true);

        const assistantMcpMsg: Message = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: reply,
          timestamp: Date.now(),
          steps: data.tool_calls?.length > 0
            ? data.tool_calls.map((tc: { tool: string; args: Record<string, unknown>; result: string }, i: number) => ({
                id: `mcp-${i}`,
                title: `Tool: ${tc.tool}`,
                status: 'success' as const,
                details: `Args: ${JSON.stringify(tc.args)}\n\nResult: ${tc.result}`,
              }))
            : undefined,
        };

        onConversationsChange(prev => {
          const idx = prev.findIndex(c => c.id === activeConvId);
          if (idx === -1) return prev;
          const updated = [...prev];
          const nextMessages = [...updated[idx].messages, assistantMcpMsg];
          updated[idx] = { ...updated[idx], messages: nextMessages, memory: buildConversationMemory(nextMessages), updatedAt: Date.now() };
          return updated;
        });
        setIsLoading(false);
        return;
      } else if (resolvedWorkflow === 'CREWAI') {
        const response = await fetch('/api/chat/crewai-planning', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          signal: controller.signal,
          body: JSON.stringify({
            message: text,
            history: memoryHistory,
            agent_state: nextPlanningAgentState ?? undefined,
            llm_base_url: config.baseUrl,
            llm_model: config.model,
            llm_api_key: config.apiKey || undefined,
            llm_provider: config.provider,
            disable_ssl_verification: disableSslVerification,
          }),
        });

        if (!response.ok) {
          const err = await response.json().catch(() => ({}));
          throw new Error(err.detail || `LangGraph Planning error: ${response.status}`);
        }

        const data = await response.json();
        reply = data.answer;
        nextPlanningAgentState = normalizePlanningAgentState(data.agent_state, browserTimeZone);
        setIsConnected(true);
        setPlannerDraft(nextPlanningAgentState.draft);
        setIsPlanningDraftDirty(false);
        setEditingPlanningPlanId(null);

        const assistantPlanningMsg: Message = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: reply,
          timestamp: Date.now(),
          steps: data.steps,
          actions: data.actions,
        };

        onConversationsChange(prev => {
          const idx = prev.findIndex(c => c.id === activeConvId);
          if (idx === -1) return prev;
          const updated = [...prev];
          updated[idx] = {
            ...updated[idx],
            agentState: {
              ...(updated[idx].agentState ?? {}),
              planning: nextPlanningAgentState,
            },
            messages: [...updated[idx].messages, assistantPlanningMsg],
            memory: buildConversationMemory([...updated[idx].messages, assistantPlanningMsg]),
            updatedAt: Date.now(),
          };
          return updated;
        });
        setIsLoading(false);
        return;
      } else if (resolvedWorkflow === 'AGENT' && resolvedAgentRole === 'manager') {
        const response = await fetch('/api/chat/manager-agent', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          signal: controller.signal,
          body: JSON.stringify({
            message: text,
            history: memoryHistory,
            manager_state: managerAgentState ?? undefined,
            clickhouse_state: clickhouseAgentState ?? undefined,
            data_analyst_state: serializedDataAnalystState,
            file_manager_state: fileManagerAgentState ?? undefined,
            pdf_creator_state: pdfCreatorAgentState ?? undefined,
            oracle_analyst_state: oracleAnalystAgentState ?? undefined,
            data_quality_state: dataQualityAgentState ?? undefined,
            clickhouse: {
              host: config.clickhouseHost,
              port: config.clickhousePort,
              database: config.clickhouseDatabase,
              username: config.clickhouseUsername,
              password: config.clickhousePassword,
              secure: config.clickhouseSecure,
              verify_ssl: effectiveClickhouseVerifySsl,
              http_path: config.clickhouseHttpPath,
              query_limit: config.clickhouseQueryLimit,
            },
            oracle_connections: (config.oracleConnections ?? []).map((connection) => ({
              id: connection.id,
              label: connection.label,
              host: connection.host,
              port: connection.port,
              service_name: connection.serviceName,
              sid: connection.sid,
              dsn: connection.dsn,
              username: connection.username,
              password: connection.password,
            })),
            oracle_analyst_config: {
              connection_id: config.oracleAnalystConfig.connectionId,
              row_limit: config.oracleAnalystConfig.rowLimit,
              max_retries: config.oracleAnalystConfig.maxRetries,
              max_iterations: config.oracleAnalystConfig.maxIterations,
              toolkit_id: config.oracleAnalystConfig.toolkitId,
              system_prompt: formattedOracleSystemPrompt,
            },
            file_manager_config: {
              base_path: config.fileManagerConfig.basePath,
              max_iterations: config.fileManagerConfig.maxIterations,
              system_prompt: formattedFileManagerSystemPrompt,
            },
            llm_base_url: config.baseUrl,
            llm_model: config.model,
            llm_api_key: config.apiKey || undefined,
            llm_provider: config.provider,
            system_prompt: formattedSystemPrompt,
            disable_ssl_verification: disableSslVerification,
          }),
        });

        if (!response.ok) {
          const err = await response.json().catch(() => ({}));
          throw new Error(err.detail || `Manager Agent error: ${response.status}`);
        }

        const data = await response.json();
        reply = data.answer;
        nextManagerAgentState = normalizeManagerAgentState((data.agent_state as any)?.manager);
        nextClickhouseAgentState = (data.agent_state as any)?.clickhouse ?? nextClickhouseAgentState;
        nextDataAnalystAgentState = normalizeDataAnalystAgentState((data.agent_state as any)?.dataAnalyst);
        nextFileManagerAgentState = normalizeFileManagerAgentState((data.agent_state as any)?.fileManager);
        nextPdfCreatorAgentState = normalizePdfCreatorAgentState((data.agent_state as any)?.pdfCreator);
        nextOracleAnalystAgentState = normalizeOracleAnalystAgentState((data.agent_state as any)?.oracleAnalyst);
        nextDataQualityAgentState = normalizeDataQualityState((data.agent_state as any)?.dataQuality);
        setIsConnected(true);

        const assistantManagerMsg: Message = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: reply,
          timestamp: Date.now(),
          steps: data.steps,
          actions: data.actions,
          chart: data.chart,
        };

        onConversationsChange(prev => {
          const idx = prev.findIndex(c => c.id === activeConvId);
          if (idx === -1) return prev;
          const updated = [...prev];
          updated[idx] = {
            ...updated[idx],
            agentState: {
              ...(updated[idx].agentState ?? {}),
              manager: nextManagerAgentState,
              clickhouse: nextClickhouseAgentState,
              dataAnalyst: nextDataAnalystAgentState,
              fileManager: nextFileManagerAgentState,
              pdfCreator: nextPdfCreatorAgentState,
              oracleAnalyst: nextOracleAnalystAgentState,
              dataQuality: nextDataQualityAgentState,
            },
            messages: [...updated[idx].messages, assistantManagerMsg],
            memory: buildConversationMemory([...updated[idx].messages, assistantManagerMsg]),
            updatedAt: Date.now(),
          };
          return updated;
        });
        setIsLoading(false);
        return;
      } else if (resolvedWorkflow === 'AGENT' && resolvedAgentRole === 'data_analyst') {
        const response = await fetch('/api/chat/data-analyst-agent', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          signal: controller.signal,
          body: JSON.stringify({
            message: text,
            history: memoryHistory,
            agent_state: serializedDataAnalystState,
            max_steps: 10,
            clickhouse: {
              host: config.clickhouseHost,
              port: config.clickhousePort,
              database: config.clickhouseDatabase,
              username: config.clickhouseUsername,
              password: config.clickhousePassword,
              secure: config.clickhouseSecure,
              verify_ssl: effectiveClickhouseVerifySsl,
              http_path: config.clickhouseHttpPath,
              query_limit: config.clickhouseQueryLimit,
            },
            llm_base_url: config.baseUrl,
            llm_model: config.model,
            llm_api_key: config.apiKey || undefined,
            llm_provider: config.provider,
            disable_ssl_verification: disableSslVerification,
          }),
        });

        if (!response.ok) {
          const err = await response.json().catch(() => ({}));
          throw new Error(err.detail || `Data Analyst Agent error: ${response.status}`);
        }

        const data = await response.json();
        reply = data.answer;
        nextDataAnalystAgentState = normalizeDataAnalystAgentState(data.agent_state);
        setIsConnected(true);

        const assistantDataAnalystMsg: Message = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: reply,
          timestamp: Date.now(),
          steps: data.steps,
        };

        onConversationsChange(prev => {
          const idx = prev.findIndex(c => c.id === activeConvId);
          if (idx === -1) return prev;
          const updated = [...prev];
          updated[idx] = {
            ...updated[idx],
            agentState: {
              ...(updated[idx].agentState ?? {}),
              dataAnalyst: nextDataAnalystAgentState,
            },
            messages: [...updated[idx].messages, assistantDataAnalystMsg],
            memory: buildConversationMemory([...updated[idx].messages, assistantDataAnalystMsg]),
            updatedAt: Date.now(),
          };
          return updated;
        });
        setIsLoading(false);
        return;
      } else if (resolvedWorkflow === 'AGENT' && resolvedAgentRole === 'clickhouse_query') {
        const response = await fetch('/api/chat/clickhouse-agent', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          signal: controller.signal,
          body: JSON.stringify({
            message: text,
            history: memoryHistory,
            agent_state: clickhouseAgentState ?? undefined,
            clickhouse: {
              host: config.clickhouseHost,
              port: config.clickhousePort,
              database: config.clickhouseDatabase,
              username: config.clickhouseUsername,
              password: config.clickhousePassword,
              secure: config.clickhouseSecure,
              verify_ssl: effectiveClickhouseVerifySsl,
              http_path: config.clickhouseHttpPath,
              query_limit: config.clickhouseQueryLimit,
            },
            llm_base_url: config.baseUrl,
            llm_model: config.model,
            llm_api_key: config.apiKey || undefined,
            llm_provider: config.provider,
            disable_ssl_verification: disableSslVerification,
          }),
        });

        if (!response.ok) {
          const err = await response.json().catch(() => ({}));
          throw new Error(err.detail || `ClickHouse Agent error: ${response.status}`);
        }

        const data = await response.json();
        reply = data.answer;
        nextClickhouseAgentState = data.agent_state;
        setIsConnected(true);

        const assistantClickHouseMsg: Message = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: reply,
          timestamp: Date.now(),
          steps: data.steps,
          chart: data.chart,
        };

        onConversationsChange(prev => {
          const idx = prev.findIndex(c => c.id === activeConvId);
          if (idx === -1) return prev;
          const updated = [...prev];
          updated[idx] = {
            ...updated[idx],
            agentState: {
              ...(updated[idx].agentState ?? {}),
              clickhouse: nextClickhouseAgentState,
            },
            messages: [...updated[idx].messages, assistantClickHouseMsg],
            memory: buildConversationMemory([...updated[idx].messages, assistantClickHouseMsg]),
            updatedAt: Date.now(),
          };
          return updated;
        });
        setIsLoading(false);
        return;
      } else if (resolvedWorkflow === 'AGENT' && resolvedAgentRole === 'file_management') {
        const response = await fetch('/api/chat/file-manager-agent', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          signal: controller.signal,
          body: JSON.stringify({
            message: text,
            history: memoryHistory,
            agent_state: fileManagerAgentState ?? undefined,
            file_manager_config: {
              base_path: config.fileManagerConfig.basePath,
              max_iterations: config.fileManagerConfig.maxIterations,
              system_prompt: formattedFileManagerSystemPrompt,
            },
            llm_base_url: config.baseUrl,
            llm_model: config.model,
            llm_api_key: config.apiKey || undefined,
            llm_provider: config.provider,
            disable_ssl_verification: disableSslVerification,
          }),
        });

        if (!response.ok) {
          const err = await response.json().catch(() => ({}));
          throw new Error(err.detail || `File Management Agent error: ${response.status}`);
        }

        const data = await response.json();
        reply = data.answer;
        nextFileManagerAgentState = normalizeFileManagerAgentState(data.agent_state);
        setIsConnected(true);

        const assistantFileManagerMsg: Message = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: reply,
          timestamp: Date.now(),
          steps: data.steps,
          actions: data.actions,
        };

        onConversationsChange(prev => {
          const idx = prev.findIndex(c => c.id === activeConvId);
          if (idx === -1) return prev;
          const updated = [...prev];
          updated[idx] = {
            ...updated[idx],
            agentState: {
              ...(updated[idx].agentState ?? {}),
              fileManager: nextFileManagerAgentState,
            },
            messages: [...updated[idx].messages, assistantFileManagerMsg],
            memory: buildConversationMemory([...updated[idx].messages, assistantFileManagerMsg]),
            updatedAt: Date.now(),
          };
          return updated;
        });
        setIsLoading(false);
        return;
      } else if (resolvedWorkflow === 'AGENT' && resolvedAgentRole === 'pdf_creator') {
        const response = await fetch('/api/chat/pdf-creator-agent', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          signal: controller.signal,
          body: JSON.stringify({
            message: text,
            history: memoryHistory,
            agent_state: pdfCreatorAgentState ?? undefined,
            file_manager_config: {
              base_path: config.fileManagerConfig.basePath,
              max_iterations: config.fileManagerConfig.maxIterations,
              system_prompt: formattedFileManagerSystemPrompt,
            },
            llm_base_url: config.baseUrl,
            llm_model: config.model,
            llm_api_key: config.apiKey || undefined,
            llm_provider: config.provider,
            disable_ssl_verification: disableSslVerification,
          }),
        });

        if (!response.ok) {
          const err = await response.json().catch(() => ({}));
          throw new Error(err.detail || `PDF Creator Agent error: ${response.status}`);
        }

        const data = await response.json();
        reply = data.answer;
        nextPdfCreatorAgentState = normalizePdfCreatorAgentState(data.agent_state);
        setIsConnected(true);

        const assistantPdfCreatorMsg: Message = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: reply,
          timestamp: Date.now(),
          steps: data.steps,
          actions: data.actions,
        };

        onConversationsChange(prev => {
          const idx = prev.findIndex(c => c.id === activeConvId);
          if (idx === -1) return prev;
          const updated = [...prev];
          updated[idx] = {
            ...updated[idx],
            agentState: {
              ...(updated[idx].agentState ?? {}),
              pdfCreator: nextPdfCreatorAgentState,
            },
            messages: [...updated[idx].messages, assistantPdfCreatorMsg],
            memory: buildConversationMemory([...updated[idx].messages, assistantPdfCreatorMsg]),
            updatedAt: Date.now(),
          };
          return updated;
        });
        setIsLoading(false);
        return;
      } else if (resolvedWorkflow === 'AGENT' && resolvedAgentRole === 'oracle_analyst') {
        const response = await fetch('/api/chat/oracle-analyst-agent', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          signal: controller.signal,
          body: JSON.stringify({
            message: text,
            history: memoryHistory,
            agent_state: oracleAnalystAgentState ?? undefined,
            oracle_connections: (config.oracleConnections ?? []).map((connection) => ({
              id: connection.id,
              label: connection.label,
              host: connection.host,
              port: connection.port,
              service_name: connection.serviceName,
              sid: connection.sid,
              dsn: connection.dsn,
              username: connection.username,
              password: connection.password,
            })),
            oracle_analyst_config: {
              connection_id: config.oracleAnalystConfig.connectionId,
              row_limit: config.oracleAnalystConfig.rowLimit,
              max_retries: config.oracleAnalystConfig.maxRetries,
              max_iterations: config.oracleAnalystConfig.maxIterations,
              toolkit_id: config.oracleAnalystConfig.toolkitId,
              system_prompt: formattedOracleSystemPrompt,
            },
            llm_base_url: config.baseUrl,
            llm_model: config.model,
            llm_api_key: config.apiKey || undefined,
            llm_provider: config.provider,
            disable_ssl_verification: disableSslVerification,
          }),
        });

        if (!response.ok) {
          const err = await response.json().catch(() => ({}));
          throw new Error(err.detail || `Oracle SQL agent error: ${response.status}`);
        }

        const data = await response.json();
        reply = data.answer;
        nextOracleAnalystAgentState = normalizeOracleAnalystAgentState(data.agent_state);
        setIsConnected(true);

        const assistantOracleMsg: Message = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: reply,
          timestamp: Date.now(),
          steps: data.steps,
        };

        onConversationsChange(prev => {
          const idx = prev.findIndex(c => c.id === activeConvId);
          if (idx === -1) return prev;
          const updated = [...prev];
          updated[idx] = {
            ...updated[idx],
            agentState: {
              ...(updated[idx].agentState ?? {}),
              oracleAnalyst: nextOracleAnalystAgentState,
            },
            messages: [...updated[idx].messages, assistantOracleMsg],
            memory: buildConversationMemory([...updated[idx].messages, assistantOracleMsg]),
            updatedAt: Date.now(),
          };
          return updated;
        });
        setIsLoading(false);
        return;
      } else {
        // Standard LLM / Agent flow (calls external API directly from frontend)
        let dynamicSystemPrompt = `${formattedSystemPrompt}\n\n${CLICKABLE_CHOICES_PROMPT}`;
        if (resolvedWorkflow === 'AGENT') {
          dynamicSystemPrompt += `\n\n[SYSTEM: You are currently operating as an Agent with the role: ${resolvedAgentRole.toUpperCase()}. Act accordingly.]`;
        }

        // Prepare messages for OpenAI/Ollama format, handling attachments
        const apiMessages = [
          { role: "system", content: dynamicSystemPrompt },
          ...activeMessages.map((m) => {
            if (m.role === 'user' && m.attachments && m.attachments.length > 0) {
              if (config.provider === 'ollama') {
                const images = m.attachments
                  .filter(a => a.type.startsWith('image/'))
                  .map(a => a.data.split(',')[1]);
                const textAttachments = m.attachments
                  .filter(a => !a.type.startsWith('image/'))
                  .map(a => `[Attached File: ${a.name}]`);
                const content = textAttachments.length > 0 
                  ? `${m.content}\n\n${textAttachments.join('\n')}`
                  : m.content;
                return {
                  role: m.role,
                  content: content,
                  images: images.length > 0 ? images : undefined
                };
              } else {
                const content: any[] = [{ type: "text", text: m.content }];
                m.attachments.forEach(a => {
                  if (a.type.startsWith('image/')) {
                    content.push({ type: "image_url", image_url: { url: a.data } });
                  } else {
                    content[0].text += `\n\n[Attached File: ${a.name}]`;
                  }
                });
                return { role: m.role, content };
              }
            }
            return { role: m.role, content: m.content };
          }),
        ];

        const baseUrl = (config.baseUrl || (config as any).endpoint || 'http://localhost:11434').replace(/\/$/, '');
        const chatEndpoint = config.provider === 'ollama' 
          ? `${baseUrl}/api/chat` 
          : `${baseUrl}/chat/completions`;

        const payload = {
          model: config.model,
          messages: apiMessages,
          stream: false,
        };

        const headers: Record<string, string> = {
          "Content-Type": "application/json",
        };

        if (config.apiKey) {
          headers["Authorization"] = `Bearer ${config.apiKey}`;
        }

        const response = await fetch(chatEndpoint, {
          method: "POST",
          headers,
          signal: controller.signal,
          body: JSON.stringify(payload),
        });

        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }

        setIsConnected(true);
        const data = await response.json();
        
        reply = config.provider === 'ollama' 
          ? data.message?.content 
          : data.choices?.[0]?.message?.content;

        if (!reply) {
          throw new Error("Invalid response format from API");
        }
      }

      const assistantMsg: Message = {
        id: (Date.now() + 1).toString(),
        role: "assistant",
        content: reply,
        timestamp: Date.now(),
        sources,
        confidence,
      };

        onConversationsChange(prev => {
          const idx = prev.findIndex(c => c.id === activeConvId);
          if (idx === -1) return prev;
          const updated = [...prev];
          updated[idx] = {
            ...updated[idx],
            agentState: resolvedWorkflow === 'AGENT' && resolvedAgentRole === 'clickhouse_query'
              ? {
                  ...(updated[idx].agentState ?? {}),
                  clickhouse: nextClickhouseAgentState,
                }
              : updated[idx].agentState,
            messages: [...updated[idx].messages, assistantMsg],
            memory: buildConversationMemory([...updated[idx].messages, assistantMsg]),
            updatedAt: Date.now()
          };
        return updated;
      });
    } catch (error) {
      if ((error as any)?.name === 'AbortError') {
        setIsConnected(true);
        return;
      }
      console.error("Error fetching from LLM:", error);
      setIsConnected(false);
      const errorMsg: Message = {
        id: (Date.now() + 1).toString(),
        role: "assistant",
        content: `**Error:** Could not complete the request.\n\n\`\`\`\n${error instanceof Error ? error.message : "Unknown error"}\n\`\`\`\n\nPlease check your configuration settings.`,
        timestamp: Date.now(),
      };
      onConversationsChange(prev => {
        const idx = prev.findIndex(c => c.id === activeConvId);
        if (idx === -1) return prev;
        const updated = [...prev];
        updated[idx] = {
          ...updated[idx],
          messages: [...updated[idx].messages, errorMsg],
          memory: buildConversationMemory([...updated[idx].messages, errorMsg]),
          updatedAt: Date.now()
        };
        return updated;
      });
    } finally {
      if (activeRequestControllerRef.current === controller) {
        activeRequestControllerRef.current = null;
      }
      setIsLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Tab" && mentionSuggestions.length > 0 && activeMentionQuery !== null) {
      e.preventDefault();
      handleMentionSelect(mentionSuggestions[0]);
      return;
    }
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const handleCheckboxToggle = (messageId: string, text: string, checked: boolean) => {
    // Clarification tiles should act like direct quick replies.
    if (checked) {
      handleSend(text);
    }
  };

  const inputPlaceholder =
    workflow === 'CREWAI'
      ? "Describe the automation you want to schedule..."
      : workflow === 'AGENT' && agentRole === 'clickhouse_query'
        ? "Ask a ClickHouse question or request a chart..."
      : workflow === 'AGENT' && agentRole === 'data_analyst'
        ? "Ask for a deeper multi-step ClickHouse investigation..."
      : workflow === 'AGENT' && agentRole === 'file_management'
        ? "Ask to list, read, create, move, edit, or delete files..."
        : workflow === 'AGENT' && agentRole === 'pdf_creator'
          ? "Ask to turn the latest analysis or pasted content into a PDF..."
        : workflow === 'AGENT' && agentRole === 'oracle_analyst'
          ? "Ask an Oracle business question and I will translate it into SQL..."
        : workflow === 'AGENT' && agentRole === 'data_quality_tables'
          ? "Open the Data Quality form to configure a profiling run..."
        : workflow === 'AGENT' && agentRole === 'manager'
          ? "Describe the outcome you want, and the Manager will route it if needed..."
        : workflow === 'MCP'
          ? "Message your MCP tool..."
          : "Message your AI agent...";
  const chatZoomPercent = Math.round(chatZoom * 100);
  const chatScaledStyle: React.CSSProperties | undefined =
    Math.abs(chatZoom - 1) < 0.001
      ? undefined
      : {
          transform: `scale(${chatZoom})`,
          transformOrigin: 'top center',
          width: `${100 / chatZoom}%`,
          height: `${100 / chatZoom}%`,
          margin: '0 auto',
        };
  const toolsPrimaryButtonBase = "inline-flex min-h-[2.65rem] items-center justify-center gap-2 rounded-full px-4 py-2 text-[11px] font-medium leading-tight transition-all whitespace-nowrap";
  const toolsSecondaryButtonBase = "inline-flex min-h-[2.55rem] items-center justify-center gap-1.5 rounded-full px-3.5 py-2 text-[11px] font-medium leading-tight transition-all whitespace-nowrap";
  const toolsNestedPanelBase = "rounded-[1.7rem] border bg-white/72 dark:bg-black/22 backdrop-blur-2xl p-3 flex flex-col gap-2.5 shadow-[0_20px_45px_rgba(15,23,42,0.10)]";
  const toolsIslandLevel = (workflow === 'AGENT' && isAgentMenuExpanded) || (workflow === 'MCP' && isMcpMenuExpanded)
    ? 2
    : isToolsIslandOpen
      ? 1
      : 0;
  const toolsIslandWidthClass =
    toolsIslandLevel === 2
      ? 'max-w-[58rem]'
      : 'max-w-[50rem]';
  const activeMcpToolLabel = (config.mcpTools ?? []).find((tool: McpTool) => tool.id === mcpToolId)?.label ?? 'MCP';
  const activeToolsSummary =
    workflow === 'AGENT'
      ? agentRole === 'manager'
        ? 'Agent Manager'
        : agentRole === 'clickhouse_query'
          ? 'Clickhouse SQL'
          : agentRole === 'data_analyst'
            ? 'Data Analyst'
            : agentRole === 'file_management'
              ? 'File management'
              : agentRole === 'pdf_creator'
                ? 'PDF creator'
                : agentRole === 'oracle_analyst'
                  ? 'Oracle SQL'
                  : 'Data quality'
      : workflow === 'MCP'
        ? activeMcpToolLabel
      : workflow === 'RAG'
          ? 'RAG Knowledge'
          : workflow === 'CREWAI'
          ? 'LangGraph Planning'
            : 'Pure LLM';
  const activeContextBadge = workflow === 'AGENT'
    ? agentRole === 'manager'
      ? {
          eyebrow: 'Agent',
          label: 'Agent Manager',
          icon: Star,
          iconWrapClass: 'bg-amber-50 text-amber-600 ring-1 ring-amber-200 dark:bg-amber-900/25 dark:text-amber-300 dark:ring-amber-800/70',
          eyebrowClass: 'text-amber-700 dark:text-amber-300',
          buttonClass: 'border-amber-400/60 bg-amber-500 text-white shadow-[0_18px_40px_rgba(245,158,11,0.28)] hover:bg-amber-400 dark:border-amber-300/20 dark:bg-amber-500 dark:hover:bg-amber-400',
        }
      : agentRole === 'clickhouse_query'
        ? {
            eyebrow: 'Agent',
            label: 'Clickhouse SQL',
            icon: Database,
            iconWrapClass: 'bg-cyan-50 text-cyan-600 ring-1 ring-cyan-200 dark:bg-cyan-900/25 dark:text-cyan-300 dark:ring-cyan-800/70',
            eyebrowClass: 'text-cyan-700 dark:text-cyan-300',
            buttonClass: 'border-cyan-400/60 bg-cyan-500 text-white shadow-[0_18px_40px_rgba(6,182,212,0.28)] hover:bg-cyan-400 dark:border-cyan-300/20 dark:bg-cyan-500 dark:hover:bg-cyan-400',
          }
        : agentRole === 'data_analyst'
          ? {
              eyebrow: 'Agent',
              label: 'Data Analyst',
              icon: Cpu,
              iconWrapClass: 'bg-violet-50 text-violet-600 ring-1 ring-violet-200 dark:bg-violet-900/25 dark:text-violet-300 dark:ring-violet-800/70',
              eyebrowClass: 'text-violet-700 dark:text-violet-300',
              buttonClass: 'border-violet-400/60 bg-violet-500 text-white shadow-[0_18px_40px_rgba(139,92,246,0.28)] hover:bg-violet-400 dark:border-violet-300/20 dark:bg-violet-500 dark:hover:bg-violet-400',
            }
          : agentRole === 'file_management'
            ? {
                eyebrow: 'Agent',
                label: 'File management',
                icon: FolderOpen,
                iconWrapClass: 'bg-emerald-50 text-emerald-600 ring-1 ring-emerald-200 dark:bg-emerald-900/25 dark:text-emerald-300 dark:ring-emerald-800/70',
                eyebrowClass: 'text-emerald-700 dark:text-emerald-300',
                buttonClass: 'border-emerald-400/60 bg-emerald-500 text-white shadow-[0_18px_40px_rgba(16,185,129,0.28)] hover:bg-emerald-400 dark:border-emerald-300/20 dark:bg-emerald-500 dark:hover:bg-emerald-400',
              }
            : agentRole === 'pdf_creator'
              ? {
                  eyebrow: 'Agent',
                  label: 'PDF creator',
                  icon: File,
                  iconWrapClass: 'bg-slate-100 text-slate-700 ring-1 ring-slate-200 dark:bg-slate-800/70 dark:text-slate-200 dark:ring-slate-700/70',
                  eyebrowClass: 'text-slate-600 dark:text-slate-300',
                  buttonClass: 'border-slate-400/60 bg-slate-700 text-white shadow-[0_18px_40px_rgba(51,65,85,0.28)] hover:bg-slate-600 dark:border-slate-300/20 dark:bg-slate-700 dark:hover:bg-slate-600',
                }
              : agentRole === 'oracle_analyst'
                ? {
                    eyebrow: 'Agent',
                    label: 'Oracle SQL',
                    icon: Database,
                    iconWrapClass: 'bg-orange-50 text-orange-600 ring-1 ring-orange-200 dark:bg-orange-900/25 dark:text-orange-300 dark:ring-orange-800/70',
                    eyebrowClass: 'text-orange-700 dark:text-orange-300',
                    buttonClass: 'border-orange-400/60 bg-orange-500 text-white shadow-[0_18px_40px_rgba(249,115,22,0.28)] hover:bg-orange-400 dark:border-orange-300/20 dark:bg-orange-500 dark:hover:bg-orange-400',
                  }
                : {
                    eyebrow: 'Agent',
                    label: 'Data quality - Tables',
                    icon: BarChart3,
                    iconWrapClass: 'bg-fuchsia-50 text-fuchsia-600 ring-1 ring-fuchsia-200 dark:bg-fuchsia-900/25 dark:text-fuchsia-300 dark:ring-fuchsia-800/70',
                    eyebrowClass: 'text-fuchsia-700 dark:text-fuchsia-300',
                    buttonClass: 'border-fuchsia-400/60 bg-fuchsia-500 text-white shadow-[0_18px_40px_rgba(217,70,239,0.28)] hover:bg-fuchsia-400 dark:border-fuchsia-300/20 dark:bg-fuchsia-500 dark:hover:bg-fuchsia-400',
                  }
    : workflow === 'MCP'
      ? {
          eyebrow: 'MCP',
          label: activeMcpToolLabel,
          icon: Network,
          iconWrapClass: 'bg-teal-50 text-teal-600 ring-1 ring-teal-200 dark:bg-teal-900/25 dark:text-teal-300 dark:ring-teal-800/70',
          eyebrowClass: 'text-teal-700 dark:text-teal-300',
          buttonClass: 'border-teal-400/60 bg-teal-500 text-white shadow-[0_18px_40px_rgba(20,184,166,0.28)] hover:bg-teal-400 dark:border-teal-300/20 dark:bg-teal-500 dark:hover:bg-teal-400',
        }
      : workflow === 'RAG'
        ? {
            eyebrow: 'Mode',
            label: 'RAG Knowledge',
            icon: Database,
            iconWrapClass: 'bg-emerald-50 text-emerald-600 ring-1 ring-emerald-200 dark:bg-emerald-900/25 dark:text-emerald-300 dark:ring-emerald-800/70',
            eyebrowClass: 'text-emerald-700 dark:text-emerald-300',
            buttonClass: 'border-emerald-400/60 bg-emerald-500 text-white shadow-[0_18px_40px_rgba(16,185,129,0.28)] hover:bg-emerald-400 dark:border-emerald-300/20 dark:bg-emerald-500 dark:hover:bg-emerald-400',
          }
        : workflow === 'CREWAI'
          ? {
              eyebrow: 'Mode',
              label: 'LangGraph Planning',
              icon: CalendarDays,
              iconWrapClass: 'bg-sky-50 text-sky-600 ring-1 ring-sky-200 dark:bg-sky-900/25 dark:text-sky-300 dark:ring-sky-800/70',
              eyebrowClass: 'text-sky-700 dark:text-sky-300',
              buttonClass: 'border-sky-400/60 bg-sky-500 text-white shadow-[0_18px_40px_rgba(14,165,233,0.28)] hover:bg-sky-400 dark:border-sky-300/20 dark:bg-sky-500 dark:hover:bg-sky-400',
            }
          : {
              eyebrow: 'Mode',
              label: 'Pure LLM',
              icon: Cpu,
              iconWrapClass: 'bg-blue-50 text-blue-600 ring-1 ring-blue-200 dark:bg-blue-900/25 dark:text-blue-300 dark:ring-blue-800/70',
              eyebrowClass: 'text-blue-700 dark:text-blue-300',
              buttonClass: 'border-blue-400/60 bg-blue-500 text-white shadow-[0_18px_40px_rgba(59,130,246,0.28)] hover:bg-blue-400 dark:border-blue-300/20 dark:bg-blue-500 dark:hover:bg-blue-400',
            };
  const floatingDockButtonBaseClass = "group flex h-14 w-14 items-center justify-center rounded-full shadow-[0_20px_50px_rgba(15,23,42,0.18)] backdrop-blur-2xl transition-all duration-300 hover:scale-[1.02]";
  const floatingGlassButtonClass = `${floatingDockButtonBaseClass} border border-white/50 bg-white/65 text-gray-800 hover:bg-white/80 dark:border-white/10 dark:bg-black/45 dark:text-white dark:hover:bg-black/55`;
  const floatingContextButtonClass = `${floatingDockButtonBaseClass} border text-white`;
  const formattedSystemPrompt = withBeautifulResponsePrompt(config.systemPrompt);
  const formattedOracleSystemPrompt = withBeautifulResponsePrompt(config.oracleAnalystConfig.systemPrompt);
  const formattedFileManagerSystemPrompt = withBeautifulResponsePrompt(config.fileManagerConfig.systemPrompt);
  const ActiveContextIcon = activeContextBadge.icon;
  const activeContextShortLabel =
    workflow === 'AGENT'
      ? AGENT_ROLE_SHORT_LABELS[agentRole]
      : workflow === 'MCP'
        ? 'MCP'
        : workflow === 'RAG'
          ? 'RAG'
          : workflow === 'CREWAI'
            ? 'PLAN'
            : 'LLM';
  const activeMentionQuery = useMemo(() => getMentionQuery(input, inputCursor), [input, inputCursor]);
  const mentionSuggestions = useMemo(() => {
    if (activeMentionQuery === null) return [];
    const normalizedQuery = normalizeMentionToken(activeMentionQuery);
    return AGENT_MENTION_TARGETS.filter((target) => (
      !normalizedQuery
        || target.aliases.some((alias) => normalizeMentionToken(alias).startsWith(normalizedQuery))
        || normalizeMentionToken(target.label).includes(normalizedQuery)
    )).slice(0, 6);
  }, [activeMentionQuery]);
  const breadcrumbNodes = useMemo<BreadcrumbNode[]>(() => {
    const nodes: BreadcrumbNode[] = [
      {
        id: 'user',
        label: 'User',
        icon: MessageSquare,
        toneClass: 'border-slate-200/80 bg-white/80 text-slate-700 dark:border-white/10 dark:bg-white/5 dark:text-slate-200',
      },
    ];

    if (workflow === 'AGENT' && agentRole === 'manager') {
      nodes.push({
        id: 'manager',
        label: 'Agent Manager',
        icon: Star,
        toneClass: 'border-amber-200/80 bg-amber-50/80 text-amber-700 dark:border-amber-800/70 dark:bg-amber-950/20 dark:text-amber-200',
      });

      if (managerAgentState.activeDelegate) {
        const delegateRole = managerAgentState.activeDelegate;
        const delegateIcon =
          delegateRole === 'file_management'
            ? FolderOpen
            : delegateRole === 'data_analyst'
              ? Cpu
              : delegateRole === 'oracle_analyst'
                ? Database
                : delegateRole === 'pdf_creator'
                  ? File
                  : delegateRole === 'data_quality_tables'
                    ? BarChart3
                    : Database;
        nodes.push({
          id: delegateRole,
          label: AGENT_ROLE_LABELS[delegateRole],
          icon: delegateIcon,
          toneClass: 'border-cyan-200/80 bg-cyan-50/80 text-cyan-700 dark:border-cyan-800/70 dark:bg-cyan-950/20 dark:text-cyan-200',
        });
      }
    } else if (workflow === 'AGENT') {
      nodes.push({
        id: agentRole,
        label: AGENT_ROLE_LABELS[agentRole],
        icon: ActiveContextIcon,
        toneClass: `${activeContextBadge.iconWrapClass} border`,
      });
    } else if (workflow === 'MCP') {
      nodes.push({
        id: 'mcp',
        label: activeMcpToolLabel,
        icon: Network,
        toneClass: 'border-teal-200/80 bg-teal-50/80 text-teal-700 dark:border-teal-800/70 dark:bg-teal-950/20 dark:text-teal-200',
      });
    } else if (workflow === 'RAG') {
      nodes.push({
        id: 'rag',
        label: 'RAG Knowledge',
        icon: Database,
        toneClass: 'border-blue-200/80 bg-blue-50/80 text-blue-700 dark:border-blue-800/70 dark:bg-blue-950/20 dark:text-blue-200',
      });
    } else if (workflow === 'CREWAI') {
      nodes.push({
        id: 'planning',
        label: 'LangGraph Planning',
        icon: CalendarDays,
        toneClass: 'border-sky-200/80 bg-sky-50/80 text-sky-700 dark:border-sky-800/70 dark:bg-sky-950/20 dark:text-sky-200',
      });
    } else {
      nodes.push({
        id: 'llm',
        label: 'Pure LLM',
        icon: Cpu,
        toneClass: 'border-blue-200/80 bg-blue-50/80 text-blue-700 dark:border-blue-800/70 dark:bg-blue-950/20 dark:text-blue-200',
      });
    }

    latestMentionTargets.forEach((target) => {
      if (nodes.some((node) => node.label === target.label)) return;
      nodes.push({
        id: `mention-${target.id}`,
        label: target.label,
        icon: target.icon,
        toneClass: 'border-violet-200/80 bg-violet-50/80 text-violet-700 dark:border-violet-800/70 dark:bg-violet-950/20 dark:text-violet-200',
      });
    });

    return nodes;
  }, [workflow, agentRole, managerAgentState.activeDelegate, latestMentionTargets, ActiveContextIcon, activeContextBadge.iconWrapClass, activeMcpToolLabel]);
  const draftArtifacts = useMemo(() => buildDraftArtifacts(messages, workflow, agentRole), [messages, workflow, agentRole]);
  const latestTraceSteps = useMemo(() => {
    for (const message of [...messages].reverse()) {
      if (message.role === 'assistant' && Array.isArray(message.steps) && message.steps.length > 0) {
        return message.steps;
      }
    }
    return [] as AgentStep[];
  }, [messages]);
  const latestConfidence = useMemo(() => {
    for (const message of [...messages].reverse()) {
      if (message.role === 'assistant' && typeof message.confidence === 'number') {
        return message.confidence;
      }
    }
    if (latestTraceSteps.length > 0) {
      const errorSteps = latestTraceSteps.filter((step) => step.status === 'error').length;
      const successSteps = latestTraceSteps.filter((step) => step.status === 'success').length;
      const base = latestTraceSteps.length === 0 ? 0.58 : Math.max(0.2, Math.min(0.94, (successSteps + 0.5) / (latestTraceSteps.length + errorSteps + 0.5)));
      return base;
    }
    return null;
  }, [messages, latestTraceSteps]);
  const confidenceBadge = useMemo(() => {
    if (latestConfidence === null || latestConfidence === undefined) {
      return {
        label: 'Estimated',
        value: 'Unknown',
        tone: 'border-slate-200/80 bg-white/80 text-slate-700 dark:border-white/10 dark:bg-white/5 dark:text-slate-200',
      };
    }
    const percent = Math.round(latestConfidence * 100);
    if (latestConfidence >= 0.72) {
      return {
        label: 'Confidence',
        value: `${percent}% · High`,
        tone: 'border-emerald-200/80 bg-emerald-50/80 text-emerald-700 dark:border-emerald-800/70 dark:bg-emerald-950/20 dark:text-emerald-200',
      };
    }
    if (latestConfidence >= 0.45) {
      return {
        label: 'Confidence',
        value: `${percent}% · Medium`,
        tone: 'border-amber-200/80 bg-amber-50/80 text-amber-700 dark:border-amber-800/70 dark:bg-amber-950/20 dark:text-amber-200',
      };
    }
    return {
      label: 'Confidence',
      value: `${percent}% · Low`,
      tone: 'border-rose-200/80 bg-rose-50/80 text-rose-700 dark:border-rose-800/70 dark:bg-rose-950/20 dark:text-rose-200',
    };
  }, [latestConfidence]);
  const activityIndicators = useMemo(() => {
    const labels = workflow === 'AGENT'
      ? agentRole === 'manager'
        ? ['routing specialists', 'reviewing prior context', 'preparing next hand-off']
        : agentRole === 'clickhouse_query'
          ? ['reviewing schema', 'planning SQL variants', 'formatting result']
          : agentRole === 'data_analyst'
            ? ['probing data patterns', 'comparing query variants', 'writing executive summary']
            : agentRole === 'file_management'
              ? ['inspecting files', 'checking safe operations', 'preparing output']
              : agentRole === 'oracle_analyst'
                ? ['inspecting Oracle schema', 'checking SQL safety', 'summarizing findings']
                : ['preparing specialist workflow', 'reviewing context', 'packaging response']
      : workflow === 'RAG'
        ? ['vectorizing request', 'retrieving context', 'grounding answer']
        : workflow === 'MCP'
          ? ['connecting tool session', 'calling MCP tool', 'formatting tool output']
          : workflow === 'CREWAI'
            ? ['reviewing schedule draft', 'checking triggers', 'assembling execution plan']
            : ['consulting prompt context', 'generating variants', 'writing final answer'];
    return labels;
  }, [workflow, agentRole]);
  const activityProgressValue = isLoading ? 72 : latestTraceSteps.length > 0 ? Math.min(100, 28 + latestTraceSteps.length * 14) : 16;

  return (
    <div className="flex h-screen relative overflow-hidden bg-[#f5f5f5] dark:bg-[#0f0f13] transition-colors duration-300">
      <div className="mesh-bg" />

      {/* Sidebar */}
      <aside className={`bg-white/60 dark:bg-black/60 border-r border-gray-200/50 dark:border-gray-800/50 backdrop-blur-xl flex flex-col z-20 flex-shrink-0 transition-all duration-300 ease-in-out hidden md:flex ${isSidebarOpen ? 'w-64 md:w-72 opacity-100' : 'w-0 opacity-0 overflow-hidden border-none'}`}>
        <div className="p-4 border-b border-gray-200/50 dark:border-gray-800/50 w-64 md:w-72">
          <button
            onClick={createNewChat}
            className="w-full flex items-center justify-center gap-2 bg-black text-white px-4 py-3 rounded-xl hover:bg-gray-800 transition-all shadow-lg shadow-black/5 font-medium"
          >
            <Plus className="w-4 h-4" /> New Chat
          </button>
        </div>
        <div className="flex-1 overflow-y-auto p-3 space-y-2">
          {conversations.map((conv) => (
            <div
              key={conv.id}
              onClick={() => onCurrentIdChange(conv.id)}
              className={`group relative w-full text-left p-3 rounded-xl text-sm transition-all cursor-pointer border ${
                currentId === conv.id
                  ? "bg-white dark:bg-gray-800 border-gray-200 dark:border-gray-700 shadow-sm"
                  : "border-transparent hover:bg-white/50 dark:hover:bg-white/5 hover:border-gray-200/50 dark:hover:border-gray-700/50"
              }`}
            >
              <div className="flex items-start gap-3">
                <MessageSquare className={`w-4 h-4 mt-0.5 flex-shrink-0 ${currentId === conv.id ? 'text-blue-500' : 'text-gray-400'}`} />
                <div className="flex-1 overflow-hidden">
                  <div className="truncate font-medium text-gray-900 dark:text-gray-100">{conv.title}</div>
                  <div className="text-xs text-gray-400 mt-1">
                    {new Date(conv.updatedAt).toLocaleDateString()}
                  </div>
                </div>
              </div>
              <button
                onClick={(e) => deleteConversation(e, conv.id)}
                className={`absolute right-2 top-1/2 -translate-y-1/2 p-1.5 rounded-lg text-gray-400 hover:text-red-500 hover:bg-red-50 transition-colors ${currentId === conv.id ? 'opacity-100' : 'opacity-0 group-hover:opacity-100'}`}
                title="Delete chat"
              >
                <Trash2 className="w-4 h-4" />
              </button>
            </div>
          ))}
          {conversations.length === 0 && (
            <div className="text-center text-sm text-gray-400 mt-10 px-4">
              No previous conversations. Start a new chat!
            </div>
          )}
        </div>
      </aside>

      {/* Main Chat Area */}
      <div className="flex-1 flex flex-col relative z-10 h-screen overflow-hidden">
        {/* Header */}
      <header className="glass-panel m-2 rounded-xl px-4 py-2 flex items-center justify-between overflow-visible z-10 relative">
        {/* Left: Sidebar Toggle */}
        <div className="flex items-center w-1/3">
          <button 
            onClick={() => setIsSidebarOpen(!isSidebarOpen)}
            className="p-2 -ml-2 rounded-xl hover:bg-black/5 dark:hover:bg-white/10 text-gray-600 dark:text-gray-300 transition-colors hidden md:block"
            title={isSidebarOpen ? "Close sidebar" : "Open sidebar"}
          >
            {isSidebarOpen ? <PanelLeftClose className="w-5 h-5" /> : <PanelLeftOpen className="w-5 h-5" />}
          </button>
        </div>

        {/* Center: Title */}
        <div className="flex flex-col items-center justify-center w-1/3">
          <div className="flex items-center gap-5">
            <div className="w-9 h-9 bg-gradient-to-tr from-slate-700 to-slate-900 rounded-xl flex items-center justify-center shadow-md shadow-slate-900/20">
              <Hammer className="w-5 h-5 text-white" />
            </div>
            <h1 className="text-3xl font-bold text-gray-900 dark:text-white tracking-tight">RAGnarok</h1>
            <div className="w-9 h-9 bg-gradient-to-tr from-slate-700 to-slate-900 rounded-xl flex items-center justify-center shadow-md shadow-slate-900/20">
              <Hammer className="w-5 h-5 text-white" />
            </div>
          </div>
          <div className="flex items-center gap-2 text-[10px] font-medium text-gray-500 dark:text-gray-400 mt-0.5">
            <span className={`w-1.5 h-1.5 rounded-full ${isConnected ? 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.6)]' : 'bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.6)]'}`} />
            {isConnected ? 'Active' : 'Offline'}
          </div>
        </div>

        {/* Right: Settings */}
        <div className="flex items-center justify-end w-1/3 gap-1">
          {currentId && (
            <button
              onClick={clearCurrentChat}
              className="glass-button p-2 rounded-xl text-red-600 dark:text-red-400 hover:text-red-700 hover:bg-red-50 dark:hover:bg-red-900/30 flex items-center gap-1.5"
              title="Reset conversation"
            >
              <Trash2 className="w-4 h-4" />
              <span className="hidden sm:inline text-xs font-medium">Reset</span>
            </button>
          )}
          <button
            onClick={onToggleDark}
            className="glass-button p-2 rounded-xl text-gray-600 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white"
            title={isDark ? "Light mode" : "Dark mode"}
          >
            {isDark ? <Sun className="w-4 h-4" /> : <Moon className="w-4 h-4" />}
          </button>
          {onGoHome && (
            <button
              onClick={onGoHome}
              className="glass-button p-2 rounded-xl text-gray-600 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white"
              title="Home"
            >
              <Home className="w-4 h-4" />
            </button>
          )}
        </div>
      </header>

      <div className="flex-1 flex flex-col min-h-0 overflow-hidden">
        <div className="flex-1 flex flex-col min-h-0" style={chatScaledStyle}>
        <div className="flex-1 min-h-0 px-4 md:px-8 pb-4">
          <div className="h-full max-w-[82rem] mx-auto flex flex-col">
            <div className="flex-1 min-w-0 flex flex-col min-h-0">
              {/* Chat Area */}
              <main ref={chatScrollContainerRef} className="flex-1 overflow-y-auto z-10 scroll-smooth">
                <div className="max-w-[77rem] mx-auto pb-16">
                  {visibleMessages.map((msg) => (
                    <div
                      key={msg.id}
                      ref={msg.id === lastAssistantMessageId ? lastAssistantMessageRef : undefined}
                    >
                      <ChatMessage
                        message={msg}
                        onCheckboxToggle={handleCheckboxToggle}
                        onAction={handleChatAction}
                      />
                    </div>
                  ))}

                  {isLoading && (
                    <div className="flex gap-3 w-full max-w-[77rem] mx-auto mb-5 animate-fade-in-up">
                      <div className="flex-shrink-0 w-9 h-9 rounded-xl bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 flex items-center justify-center shadow-sm">
                        <Bot className="w-5 h-5 text-gray-400 dark:text-gray-500" />
                      </div>
                      <div className="glass-panel px-5 py-3 rounded-[1.7rem] rounded-tl-sm flex items-center gap-2">
                        <Loader2 className="w-4.5 h-4.5 text-blue-500 animate-spin" />
                        <span className="text-[13px] text-gray-500 dark:text-gray-400 font-medium">Thinking...</span>
                      </div>
                    </div>
                  )}

                  {workflow === 'CREWAI' && (
                    <div className="max-w-[77rem] mx-auto mb-4 p-4 rounded-xl bg-gradient-to-br from-emerald-50 via-teal-50 to-cyan-50 dark:from-emerald-900/20 dark:via-teal-900/20 dark:to-cyan-900/20 border border-emerald-200/60 dark:border-emerald-700/40 shadow-sm animate-fade-in-up">
                      <div className="flex flex-col md:flex-row md:items-start md:justify-between gap-4">
                        <div>
                          <div className="flex items-center gap-2 mb-1.5">
                            <CalendarDays className="w-4 h-4 text-emerald-600 dark:text-emerald-300" />
                            <h3 className="font-semibold text-emerald-900 dark:text-emerald-200 text-[13px]">LangGraph Planning</h3>
                          </div>
                          <div className="text-[11px] text-emerald-900/85 dark:text-emerald-300/90 leading-relaxed space-y-1">
                            <p>This mode schedules existing agents from natural language or from a guided planning form powered by LangGraph.</p>
                            <ul className="list-disc pl-4 space-y-0.5">
                              <li>Use natural language to describe what should run, when it should run, and which agents should execute.</li>
                              <li>Open the planning form to configure fixed schedules, ClickHouse watches, or file-arrival triggers.</li>
                              <li>Saved plans are persisted in the backend and executed automatically through the Python LangGraph scheduler.</li>
                            </ul>
                          </div>
                        </div>
                        <div className="flex flex-col items-start md:items-end gap-2">
                          <div className="text-[11px] text-emerald-800/80 dark:text-emerald-300/80">
                            {planningState.plans.length} plan(s) · {planningState.runs.length} run(s)
                          </div>
                          <div className="flex flex-wrap items-center gap-2">
                            <button
                              type="button"
                              onClick={() => openPlanningModal(planningAgentState.draft)}
                              className="inline-flex items-center gap-2 px-4 py-2 rounded-2xl bg-black text-white text-xs font-medium hover:bg-gray-800 transition-colors"
                            >
                              <CalendarDays className="w-3.5 h-3.5" />
                              Open planning form
                            </button>
                            <button
                              type="button"
                              onClick={openPlanningMonitor}
                              className="inline-flex items-center gap-2 px-4 py-2 rounded-2xl border border-emerald-300/70 bg-white/70 text-emerald-900 text-xs font-medium hover:bg-white transition-colors dark:border-emerald-700/60 dark:bg-emerald-950/30 dark:text-emerald-200 dark:hover:bg-emerald-950/50"
                            >
                              <BarChart3 className="w-3.5 h-3.5" />
                              Open activity monitor
                            </button>
                          </div>
                        </div>
                      </div>
                    </div>
                  )}

                  {workflow === 'RAG' && (
                    <div className="max-w-[77rem] mx-auto mb-4 p-3 rounded-xl bg-gradient-to-br from-blue-50 to-indigo-50 dark:from-blue-900/20 dark:to-indigo-900/20 border border-blue-200/50 dark:border-blue-700/40 shadow-sm animate-fade-in-up">
                      <div className="flex items-center gap-2 mb-1.5">
                        <Database className="w-4 h-4 text-blue-500" />
                        <h3 className="font-semibold text-blue-900 dark:text-blue-200 text-[13px]">Retrieval-Augmented Generation (RAG)</h3>
                      </div>
                      <div className="text-[11px] text-blue-800/90 dark:text-blue-300/90 leading-relaxed space-y-1">
                        <p>Welcome to <strong>RAG</strong> mode. This workflow helps you find and synthesize information from your documents:</p>
                        <ul className="list-disc pl-4 space-y-0.5">
                          <li><strong>Retrieve:</strong> Your query is converted into a vector using the local embedding model and searched against the Elasticsearch database.</li>
                          <li><strong>Augment:</strong> The most relevant document chunks (optimized with overlap) are retrieved using K-Nearest Neighbors (KNN).</li>
                          <li><strong>Generate:</strong> The LLM uses this retrieved context to provide accurate, grounded answers.</li>
                        </ul>
                        <p className="italic mt-1 text-blue-700/70">Tip: You can configure the Elasticsearch URL, embedding model, chunk size, and KNN neighbors in the settings.</p>
                      </div>
                    </div>
                  )}

                </div>
              </main>

              {/* Input Area */}
              <div className="pt-3 z-10 w-full max-w-[77rem] mx-auto">
                <div className="glass-panel rounded-[1.8rem] p-1.5 flex flex-col gap-1.5 shadow-2xl shadow-black/5">
                  {activeMentionQuery !== null && (
                    <div className="px-3.5 pt-2.5 pb-1">
                      <div className="rounded-[1.35rem] border border-white/70 bg-white/78 p-2.5 shadow-sm backdrop-blur-xl dark:border-white/10 dark:bg-white/6">
                        <div className="flex items-center justify-between gap-3">
                          <div>
                            <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">
                              Agent mentions
                            </div>
                            <div className="mt-1 text-xs text-gray-600 dark:text-gray-300">
                              Type <span className="font-semibold">@</span> to target a specialist or open an orchestrated hand-off.
                            </div>
                          </div>
                          <span className="rounded-full border border-slate-200/80 bg-slate-50/80 px-2 py-1 text-[9px] font-semibold uppercase tracking-[0.14em] text-slate-500 dark:border-white/10 dark:bg-white/5 dark:text-slate-300">
                            Tab to insert
                          </span>
                        </div>
                        <div className="mt-2 flex flex-wrap gap-2">
                          {mentionSuggestions.map((target) => {
                            const MentionIcon = target.icon;
                            return (
                              <button
                                key={`mention-${target.id}`}
                                type="button"
                                onClick={() => handleMentionSelect(target)}
                                className="inline-flex items-center gap-2 rounded-full border border-slate-200/80 bg-white/85 px-3 py-2 text-xs font-medium text-gray-700 transition-colors hover:bg-white dark:border-white/10 dark:bg-white/5 dark:text-gray-200 dark:hover:bg-white/10"
                              >
                                <MentionIcon className="h-3.5 w-3.5" />
                                <span>@{target.aliases[0]}</span>
                              </button>
                            );
                          })}
                        </div>
                      </div>
                    </div>
                  )}
                  {attachments.length > 0 && (
                    <div className="flex flex-wrap gap-2 px-3.5 pt-2.5 pb-1">
                      {attachments.map(att => (
                        <div key={att.id} className="relative group flex items-center gap-2 bg-white/60 dark:bg-gray-800/60 border border-gray-200/60 dark:border-gray-700/60 px-2.5 py-1.5 rounded-xl text-sm shadow-sm">
                          {att.type.startsWith('image/') ? (
                            <img src={att.data} alt={att.name} className="w-6 h-6 object-cover rounded-md" />
                          ) : (
                            <File className="w-4 h-4 text-blue-500 dark:text-blue-400" />
                          )}
                          <span className="truncate max-w-[120px] font-medium text-gray-700 dark:text-gray-200">{att.name}</span>
                          <button
                            onClick={() => removeAttachment(att.id)}
                            className="absolute -top-2 -right-2 bg-red-500 text-white rounded-full p-0.5 opacity-0 group-hover:opacity-100 transition-opacity shadow-sm"
                          >
                            <X className="w-3 h-3" />
                          </button>
                        </div>
                      ))}
                    </div>
                  )}

                  <div className="flex items-end gap-2 w-full">
                    <input
                      type="file"
                      multiple
                      ref={fileInputRef}
                      onChange={handleFileChange}
                      className="hidden"
                      accept="image/*,.pdf,.txt,.csv,.md"
                    />
                    <button
                      onClick={() => fileInputRef.current?.click()}
                      className="flex-shrink-0 w-11 h-11 rounded-2xl text-gray-400 dark:text-gray-500 hover:text-blue-500 dark:hover:text-blue-400 hover:bg-blue-50 dark:hover:bg-blue-900/30 flex items-center justify-center transition-colors mb-0.5 ml-1"
                      title="Attach file"
                    >
                      <Paperclip className="w-5 h-5" />
                    </button>
                    <textarea
                      ref={textareaRef}
                      value={input}
                      onChange={handleInputChange}
                      onClick={(event) => setInputCursor(event.currentTarget.selectionStart ?? event.currentTarget.value.length)}
                      onSelect={(event) => setInputCursor(event.currentTarget.selectionStart ?? event.currentTarget.value.length)}
                      onKeyDown={handleKeyDown}
                      placeholder={inputPlaceholder}
                      className="w-full min-h-[48px] bg-transparent border-none resize-none focus:ring-0 px-2 py-3 text-[14px] leading-[1.65] text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 outline-none overflow-y-auto"
                      rows={1}
                    />
                    <button
                      type="button"
                      onClick={handleCopyInput}
                      disabled={!input.trim()}
                      className="flex-shrink-0 w-11 h-11 rounded-2xl text-gray-400 dark:text-gray-500 hover:text-gray-700 dark:hover:text-gray-200 hover:bg-black/5 dark:hover:bg-white/10 flex items-center justify-center transition-colors mb-0.5 disabled:opacity-40 disabled:cursor-not-allowed"
                      title={isInputCopied ? "Copied" : "Copy input"}
                    >
                      {isInputCopied ? <Check className="w-4.5 h-4.5" /> : <Copy className="w-4.5 h-4.5" />}
                    </button>
                    <button
                      onClick={() => {
                        if (isLoading) {
                          stopCurrentExecution();
                          return;
                        }
                        handleSend();
                      }}
                      disabled={!isLoading && (!input.trim() && attachments.length === 0)}
                      className={`flex-shrink-0 w-11 h-11 rounded-2xl flex items-center justify-center disabled:opacity-50 disabled:cursor-not-allowed transition-colors mb-0.5 mr-0.5 ${
                        isLoading
                          ? 'bg-rose-600 text-white hover:bg-rose-700'
                          : 'bg-black text-white hover:bg-gray-800'
                      }`}
                      title={isLoading ? 'Stop current execution' : 'Send'}
                    >
                      {isLoading ? <X className="w-4.5 h-4.5" /> : <Send className="w-4.5 h-4.5 ml-0.5" />}
                    </button>
                  </div>
                </div>

              </div>
            </div>
          </div>
        </div>
      </div>
      </div>

      {isToolsIslandOpen && (
        <div className="absolute inset-0 z-40 flex items-center justify-center px-4 py-8 md:px-8 pointer-events-none">
          <div className="absolute inset-0 bg-slate-950/10 backdrop-blur-[2px] pointer-events-none dark:bg-black/30" aria-hidden="true" />
          <div
            ref={toolsIslandRef}
            className={`pointer-events-auto relative w-full ${toolsIslandWidthClass} animate-scale-in rounded-[2.35rem] border border-white/70 bg-white/82 p-4 shadow-[0_30px_90px_rgba(15,23,42,0.20)] backdrop-blur-3xl dark:border-white/10 dark:bg-black/55`}
          >
            <div className="flex items-start justify-between gap-4 rounded-[1.7rem] bg-gradient-to-r from-black/[0.04] via-white/80 to-black/[0.04] px-4 py-3 dark:from-white/[0.04] dark:via-white/[0.08] dark:to-white/[0.04]">
              <div className="flex min-w-0 items-center gap-3">
                <div className="flex h-11 w-11 items-center justify-center rounded-full bg-black text-white shadow-sm dark:bg-white dark:text-black">
                  <Hammer className="h-4.5 w-4.5" />
                </div>
                <div className="min-w-0">
                  <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-400 dark:text-gray-500">
                    Tools
                  </div>
                  <div className="truncate text-base font-semibold text-gray-900 dark:text-gray-100">
                    {activeToolsSummary}
                  </div>
                  <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                    Open a mode, drill into level 2 when needed, then the island folds back away.
                  </div>
                </div>
              </div>
              <button
                type="button"
                onClick={collapseToolsIsland}
                className="inline-flex h-10 w-10 flex-shrink-0 items-center justify-center rounded-full bg-white/80 text-gray-500 shadow-sm transition-colors hover:bg-white hover:text-gray-800 dark:bg-white/10 dark:text-gray-300 dark:hover:bg-white/15 dark:hover:text-white"
                title="Close tools menu"
              >
                <X className="h-4.5 w-4.5" />
              </button>
            </div>

            <div className="mt-4 max-h-[min(34rem,calc(100vh-11rem))] overflow-y-auto pr-1">
              <div className="flex flex-wrap items-center justify-center gap-2">
                <button
                  onClick={() => handleWorkflowSelection('LLM')}
                  className={`${toolsPrimaryButtonBase} ${workflow === 'LLM' ? 'bg-blue-500 text-white shadow-md shadow-blue-500/20' : 'bg-white/80 text-gray-700 border border-gray-200/80 hover:bg-white dark:bg-white/7 dark:text-gray-200 dark:border-gray-700 dark:hover:bg-white/10'}`}
                >
                  <Cpu className="h-4 w-4" /> Pure LLM
                </button>
                <button
                  onClick={() => handleWorkflowSelection('RAG')}
                  className={`${toolsPrimaryButtonBase} ${workflow === 'RAG' ? 'bg-emerald-500 text-white shadow-md shadow-emerald-500/20' : 'bg-white/80 text-gray-700 border border-gray-200/80 hover:bg-white dark:bg-white/7 dark:text-gray-200 dark:border-gray-700 dark:hover:bg-white/10'}`}
                >
                  <Database className="h-4 w-4" /> RAG Knowledge
                </button>
                <button
                  onClick={() => handleWorkflowSelection('AGENT')}
                  className={`${toolsPrimaryButtonBase} ${workflow === 'AGENT' ? 'bg-purple-500 text-white shadow-md shadow-purple-500/20' : 'bg-white/80 text-gray-700 border border-gray-200/80 hover:bg-white dark:bg-white/7 dark:text-gray-200 dark:border-gray-700 dark:hover:bg-white/10'}`}
                >
                  <Network className="h-4 w-4" /> Agents
                </button>
                <button
                  onClick={() => handleWorkflowSelection('MCP')}
                  className={`${toolsPrimaryButtonBase} ${workflow === 'MCP' ? 'bg-teal-500 text-white shadow-md shadow-teal-500/20' : 'bg-white/80 text-gray-700 border border-gray-200/80 hover:bg-white dark:bg-white/7 dark:text-gray-200 dark:border-gray-700 dark:hover:bg-white/10'}`}
                >
                  <Cpu className="h-4 w-4" /> MCP
                </button>
                <button
                  onClick={() => handleWorkflowSelection('CREWAI')}
                  className={`${toolsPrimaryButtonBase} ${workflow === 'CREWAI' ? 'bg-sky-500 text-white shadow-md shadow-sky-500/20' : 'bg-white/80 text-gray-700 border border-gray-200/80 hover:bg-white dark:bg-white/7 dark:text-gray-200 dark:border-gray-700 dark:hover:bg-white/10'}`}
                >
                  <CalendarDays className="h-4 w-4" /> LangGraph Planning
                </button>
              </div>

              {workflow === 'MCP' && isMcpMenuExpanded && (
                <div className={`mt-4 ${toolsNestedPanelBase} border-teal-200/80 dark:border-teal-800/60`}>
                  <div className="flex items-center justify-between gap-3 px-1">
                    <div>
                      <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-teal-700 dark:text-teal-300">
                        MCP Tools
                      </div>
                      <div className="mt-1 text-xs text-teal-900/70 dark:text-teal-200/75">
                        Pick the MCP connector you want to message.
                      </div>
                    </div>
                    <span className="rounded-full border border-teal-200/80 bg-teal-50/80 px-2 py-1 text-[9px] font-semibold uppercase tracking-[0.14em] text-teal-600 dark:border-teal-800/70 dark:bg-teal-950/35 dark:text-teal-300">
                      Level 2
                    </span>
                  </div>

                  {(config.mcpTools ?? []).length === 0 ? (
                    <span className="px-1 text-xs text-gray-400 italic">No MCP tools configured. Open Settings to add one.</span>
                  ) : (
                    <div className="flex flex-wrap gap-2">
                      {(config.mcpTools ?? []).map((tool: McpTool) => (
                        <button
                          key={tool.id}
                          onClick={() => handleMcpToolSelection(tool.id)}
                          className={`${toolsSecondaryButtonBase} ${mcpToolId === tool.id ? 'bg-teal-500 text-white shadow-md shadow-teal-500/20' : 'bg-white/85 text-gray-700 border border-teal-200/70 hover:bg-white dark:bg-white/7 dark:text-gray-200 dark:border-teal-800/70 dark:hover:bg-white/10'}`}
                        >
                          <Network className="h-3.5 w-3.5" /> {tool.label}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              )}

              {workflow === 'AGENT' && isAgentMenuExpanded && (
                <div className={`mt-4 ${toolsNestedPanelBase} border-purple-200/80 dark:border-purple-800/60`}>
                  <div className="flex items-center justify-between gap-3 px-1">
                    <div>
                      <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-purple-700 dark:text-purple-300">
                        Agent Selection
                      </div>
                      <div className="mt-1 text-xs text-purple-900/70 dark:text-purple-200/75">
                        Choose a specialist, then the island retracts back into its compact mode.
                      </div>
                    </div>
                    <span className="rounded-full border border-purple-200/80 bg-purple-50/80 px-2 py-1 text-[9px] font-semibold uppercase tracking-[0.14em] text-purple-600 dark:border-purple-800/70 dark:bg-purple-950/35 dark:text-purple-300">
                      Level 2
                    </span>
                  </div>

                  <div className="flex flex-wrap gap-2">
                    <button
                      onClick={() => handleAgentRoleSelection('manager')}
                      className={`${toolsSecondaryButtonBase} ${agentRole === 'manager' ? 'bg-gradient-to-r from-amber-500 to-orange-500 text-white shadow-md shadow-orange-500/25' : 'bg-white/85 text-gray-700 border border-purple-200/70 hover:bg-white dark:bg-white/7 dark:text-gray-200 dark:border-purple-800/70 dark:hover:bg-white/10'}`}
                    >
                      <Star className={`h-3.5 w-3.5 ${agentRole === 'manager' ? 'fill-white text-white' : 'fill-amber-500 text-amber-500'}`} />
                      Agent Manager
                    </button>

                    <button
                      onClick={() => setIsOtherAgentsOpen((open) => !open)}
                      className={`${toolsSecondaryButtonBase} ${isOtherAgentsOpen ? 'bg-purple-500 text-white shadow-md shadow-purple-500/20' : 'bg-white/85 text-gray-700 border border-purple-200/70 hover:bg-white dark:bg-white/7 dark:text-gray-200 dark:border-purple-800/70 dark:hover:bg-white/10'}`}
                    >
                      {isOtherAgentsOpen ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
                      Other agents
                    </button>
                  </div>

                  <div className={`overflow-hidden transition-all duration-300 ease-in-out ${isOtherAgentsOpen ? 'max-h-[24rem] opacity-100' : 'max-h-0 opacity-0'}`}>
                    <div className="rounded-[1.4rem] border border-cyan-200/70 bg-cyan-50/70 p-2.5 dark:border-cyan-800/60 dark:bg-cyan-950/20">
                      <div className="flex flex-wrap gap-2">
                        <button
                          onClick={() => handleAgentRoleSelection('clickhouse_query')}
                          className={`${toolsSecondaryButtonBase} ${agentRole === 'clickhouse_query' ? 'bg-cyan-500 text-white shadow-md shadow-cyan-500/20' : 'bg-white/85 text-gray-700 border border-cyan-200/70 hover:bg-white dark:bg-white/7 dark:text-gray-200 dark:border-cyan-800/70 dark:hover:bg-white/10'}`}
                        >
                          <Database className="h-3.5 w-3.5" /> Clickhouse SQL
                        </button>
                        <button
                          onClick={() => handleAgentRoleSelection('data_analyst')}
                          className={`${toolsSecondaryButtonBase} ${agentRole === 'data_analyst' ? 'bg-violet-500 text-white shadow-md shadow-violet-500/20' : 'bg-white/85 text-gray-700 border border-violet-200/70 hover:bg-white dark:bg-white/7 dark:text-gray-200 dark:border-violet-800/70 dark:hover:bg-white/10'}`}
                        >
                          <Cpu className="h-3.5 w-3.5" /> Data Analyst
                        </button>
                        <button
                          onClick={() => handleAgentRoleSelection('file_management')}
                          onDoubleClick={() => setIsFileManagerConfigOpen(true)}
                          title="Double-click to configure"
                          className={`${toolsSecondaryButtonBase} ${agentRole === 'file_management' ? 'bg-emerald-500 text-white shadow-md shadow-emerald-500/20' : 'bg-white/85 text-gray-700 border border-emerald-200/70 hover:bg-white dark:bg-white/7 dark:text-gray-200 dark:border-emerald-800/70 dark:hover:bg-white/10'}`}
                        >
                          <FolderOpen className="h-3.5 w-3.5" /> File management
                        </button>
                        <button
                          onClick={() => handleAgentRoleSelection('pdf_creator')}
                          className={`${toolsSecondaryButtonBase} ${agentRole === 'pdf_creator' ? 'bg-slate-700 text-white shadow-md shadow-slate-700/20' : 'bg-white/85 text-gray-700 border border-slate-200/70 hover:bg-white dark:bg-white/7 dark:text-gray-200 dark:border-slate-700 dark:hover:bg-white/10'}`}
                        >
                          <File className="h-3.5 w-3.5" /> PDF creator
                        </button>
                        <button
                          onClick={() => handleAgentRoleSelection('oracle_analyst')}
                          className={`${toolsSecondaryButtonBase} ${agentRole === 'oracle_analyst' ? 'bg-orange-500 text-white shadow-md shadow-orange-500/20' : 'bg-white/85 text-gray-700 border border-orange-200/70 hover:bg-white dark:bg-white/7 dark:text-gray-200 dark:border-orange-800/70 dark:hover:bg-white/10'}`}
                        >
                          <Database className="h-3.5 w-3.5" /> Oracle SQL
                        </button>
                        <button
                          onClick={() => handleAgentRoleSelection('data_quality_tables')}
                          className={`${toolsSecondaryButtonBase} ${agentRole === 'data_quality_tables' ? 'bg-fuchsia-500 text-white shadow-md shadow-fuchsia-500/20' : 'bg-white/85 text-gray-700 border border-fuchsia-200/70 hover:bg-white dark:bg-white/7 dark:text-gray-200 dark:border-fuchsia-800/70 dark:hover:bg-white/10'}`}
                        >
                          <BarChart3 className="h-3.5 w-3.5" /> Data quality - Tables
                        </button>
                      </div>
                    </div>
                  </div>

                  {(agentRole === 'file_management' || agentRole === 'data_quality_tables') && (
                    <div className="flex flex-wrap gap-2">
                      {agentRole === 'file_management' && (
                        <button
                          type="button"
                          onClick={() => setIsFileManagerConfigOpen(true)}
                          className="inline-flex items-center justify-center gap-1.5 rounded-full bg-black px-4 py-2 text-xs font-medium text-white transition-colors hover:bg-gray-800"
                        >
                          <Settings className="h-3.5 w-3.5" />
                          Configure
                        </button>
                      )}
                      {agentRole === 'data_quality_tables' && (
                        <button
                          type="button"
                          onClick={openDataQualityModal}
                          className="inline-flex items-center justify-center gap-1.5 rounded-full bg-black px-4 py-2 text-xs font-medium text-white transition-colors hover:bg-gray-800"
                        >
                          <BarChart3 className="h-3.5 w-3.5" />
                          Open form
                        </button>
                      )}
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      <div className="absolute left-5 top-1/2 z-30 hidden -translate-y-1/2 lg:flex">
        <div className="flex flex-col items-start gap-3">
          <button
            type="button"
            onClick={() => {
              setIsZoomControlOpen(false);
              setIsThinkingPanelOpen(false);
              setIsToolsIslandOpen(true);
            }}
            className={`${floatingContextButtonClass} ${activeContextBadge.buttonClass}`}
            title={activeContextBadge.label}
          >
            <div className="flex flex-col items-center gap-0.5">
              <ActiveContextIcon className="h-4.5 w-4.5" />
              <span className="text-[9px] font-semibold tracking-[0.12em] text-white/90">
                {activeContextShortLabel}
              </span>
            </div>
          </button>
          <button
            type="button"
            onClick={() => setIsDraftPanelOpen((open) => !open)}
            className={`${floatingGlassButtonClass} ${isDraftPanelOpen ? 'bg-white/85 ring-1 ring-black/10 dark:bg-black/60 dark:ring-white/10' : ''}`}
            title="Draft zone"
          >
            <div className="flex flex-col items-center gap-0.5">
              <FilePenLine className="h-4.5 w-4.5" />
              <span className="text-[9px] font-semibold tracking-[0.16em] text-gray-500 dark:text-gray-400">
                DRAFT
              </span>
            </div>
          </button>
          <button
            type="button"
            onClick={() => setIsAgentStatePanelOpen((open) => !open)}
            className={`${floatingGlassButtonClass} ${isAgentStatePanelOpen ? 'bg-white/85 ring-1 ring-black/10 dark:bg-black/60 dark:ring-white/10' : ''}`}
            title="Agent state"
          >
            <div className="flex flex-col items-center gap-0.5">
              <Gauge className="h-4.5 w-4.5" />
              <span className="text-[9px] font-semibold tracking-[0.16em] text-gray-500 dark:text-gray-400">
                STATE
              </span>
            </div>
          </button>
        </div>
      </div>

      {isAgentStatePanelOpen && (
        <div className="absolute left-24 top-[8.25rem] z-30 hidden w-[23rem] lg:block">
          <div className="overflow-hidden rounded-[2rem] border border-white/60 bg-white/78 shadow-[0_20px_60px_rgba(15,23,42,0.16)] backdrop-blur-2xl dark:border-white/10 dark:bg-black/40">
            <div className="flex items-start justify-between gap-3 border-b border-black/5 px-4 py-4 dark:border-white/10">
              <div>
                <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">
                  Agent state
                </div>
                <div className="mt-1 text-sm font-semibold text-gray-900 dark:text-gray-100">
                  Transparent execution view
                </div>
                <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                  Live activity, cognitive load indicators, execution trace depth, and confidence.
                </div>
              </div>
              <button
                type="button"
                onClick={() => setIsAgentStatePanelOpen(false)}
                className="inline-flex h-9 w-9 items-center justify-center rounded-full bg-black/5 text-gray-600 transition-colors hover:bg-black/10 dark:bg-white/10 dark:text-gray-300 dark:hover:bg-white/15"
                title="Close agent state"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <div className="space-y-4 px-4 py-4">
              <div className="rounded-[1.45rem] border border-white/70 bg-white/80 p-3 shadow-sm dark:border-white/10 dark:bg-white/5">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-gray-500 dark:text-gray-400">
                      Cognitive load
                    </div>
                    <div className="mt-1 text-sm font-semibold text-gray-900 dark:text-gray-100">
                      {isLoading ? 'Agent actively working' : 'Idle but ready'}
                    </div>
                  </div>
                  <div className={`rounded-full px-2.5 py-1 text-[10px] font-semibold ${isLoading ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-200' : 'bg-slate-100 text-slate-700 dark:bg-slate-800/70 dark:text-slate-200'}`}>
                    {isLoading ? 'LIVE' : 'READY'}
                  </div>
                </div>
                <div className="mt-3 h-2.5 overflow-hidden rounded-full bg-black/8 dark:bg-white/10">
                  <div
                    className={`h-full rounded-full bg-gradient-to-r from-cyan-500 via-blue-500 to-violet-500 ${isLoading ? 'animate-pulse' : ''}`}
                    style={{ width: `${activityProgressValue}%` }}
                  />
                </div>
                <div className="mt-3 space-y-1.5">
                  {activityIndicators.map((label, index) => (
                    <div key={label} className="flex items-center gap-2 text-[12px] text-gray-600 dark:text-gray-300">
                      <span className={`h-1.5 w-1.5 rounded-full ${index === 0 && isLoading ? 'bg-blue-500 shadow-[0_0_10px_rgba(59,130,246,0.5)]' : 'bg-gray-300 dark:bg-gray-600'}`} />
                      <span>{label}</span>
                    </div>
                  ))}
                </div>
              </div>

              <div className="grid grid-cols-2 gap-3">
                <div className="rounded-[1.35rem] border border-white/70 bg-white/80 p-3 shadow-sm dark:border-white/10 dark:bg-white/5">
                  <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-gray-500 dark:text-gray-400">
                    Trace depth
                  </div>
                  <div className="mt-1 text-xl font-semibold text-gray-900 dark:text-gray-100">
                    {latestTraceSteps.length}
                  </div>
                  <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                    step{latestTraceSteps.length > 1 ? 's' : ''} in the latest execution trace
                  </div>
                </div>
                <div className={`rounded-[1.35rem] border p-3 shadow-sm ${confidenceBadge.tone}`}>
                  <div className="text-[10px] font-semibold uppercase tracking-[0.16em] opacity-75">
                    {confidenceBadge.label}
                  </div>
                  <div className="mt-1 text-xl font-semibold">
                    {confidenceBadge.value}
                  </div>
                  <div className="mt-1 text-xs opacity-75">
                    Based on explicit score when available, otherwise inferred from execution quality.
                  </div>
                </div>
              </div>

              <div className="rounded-[1.45rem] border border-white/70 bg-white/80 p-3 shadow-sm dark:border-white/10 dark:bg-white/5">
                <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-gray-500 dark:text-gray-400">
                  Latest visible route
                </div>
                <div className="mt-2 flex flex-wrap items-center gap-2">
                  {breadcrumbNodes.map((node, index) => {
                    const NodeIcon = node.icon;
                    return (
                      <React.Fragment key={`agent-state-${node.id}`}>
                        <span className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1.5 text-[11px] font-medium ${node.toneClass}`}>
                          <NodeIcon className="h-3.5 w-3.5" />
                          {node.label}
                        </span>
                        {index < breadcrumbNodes.length - 1 && (
                          <ChevronRight className="h-3.5 w-3.5 text-gray-400 dark:text-gray-500" />
                        )}
                      </React.Fragment>
                    );
                  })}
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {isDraftPanelOpen && (
        <div className="absolute left-24 top-[6.75rem] bottom-6 z-30 hidden w-[22rem] lg:block">
          <div className="flex h-full flex-col overflow-hidden rounded-[2rem] border border-white/60 bg-white/76 shadow-[0_20px_60px_rgba(15,23,42,0.16)] backdrop-blur-2xl dark:border-white/10 dark:bg-black/40">
            <div className="flex items-start justify-between gap-3 border-b border-black/5 px-4 py-4 dark:border-white/10">
              <div>
                <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">
                  Draft zone
                </div>
                <div className="mt-1 text-sm font-semibold text-gray-900 dark:text-gray-100">
                  Working artifacts
                </div>
                <div className="mt-1 text-xs text-gray-500 dark:text-gray-400">
                  Long-form outputs, code, and reusable notes stay here while the main chat remains focused.
                </div>
              </div>
              <button
                type="button"
                onClick={() => setIsDraftPanelOpen(false)}
                className="inline-flex h-9 w-9 items-center justify-center rounded-full bg-black/5 text-gray-600 transition-colors hover:bg-black/10 dark:bg-white/10 dark:text-gray-300 dark:hover:bg-white/15"
                title="Close draft zone"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
            <div className="flex-1 space-y-3 overflow-y-auto px-4 py-4">
              {draftArtifacts.length === 0 ? (
                <div className="rounded-[1.4rem] border border-dashed border-gray-200/80 bg-white/70 px-4 py-5 text-sm text-gray-500 dark:border-white/10 dark:bg-white/5 dark:text-gray-400">
                  No drafts yet. As soon as the assistant produces a long report, code block, or chart payload, it will appear here.
                </div>
              ) : (
                draftArtifacts.map((artifact) => (
                  <div
                    key={artifact.id}
                    className="rounded-[1.45rem] border border-white/70 bg-white/80 p-3 shadow-sm dark:border-white/10 dark:bg-white/5"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-gray-500 dark:text-gray-400">
                          {artifact.kindLabel}
                        </div>
                        <div className="mt-1 text-sm font-semibold text-gray-900 dark:text-gray-100">
                          {artifact.title}
                        </div>
                      </div>
                      <button
                        type="button"
                        onClick={() => navigator.clipboard.writeText(artifact.content)}
                        className="inline-flex h-8 w-8 items-center justify-center rounded-full bg-black/5 text-gray-600 transition-colors hover:bg-black/10 dark:bg-white/10 dark:text-gray-300 dark:hover:bg-white/15"
                        title="Copy draft"
                      >
                        <Copy className="h-3.5 w-3.5" />
                      </button>
                    </div>
                    <p className="mt-2 text-[12px] leading-relaxed text-gray-600 dark:text-gray-300">
                      {artifact.preview}
                    </p>
                    {artifact.kind === 'sql' && (
                      <div className="mt-3 flex items-center gap-2">
                        <button
                          type="button"
                          onClick={() => openSqlDraftPreview(artifact)}
                          className="inline-flex items-center justify-center rounded-full bg-cyan-500 px-3.5 py-2 text-xs font-semibold text-white shadow-md shadow-cyan-500/20 transition-colors hover:bg-cyan-400"
                        >
                          Open SQL preview
                        </button>
                        <span className="text-[11px] text-gray-500 dark:text-gray-400">
                          Review, edit, run, sort columns, and export.
                        </span>
                      </div>
                    )}
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      )}

      <div
        ref={zoomControlRef}
        className="absolute right-5 top-1/2 z-30 flex -translate-y-1/2 flex-col items-end gap-3"
      >
        {isZoomControlOpen && (
          <div className="glass-panel w-56 rounded-[1.75rem] p-3 shadow-[0_18px_45px_rgba(15,23,42,0.18)] animate-scale-in">
            <div className="flex items-center justify-between px-1 pb-2">
              <div>
                <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">
                  Chat Zoom
                </div>
                <div className="mt-1 text-lg font-semibold text-gray-900 dark:text-gray-100">
                  {chatZoomPercent}%
                </div>
              </div>
              <button
                type="button"
                onClick={() => setChatZoom(CHAT_ZOOM_DEFAULT)}
                className="inline-flex items-center gap-1 rounded-full bg-black/5 px-2.5 py-1.5 text-[11px] font-medium text-gray-600 transition-colors hover:bg-black/10 dark:bg-white/10 dark:text-gray-300 dark:hover:bg-white/15"
              >
                <RotateCcw className="h-3.5 w-3.5" />
                Reset
              </button>
            </div>

            <div className="rounded-[1.5rem] bg-white/60 p-1.5 dark:bg-white/5">
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => setChatZoom((current) => clampChatZoom(current - CHAT_ZOOM_STEP))}
                  disabled={chatZoom <= CHAT_ZOOM_MIN}
                  className="inline-flex h-10 w-10 items-center justify-center rounded-[1.15rem] text-gray-700 transition-colors hover:bg-black/5 disabled:opacity-40 dark:text-gray-200 dark:hover:bg-white/10"
                  title="Zoom out"
                >
                  <Minus className="h-4 w-4" />
                </button>
                <div className="min-w-0 flex-1">
                  <input
                    type="range"
                    min={CHAT_ZOOM_MIN}
                    max={CHAT_ZOOM_MAX}
                    step={CHAT_ZOOM_STEP}
                    value={chatZoom}
                    onChange={(event) => setChatZoom(clampChatZoom(Number(event.target.value)))}
                    className="h-2 w-full cursor-pointer appearance-none rounded-full bg-black/10 accent-black dark:bg-white/10 dark:accent-white"
                    aria-label="Chat zoom"
                  />
                </div>
                <button
                  type="button"
                  onClick={() => setChatZoom((current) => clampChatZoom(current + CHAT_ZOOM_STEP))}
                  disabled={chatZoom >= CHAT_ZOOM_MAX}
                  className="inline-flex h-10 w-10 items-center justify-center rounded-[1.15rem] text-gray-700 transition-colors hover:bg-black/5 disabled:opacity-40 dark:text-gray-200 dark:hover:bg-white/10"
                  title="Zoom in"
                >
                  <Plus className="h-4 w-4" />
                </button>
              </div>
            </div>

            <div className="mt-2 px-1 text-[11px] leading-relaxed text-gray-500 dark:text-gray-400">
              Adjust the message area and composer without changing the rest of the interface.
            </div>
          </div>
        )}

        {isThinkingPanelOpen && (
          <div className="glass-panel w-[28rem] max-w-[calc(100vw-5.5rem)] overflow-hidden rounded-[1.9rem] p-3 shadow-[0_22px_55px_rgba(15,23,42,0.2)] animate-scale-in">
            <div className="flex items-start justify-between gap-3 px-1 pb-3">
              <div>
                <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-gray-500 dark:text-gray-400">
                  Thinking Trace
                </div>
                <div className="mt-1 text-lg font-semibold text-gray-900 dark:text-gray-100">
                  {thinkingMessages.length} response{thinkingMessages.length > 1 ? 's' : ''}
                </div>
              </div>
              <button
                type="button"
                onClick={() => setIsThinkingPanelOpen(false)}
                className="inline-flex h-9 w-9 items-center justify-center rounded-full bg-black/5 text-gray-600 transition-colors hover:bg-black/10 dark:bg-white/10 dark:text-gray-300 dark:hover:bg-white/15"
                title="Close thinking trace"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <div className="max-h-[min(34rem,calc(100vh-8rem))] space-y-3 overflow-y-auto pr-1">
              {thinkingMessages.length === 0 ? (
                <div className="rounded-[1.45rem] border border-dashed border-gray-200/80 bg-white/60 px-4 py-5 text-sm text-gray-500 dark:border-gray-700/80 dark:bg-white/5 dark:text-gray-400">
                  No thinking steps available for this conversation yet.
                </div>
              ) : (
                [...thinkingMessages].reverse().map((message) => (
                  <div
                    key={`thinking-${message.id}`}
                    className="rounded-[1.5rem] border border-white/70 bg-white/65 p-3 shadow-sm dark:border-white/10 dark:bg-white/5"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <div className="text-[11px] font-semibold uppercase tracking-[0.14em] text-gray-500 dark:text-gray-400">
                          {new Date(message.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                        </div>
                        <div className="mt-1 text-sm font-medium text-gray-900 dark:text-gray-100">
                          {compactMessagePreview(message.content)}
                        </div>
                      </div>
                      <div className="rounded-full bg-black/5 px-2.5 py-1 text-[10px] font-semibold text-gray-600 dark:bg-white/10 dark:text-gray-300">
                        {message.steps?.length ?? 0} step{(message.steps?.length ?? 0) > 1 ? 's' : ''}
                      </div>
                    </div>

                    <div className="mt-3 space-y-2.5">
                      {(message.steps ?? []).map((step, index) => (
                        <div
                          key={`${message.id}-${step.id ?? index}`}
                          className="rounded-[1.2rem] border border-gray-200/70 bg-white/75 px-3 py-3 dark:border-gray-700/70 dark:bg-black/20"
                        >
                          <div className="flex items-start justify-between gap-3">
                            <div className="min-w-0">
                              <div className="text-sm font-semibold text-gray-900 dark:text-gray-100">
                                {step.title}
                              </div>
                              <div className="mt-1 flex flex-wrap items-center gap-2 text-[10px]">
                                {stepBadgeLabel(step) && (
                                  <span className="rounded-full bg-purple-100 px-2 py-0.5 font-medium text-purple-700 dark:bg-purple-900/30 dark:text-purple-200">
                                    {stepBadgeLabel(step)}
                                  </span>
                                )}
                                {stepRowCount(step) !== null && (
                                  <span className="text-gray-500 dark:text-gray-400">
                                    {stepRowCount(step)} row(s)
                                  </span>
                                )}
                                {(step as any).retried && (
                                  <span className="text-amber-600 dark:text-amber-300">
                                    Auto-retried
                                  </span>
                                )}
                                {stepSuggestedPath(step) && (
                                  <span className="text-gray-500 dark:text-gray-400">
                                    {stepSuggestedPath(step)}
                                  </span>
                                )}
                              </div>
                            </div>
                            <span
                              className={`mt-0.5 h-2.5 w-2.5 flex-shrink-0 rounded-full ${
                                step.status === 'success'
                                  ? 'bg-emerald-500'
                                  : step.status === 'error'
                                    ? 'bg-red-500'
                                    : step.status === 'running'
                                      ? 'bg-blue-500'
                                      : 'bg-gray-300 dark:bg-gray-600'
                              }`}
                            />
                          </div>

                          {stepReasoning(step) && (
                            <p className="mt-2 text-[12px] leading-relaxed text-gray-700 dark:text-gray-200">
                              {stepReasoning(step)}
                            </p>
                          )}
                          {stepResultSummary(step) && (
                            <p className="mt-1 text-[12px] leading-relaxed text-gray-500 dark:text-gray-400 whitespace-pre-wrap">
                              {stepResultSummary(step)}
                            </p>
                          )}
                          {stepSql(step) && (
                            <div className="mt-2 overflow-hidden rounded-xl border border-gray-200 bg-gray-950/95 dark:border-gray-700">
                              <div className="flex items-center justify-between border-b border-white/10 px-3 py-2">
                                <span className="text-[11px] font-medium uppercase tracking-wide text-gray-400">SQL</span>
                              </div>
                              <pre className="overflow-x-auto px-3 py-3 text-[11px] leading-relaxed text-gray-100">
                                <code>{stepSql(step)}</code>
                              </pre>
                            </div>
                          )}
                          {step.details && !stepReasoning(step) && !stepResultSummary(step) && !stepSql(step) && (
                            <p className="mt-2 whitespace-pre-wrap font-mono text-[11px] leading-relaxed text-gray-500 dark:text-gray-400">
                              {step.details}
                            </p>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>
        )}

        <div className="flex flex-col items-end gap-3">
          <button
            type="button"
            onClick={() => {
              if (isToolsIslandOpen) {
                collapseToolsIsland();
              } else {
                setIsZoomControlOpen(false);
                setIsThinkingPanelOpen(false);
                setIsToolsIslandOpen(true);
                setIsAgentMenuExpanded(false);
                setIsMcpMenuExpanded(false);
                setIsOtherAgentsOpen(false);
              }
            }}
            className={`${floatingGlassButtonClass} ${isToolsIslandOpen ? 'bg-white/85 ring-1 ring-black/10 dark:bg-black/60 dark:ring-white/10' : ''}`}
            title="Tools"
          >
            <Hammer className="h-4.5 w-4.5" />
          </button>
          <button
            type="button"
            onClick={() => setIsConsoleOpen(true)}
            className={floatingGlassButtonClass}
            title="Agent Console"
          >
            <div className="flex flex-col items-center gap-0.5">
              <Terminal className="h-4.5 w-4.5" />
              <span className="text-[9px] font-semibold tracking-[0.16em] text-gray-500 dark:text-gray-400">
                LOG
              </span>
            </div>
          </button>
          <button
            type="button"
            onClick={() => {
              setIsThinkingPanelOpen((open) => !open);
              setIsZoomControlOpen(false);
            }}
            className={`${floatingGlassButtonClass} ${isThinkingPanelOpen ? 'bg-white/85 ring-1 ring-black/10 dark:bg-black/60 dark:ring-white/10' : ''}`}
            title="Thinking trace"
          >
            <div className="flex flex-col items-center gap-0.5">
              <BrainCircuit className="h-4.5 w-4.5" />
              <span className="text-[9px] font-semibold tracking-[0.16em] text-gray-500 dark:text-gray-400">
                THINK
              </span>
            </div>
          </button>
          <button
            type="button"
            onClick={() => {
              setIsZoomControlOpen((open) => !open);
              setIsThinkingPanelOpen(false);
            }}
            className={floatingGlassButtonClass}
            title="Chat zoom"
          >
            <div className="flex flex-col items-center gap-0.5">
              <ZoomIn className="h-4.5 w-4.5" />
              <span className="text-[9px] font-semibold tracking-[0.16em] text-gray-500 dark:text-gray-400">
                {chatZoomPercent}
              </span>
            </div>
          </button>
        </div>
      </div>
      </div>

      <PlanningModal
        isOpen={isPlanningModalOpen}
        onClose={() => {
          setIsPlanningModalOpen(false);
          setIsPlanningDraftDirty(false);
        }}
        draft={plannerDraft}
        editingPlanId={editingPlanningPlanId}
        planningState={planningState}
        isBusy={planningBusy}
        error={planningError}
        onDraftChange={(draft) => {
          setIsPlanningDraftDirty(true);
          setPlannerDraft(normalizeCrewPlanDraft(draft, browserTimeZone));
        }}
        onStartNewDraft={startNewPlanningDraft}
        onSavePlan={savePlanningPlan}
        onEditPlan={editPlanningPlan}
        onTogglePlanStatus={togglePlanningPlanStatus}
        onDeletePlan={deletePlanningPlan}
        onRunPlan={runPlanningPlan}
        onRefresh={loadPlanningState}
      />
      <PlanningMonitorModal
        isOpen={isPlanningMonitorOpen}
        onClose={() => setIsPlanningMonitorOpen(false)}
        planningState={planningState}
        isBusy={planningBusy}
        onRefresh={loadPlanningState}
      />
      <FileManagerConfigModal
        isOpen={isFileManagerConfigOpen}
        config={config.fileManagerConfig}
        onClose={() => setIsFileManagerConfigOpen(false)}
        onSave={(nextConfig: FileManagerAgentConfig) => {
          onConfigChange(normalizeAppConfig({
            ...config,
            fileManagerConfig: nextConfig,
          }));
          setIsFileManagerConfigOpen(false);
        }}
      />
      <DataQualityModal
        isOpen={isDataQualityModalOpen}
        isBusy={isLoading}
        isLoadingMetadata={isDataQualityMetadataLoading}
        error={dataQualityFormError}
        tables={dataQualityTables}
        schema={dataQualitySchema}
        selectedTable={dataQualityForm.table}
        selectedColumns={dataQualityForm.columns}
        sampleSize={dataQualityForm.sampleSize}
        rowFilter={dataQualityForm.rowFilter}
        timeColumn={dataQualityForm.timeColumn}
        onClose={() => setIsDataQualityModalOpen(false)}
        onRefreshMetadata={() => loadDataQualityMetadata(dataQualityForm.table, dataQualityForm.columns)}
        onTableChange={(table) => {
          setDataQualityForm((prev) => ({
            ...prev,
            table,
            columns: [],
            timeColumn: null,
          }));
          void loadDataQualityMetadata(table);
        }}
        onColumnsChange={(columns) => {
          const uniqueColumns = Array.from(new Set(columns));
          setDataQualityForm((prev) => ({
            ...prev,
            columns: uniqueColumns,
          }));
        }}
        onSampleSizeChange={(sampleSize) => {
          setDataQualityForm((prev) => ({ ...prev, sampleSize }));
        }}
        onRowFilterChange={(rowFilter) => {
          setDataQualityForm((prev) => ({ ...prev, rowFilter }));
        }}
        onTimeColumnChange={(timeColumn) => {
          setDataQualityForm((prev) => ({ ...prev, timeColumn }));
        }}
        onSubmit={handleDataQualityRun}
        onStop={stopCurrentExecution}
      />
      <SqlDraftModal
        isOpen={isSqlDraftModalOpen}
        artifactTitle={selectedSqlDraft?.title || 'SQL draft'}
        sql={sqlDraftText}
        engine={sqlDraftEngine}
        rowLimit={sqlDraftRowLimit}
        isLoading={isSqlDraftRunning}
        error={sqlDraftError}
        result={sqlDraftResult}
        onClose={closeSqlDraftPreview}
        onSqlChange={setSqlDraftText}
        onEngineChange={setSqlDraftEngine}
        onRowLimitChange={setSqlDraftRowLimit}
        onRun={runSqlDraftPreview}
      />
      <AgentConsoleModal
        isOpen={isConsoleOpen}
        onClose={() => setIsConsoleOpen(false)}
        isDark={isDark}
      />
    </div>
  );
}
