# ODIN AI Portal — RAGnarok ⚡

A privacy-first AI workspace combining **pure LLM chat**, **Retrieval-Augmented Generation (RAG)** with OpenSearch, **specialist agents** orchestrated by an **Agent Manager**, **LangGraph-based planning**, and **MCP (Model Context Protocol)** tool integration — powered by Ollama or any OpenAI-compatible server, with durable app state persisted by the Python backend.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Interaction Modes](#interaction-modes)
- [State Persistence & Backup](#state-persistence--backup)
- [CrewAI Planning](#crewai-planning)
- [RAG Pipeline](#rag-pipeline)
- [ClickHouse SQL Agent](#clickhouse-sql-agent)
- [Data Analyst Agent](#data-analyst-agent)
- [Auto-ML Agent](#auto-ml-agent)
- [Data Cleaner Agent](#data-cleaner-agent)
- [Anonymizer Agent](#anonymizer-agent)
- [Oracle SQL Agent](#oracle-sql-agent)
- [File Management Agent](#file-management-agent)
- [PDF Creator Agent](#pdf-creator-agent)
- [Custom Python Agents](#custom-python-agents)
- [Agents & Tools App Portal](#agents--tools-app-portal)
- [MCP Integration](#mcp-integration)
- [Configuration](#configuration)
- [Production Build](#production-build)
- [Privacy](#privacy)

---

## Features

| Category | Details |
|----------|---------|
| **5 chat modes** | Pure LLM, RAG, Agents, MCP Tools, LangGraph Planning |
| **9 built-in agents** | Agent Manager, ClickHouse SQL, Data Analyst, Auto-ML, Data Cleaner, Anonymizer, Oracle SQL, File management, PDF creator |
| **Custom Python agents** | Paste Python code in Settings, let the local LLM generate a complete agent profile, then enable or disable the new agent from the UI |
| **Manager orchestration** | Agent Manager can delegate to every built-in specialist plus enabled custom agents, keep follow-up context across clarifications and confirmations, and optionally preload functional context from RAG before routing |
| **OpenSearch backend** | kNN vector search (HNSW/cosinesimil), index setup & document ingest from the UI |
| **ClickHouse agent** | Table inference, schema inspection, ambiguity clarification via clickable tiles, safe read-only SQL generation, optional chart rendering |
| **Data Analyst agent** | Multi-step ClickHouse investigations with iterative evidence gathering, optional knowledge-base search, automatic SQL simplification retry, and CSV export on demand |
| **Auto-ML agent** | Guided model benchmarking flow with target-column selection, optional row scope / WHERE clause, sample-size cap, comparison table, and baseline recommendation |
| **Data Cleaner agent** | Guided ClickHouse data-quality audit for duplicates, missing values, empty strings, and inconsistent formats, with optional WHERE scope and suggested SQL cleanup scripts |
| **Anonymizer agent** | Guided ClickHouse privacy scan for likely PII columns, with optional WHERE scope, masking / hashing SQL suggestions, and a governance-oriented summary |
| **Oracle agent** | Natural-language Oracle analysis, schema discovery, SQL validation, automatic repair on query errors, narrative Markdown output |
| **File management agent** | Backend Python-only ReAct loop for file browsing, reading, creating, editing, moving and guarded destructive actions, with early stop once the requested filesystem change is complete |
| **PDF creator agent** | Backend Python-only PDF export agent to turn the latest useful analysis or pasted content into a polished document |
| **Planner / scheduler** | Schedule existing agents on fixed frequency, ClickHouse watch, or file-arrival trigger |
| **MCP tools** | Connect any MCP server via the Python MCP SDK, test connection, real agentic tool-call loop over SSE or streamable HTTP |
| **Backend persistence** | Shared app config plus user-isolated conversations and preferences stored in backend-managed `DB.json` |
| **Multi-user isolation** | Each browser/client keeps its own conversations and UI state while everyone shares the same server configuration |
| **Backup workflow** | Export/import DB backups and force a resync from the latest backend state in Settings |
| **Agents & Tools portal** | Configurable external application tiles managed in Settings and rendered as a glass-style portal page from the landing screen |
| **Conversation memory** | Current conversation keeps a short backend-synced working memory window (at least 5 recent steps, currently 10 useful messages) |
| **Markdown & HTML** | Syntax-highlighted code blocks, proper tables, raw HTML from LLMs, copy button |
| **Tabular answers on demand** | Pure LLM plus all SQL / analyst / file-oriented agents can return a compact Markdown table when the user explicitly asks for rows, columns, schema lists, previews, or tabular output |
| **Clickable clarifications** | Agent choices are rendered as large clickable tiles in the chat instead of requiring manual retyping when possible |
| **Chat zoom** | Floating Apple-inspired zoom control on the right side of the chat with fine-grained scaling |
| **Compact answer layout** | Main answer stays visible while SQL, reasoning, previews, and other technical appendices collapse into expandable details blocks |
| **Reading-first chat scroll** | New assistant responses scroll into view from their top edge instead of snapping to the very bottom of the thread |
| **Stable agent intro cards** | Agent descriptions use higher-contrast cards in the chat and disappear on first typing without the previous visual shimmer |
| **Floating tools island** | Pure LLM / RAG / Agents / MCP / LangGraph Planning are opened from a compact hammer button in the right-side utility dock |
| **Discreet console access** | Agent Console is exposed as a minimal floating button in the right-side utility dock |
| **Apple-inspired landing** | Light, modern black/white/slate cards with restrained accent colors, animated hover, contact modal, and page routing |
| **Dark mode** | Full dark/light toggle persisted through backend state sync |
| **File attachments** | Images, PDFs, text files alongside messages |
| **Conversation history** | Multi-session sidebar, persisted in backend state with browser fallback cache |
| **Protected settings access** | Discreet Settings entry on the landing page, protected by a password (`MM@2026` by default, configurable in-app) |
| **Settings panel** | All parameters configurable in-app, no `.env` required |

---

## Architecture

```
┌────────────────────────────────────────────────────────┐
│  Browser  (React 19 + Vite + Tailwind CSS)             │
│                                                        │
│  Landing Page ──► RAGnarok Chat                        │
│                   ┌──────────────────────────────────┐ │
│                   │ LLM │ RAG │ Agents │ MCP │ CrewAI│ │
│                   └──────┬───────┬──────────┬────────┘ │
└──────────────────────────┼───────┼──────────┼──────────┘
                           │       │          │
                    direct │  /api/│chat/rag  │/api/chat/mcp
                    call   │       │          │
                           │  ┌────▼─────────────────────────┐
                           │  │     server.py (FastAPI)      │
                           │  │                              │
                           │  │  /api/db/state               │
                           │  │  /api/db/export              │
                           │  │  /api/db/import              │
                           │  │  /api/opensearch/test        │
                           │  │  /api/opensearch/setup       │
                           │  │  /api/documents/ingest       │
                           │  │  /api/chat/rag               │
                           │  │  /api/clickhouse/test        │
                           │  │  /api/chat/clickhouse-agent  │
                           │  │  /api/chat/data-analyst-agent│
                           │  │  /api/chat/file-manager-agent│
                           │  │  /api/chat/pdf-creator-agent │
                           │  │  /api/oracle/test            │
                           │  │  /api/chat/oracle-analyst... │
                           │  │  /api/clickhouse/guide-metadata │
                           │  │  /api/chat/feature-engineer...│
                           │  │  /api/chat/auto-ml-agent     │
                           │  │  /api/chat/data-cleaner-agent│
                           │  │  /api/chat/anonymizer-agent  │
                           │  │  /api/custom-agent/analyze   │
                           │  │  /api/chat/custom-agent      │
                           │  │  /api/chat/manager-agent     │
                           │  │  /api/planning/state         │
                           │  │  /api/planning/plans/*       │
                           │  │  /api/mcp/test               │
                           │  │  /api/chat/mcp               │
                           │  └────┬──────────┬────────┬─────┘
                           │       │          │        │
                    ┌──────▼───────▼─┐   ┌────▼──────┐ │
                    │  Ollama / LLM  │   │ OpenSearch│ │
                    │  llama3        │   │ kNN index │ │
                    │  nomic-embed   │   └───────────┘ │
                    └────────────────┘                 │
                                                ┌──────▼─────────┐
                                                │  ClickHouse     │
                                                │  analytics DB   │
                                                └─────────────────┘
                                                    MCP Server
                                               ┌────▼──────────────┐
                                               │ any MCP server    │
                                               │ SSE or HTTP       │
                                               └───────────────────┘
```

- **Frontend** — React 19 + TypeScript + Tailwind CSS, bundled with Vite.
- **Backend** — Python FastAPI (`server.py`) — multi-user state persistence, RAG pipeline, agent orchestration, ClickHouse specialists, file-management agent, planner, MCP client, and OpenSearch management.
- **Vector store** — OpenSearch with `opensearch-py`.
- **Analytics store** — ClickHouse over HTTP for the SQL agent.
- **Oracle store** — Oracle via Python `oracledb` for the Oracle SQL agent.
- **LLM / Embeddings** — Ollama (local) or any OpenAI-compatible API.

---

## Requirements

### System

- **Python 3.11+**
- **Node.js 18+**
- **[Ollama](https://ollama.com)** (or any OpenAI-compatible server)
- **[OpenSearch](https://opensearch.org)** running locally or remotely (for RAG mode)
- **[ClickHouse](https://clickhouse.com)** running locally or remotely (optional, for the ClickHouse SQL agent)
- **Oracle Database** reachable from the backend (optional, for the Oracle SQL agent)

### Ollama models

```bash
ollama pull llama3            # Chat, HyDE, reranking
ollama pull nomic-embed-text  # Text embeddings for RAG
```

Any other Ollama model works — select it in the Settings panel.

---

## Getting Started

### 1. Clone

```bash
git clone <repo-url>
cd RAG-Chat
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Start the backend

```bash
python server.py
# Listening on http://localhost:8000
```

On first run, the backend also creates a local `DB.json` file at the project root to persist shared application configuration plus user-isolated chat history and preferences.

### 4. Install frontend dependencies and start dev server

```bash
npm install
npm run dev
# http://localhost:5173
```

The Vite dev server proxies all `/api/*` requests to `localhost:8000` automatically.

---

## Interaction Modes

### Pure LLM

Direct chat with any Ollama or OpenAI-compatible model. Supports:
- File attachments (images rendered inline, files appended as context)
- System prompt customisation
- Multi-turn conversation history

### RAG Knowledge

Full Retrieval-Augmented Generation pipeline backed by OpenSearch:

1. Configure OpenSearch URL + credentials in **Settings → RAG & OpenSearch**.
2. Click **Test Connection** to verify.
3. Click **Setup Index** to create the kNN index (one time).
4. Paste or load a document in **Index a Document** → click **Index Document**.
5. Switch to **RAG Knowledge** mode in the chat and ask questions.

Answers include cited sources `[1]`, `[2]` and a confidence score.

### Agents

Multi-agent orchestration with built-in specialists plus optional custom Python agents:

| Role | Behaviour |
|------|-----------|
| **Agent Manager** | Routes requests to specialist agents, keeps conversation state, continues delegated follow-ups automatically, and can optionally query the RAG knowledge base first for business definitions and field descriptions |
| **ClickHouse SQL** | Infers the best table when possible, asks for table/field/date choices only when ambiguous, runs safe read-only SQL, can produce charts, and can show result sets or schema lists as Markdown tables on demand |
| **Data Analyst** | Runs deeper multi-step ClickHouse investigations, can search configured knowledge sources, retries failed SQL automatically, can export the latest dataset to CSV, and can surface a visible result table when requested |
| **Auto-ML** | Opens a guided setup to choose the ClickHouse table, target column, optional row scope and sample-size cap, then benchmarks baseline models and returns a comparison table |
| **Data Cleaner** | Opens a guided setup to choose one ClickHouse table, define an optional WHERE scope, then audit duplicate risk, missing values, empty strings, and mixed date formats |
| **Anonymizer** | Opens a guided setup to choose one ClickHouse table, define an optional WHERE scope, then scan for likely PII exposure and suggest masking or hashing SQL patterns |
| **Oracle SQL** | Queries Oracle from natural language, inspects schema, validates Oracle SQL, executes safe read-only queries, answers in business-facing Markdown, and preserves a readable data table in the final answer |
| **File management** | Uses backend Python tools to inspect and manage files safely, with explicit confirmation for overwrite/delete/move operations and tabular output when a file summary is naturally table-shaped |
| **PDF creator** | Turns the latest useful analysis or pasted content into a polished PDF with guarded overwrite confirmation |
| **Custom Agent** | Generated from pasted Python code in Settings, analyzed by the local LLM, optionally enabled in the Agents menu, and callable by the Agent Manager |

#### ClickHouse SQL agent quick flow

1. Configure ClickHouse in **Settings → ClickHouse SQL**.
2. Switch to **Agents** mode and select **ClickHouse SQL**.
3. Ask the analytical question directly — the agent tries to infer the best table automatically.
4. If multiple tables, fields or date columns are plausible, it asks the user to choose with clickable task-tile options.
5. It generates a read-only ClickHouse query, validates it, executes it, and returns:
   - a short final answer in English,
   - a Markdown result table when you explicitly ask for a tabular view,
   - the executed SQL,
   - a concise reasoning summary,
   - and, when relevant, an inline chart or a chart suggestion.

The ClickHouse agent uses the backend only and always relies on the configured local/application LLM endpoint for planning and summarisation.

#### Data Analyst quick flow

1. Configure ClickHouse in **Settings → ClickHouse SQL**.
2. Optionally configure OpenSearch + embeddings if you want the agent to use knowledge-base lookups as part of the analysis.
3. Switch to **Agents** mode and select **Data Analyst**.
4. Ask a deeper analytical question, for example:
   - root-cause analysis,
   - multi-angle comparison,
   - retention / cohort / driver investigation,
   - or a request that should end with a CSV export.
5. The backend agent can:
   - infer the best primary table or ask for a clickable table choice when needed,
   - inspect schema,
   - run several distinct ClickHouse queries in sequence,
   - search the knowledge base when it helps,
   - retry failed SQL with a simpler alternative without spending an extra credited step,
   - export the latest dataset to CSV when explicitly requested,
   - and return a business-facing Markdown answer with a visible result table when you ask for one.

#### Auto-ML quick flow

1. Configure ClickHouse once in **Settings → ClickHouse SQL**.
2. Switch to **Agents** mode and select **Auto-ML**.
3. A guided setup opens automatically on a fresh conversation.
4. Choose:
   - the training table,
   - the target column,
   - an optional row scope (`WHERE`) that can be suggested with the local LLM,
   - an explicit sample-row cap such as `1000`,
   - the benchmark objective,
   - optional analyst notes.
5. Launch the run to receive:
   - a model comparison table,
   - the recommended baseline model,
   - a practical narrative on how to proceed.

#### Data Cleaner / Anonymizer quick flow

1. Configure ClickHouse once in **Settings → ClickHouse SQL**.
2. Switch to **Agents** mode and select **Data Cleaner** or **Anonymizer**.
3. A guided setup opens automatically on a fresh conversation, and you can reopen it later with **Open guide**.
4. Choose:
   - the source table,
   - an optional row scope (`WHERE`) that can be suggested with the local LLM,
   - the functional objective,
   - optional notes for the agent.
5. Launch the run to receive a business-facing summary in the chat plus the masked technical details and SQL patterns underneath.

#### Oracle SQL quick flow

1. Configure Oracle in **Settings → Oracle SQL**.
2. Add one or more Oracle connections and use **Test Oracle** to verify access and preview tables.
3. Switch to **Agents** mode and select **Oracle SQL**.
4. Ask a business question in English.
5. The backend agent can:
   - list accessible Oracle tables,
   - inspect schema,
   - validate the SQL with Oracle-friendly checks,
   - execute the query,
   - repair the SQL automatically when Oracle returns a recoverable error,
   - and return an English Markdown answer with executive summary, key metrics, SQL used, preview table, insights, actions performed, and confidence score.

#### File management quick flow

1. Switch to **Agents** mode and select **File management**.
2. Optionally double-click the agent chip or click **Configure** to set:
   - sandbox `base_path`,
   - max iterations,
   - custom system prompt.
3. Ask to list, read, create, move, edit or delete files.
4. The backend Python agent plans one tool at a time and asks for confirmation before overwrite/delete/move actions.

#### PDF creator quick flow

1. Switch to **Agents** mode and select **PDF creator**.
2. Ask to export the latest useful analysis, or paste the content to turn into a PDF.
3. The backend Python agent prepares a clean export with a professional layout.
4. If the target PDF path already exists, the agent asks for explicit confirmation before overwrite.

### CrewAI Planning

The **CrewAI - Planning** mode is a backend Python scheduler for existing agents.

It supports:

- fixed schedules (`once`, `daily`, `weekly`, `interval`),
- ClickHouse watch triggers based on a read-only SQL result,
- file-arrival triggers on a watched directory,
- pause/resume, edit, delete and `Run now`,
- recent run history in the planner overlay.

You can describe an automation in natural language in the chat or open the dedicated planner form and configure it manually.

---

## State Persistence & Backup

RAGnarok now uses a backend-managed `DB.json` file as its durable source of truth.

What is stored there:

- shared application configuration,
- per-user conversation history,
- per-user durable UI preferences such as dark mode, selected workflow and current conversation,
- agent state needed for guided workflows,
- planner state and saved jobs,
- short conversation memory used to preserve recent context across follow-up turns.

### Sync behaviour

- On startup, the frontend fetches the latest state from the backend.
- Each browser/client receives a stable local user identifier and only reads/writes its own conversations and preferences.
- Application configuration remains shared across all users connected to the same deployment.
- If the backend DB is empty but the browser still has legacy data, the app migrates that state into `DB.json`.
- If the backend still contains an older single-user snapshot, it is migrated automatically to the new multi-user structure.
- The app re-syncs when the window regains focus or becomes visible again.
- The browser still keeps a lightweight fallback cache, but the backend DB is the source of truth.
- Each conversation also keeps a short rolling memory window to preserve current context for backend agents.

### Backup tools

In **Settings → DB Backup**, you can:

- export the current backend DB as JSON,
- import a previous JSON backup,
- force a manual resync from the backend.

### MCP Tools

Connect any [Model Context Protocol](https://modelcontextprotocol.io) server and have the LLM call its tools autonomously:

1. Open **Settings → MCP Tools**.
2. Add a tool: give it a label and its MCP endpoint URL (for example `http://localhost:3000/sse` or a streamable HTTP endpoint).
3. Click **Test** — available tools appear as badges on success.
4. In the chat, switch to **MCP** mode and select the tool.
5. Send a message — the backend runs the full agentic loop (list tools → LLM decides → call tool via MCP → feed result → final answer). Tool calls are visible in the collapsible "Agent Thinking Process" panel.

---

## RAG Pipeline

```
User query
    │
    ▼
HyDE  ── LLM generates a hypothetical answer to improve embedding quality
    │
    ▼
Embed query  ── nomic-embed-text (or configured model)
    │
    ▼
kNN search  ── OpenSearch HNSW (cosinesimil), top-K candidates
    │
    ▼
Keyword boost  ── Python-side TF score blended 70 % vector / 30 % keyword
    │
    ▼
LLM reranking  ── scores each chunk 0–10, re-sorts, filters score < 0.3
    │
    ▼
Cited generation  ── LLM answers with [1][2] citations
    │
    ▼
Response + sources + confidence score
```

Notes:
- The same embedding endpoint is used for document chunk vectorization and live user-query vectorization.
- Direct embedding URLs such as `https://host/v1/embeddings` are supported.
- When possible, model discovery is derived automatically from the sibling `/models` endpoint.

---

## ClickHouse SQL Agent

The ClickHouse SQL agent is designed to follow a conservative analytics workflow instead of guessing columns blindly.

### Workflow

```text
User enters request
    ↓
Agent tries to infer the best ClickHouse table
    ↓
If ambiguous: user selects a table via clickable tiles
    ↓
Backend inspects system.columns for that table
    ↓
Local LLM maps request to likely business fields
    ↓
If ambiguous: user chooses field and/or date column via clickable tiles
    ↓
Local LLM generates one safe read-only SQL query
    ↓
Backend validates and executes the query in ClickHouse
    ↓
Local LLM writes final answer in English
    ↓
Optional chart generation when requested or clearly useful
```

### Safety / best-practice behaviour

- Only read-only `SELECT` / `WITH ... SELECT` style queries are accepted.
- Multi-statement SQL is rejected.
- Destructive keywords such as `DROP`, `ALTER`, `DELETE`, `INSERT`, `TRUNCATE`, etc. are blocked.
- The backend enforces a result limit for row-oriented queries.
- The agent tries to repair SQL once if ClickHouse returns an execution error.
- The response stays short and includes both the SQL and a reasoning summary for auditability.
- Clarification choices are rendered as clickable tiles in the chat whenever possible.
- The agent can suggest or render a chart directly in the conversation.

### Backend endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /api/clickhouse/test` | Test ClickHouse connectivity and preview available tables |
| `POST /api/chat/clickhouse-agent` | Guided ClickHouse agent workflow: discover tables, inspect schema, clarify fields/dates, generate and execute SQL |

---

## Data Analyst Agent

The Data Analyst agent is a deeper ClickHouse specialist designed for complex investigations that need more than one SQL query.

### Workflow

```text
User enters a complex analytical request
    ↓
Agent infers the best primary ClickHouse table
    ↓
If ambiguous: user selects a table via clickable tiles
    ↓
Backend inspects the selected table schema
    ↓
Local LLM decides the next action:
    - query
    - search_knowledge
    - export_csv
    - finish
    ↓
Each successful step adds new evidence to the running context
    ↓
If a query fails, the agent generates a simpler repaired SQL automatically
    ↓
When enough evidence is gathered, the agent writes the final answer in English
```

### Behaviour

- Uses a credited action budget with an anti-loop cap, now defaulting to up to **10** credited analytical actions.
- Keeps the ongoing analytical context inside the same conversation.
- Runs a stronger ReAct-style loop: inspect evidence, form the next hypothesis, execute the next action, then continue until the analysis is genuinely mature.
- Allows simple subqueries / derived tables when they make the analysis clearer or more correct.
- Forces date filtering toward explicit `BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'` windows instead of complex date helper functions.
- Shows the analytical steps directly in the chat, including reasoning, SQL, result summary, row count, and retry status.
- Can search the configured knowledge base when OpenSearch is available.
- Only exports CSV when the user explicitly asks for it.
- Produces a more developed business-facing Markdown answer, plus a data preview, confidence signal, and expandable technical appendices.

### Backend endpoint

| Endpoint | Description |
|----------|-------------|
| `POST /api/chat/data-analyst-agent` | Multi-step ClickHouse analyst loop with table selection, iterative queries, knowledge lookup, SQL retry, and optional CSV export |

---

## Auto-ML Agent

The Auto-ML agent benchmarks several baseline models on top of a ClickHouse dataset and returns a practical recommendation.

### Guided UX

- A dedicated guide opens over the chat on a fresh session.
- The user chooses the source table and then the prediction target column.
- A live schema preview helps the user understand the available fields before launching the benchmark.
- Business framing and analyst notes can be added before running the comparison.

### Backend endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /api/clickhouse/guide-metadata` | Load ClickHouse tables, schema preview, and candidate targets for guided agent setup |
| `POST /api/auto-ml/filter-suggestion` | Ask the local LLM to suggest a safe row-scope clause (`WHERE`) for the Auto-ML run |
| `POST /api/chat/auto-ml-agent` | Benchmark baseline models on ClickHouse data and return a comparison table with recommendation |

---

## Data Cleaner Agent

The Data Cleaner agent is a ClickHouse-focused audit specialist for practical cleanup work.

### What it checks

- duplicate risk based on likely business keys,
- null-heavy or empty-string-heavy columns,
- mixed string date conventions such as `YYYY-MM-DD` vs `DD/MM/YYYY`,
- and a sampled preview of the table under review.

### Output

- a business-facing quality summary,
- prioritized findings,
- and suggested SQL cleanup scripts kept in technical details.

### Backend endpoint

| Endpoint | Description |
|----------|-------------|
| `POST /api/chat/data-cleaner-agent` | Run a guided ClickHouse data-quality audit and return findings plus suggested cleanup SQL |

---

## Anonymizer Agent

The Anonymizer agent helps assess privacy risk before data is shared or reused.

### What it does

- inspects schema and sampled values,
- flags likely PII candidates such as emails, phone numbers, names, addresses, dates of birth, IP addresses, and identifiers,
- and proposes hashing / masking SQL patterns adapted to the detected signal.

### Output

- a privacy-oriented narrative in English,
- a list of PII findings,
- and suggested masking / hashing SQL patterns in technical details.

### Backend endpoint

| Endpoint | Description |
|----------|-------------|
| `POST /api/chat/anonymizer-agent` | Scan a ClickHouse table for likely PII exposure and return masking guidance plus suggested SQL |

---

## Oracle SQL Agent

The Oracle SQL agent is designed for Oracle-specific natural-language analytics with a narrative business-facing answer.

### Workflow

```text
User enters Oracle business question
    ↓
Agent lists accessible tables when needed
    ↓
Agent inspects Oracle schema
    ↓
Local LLM generates Oracle-optimized read-only SQL
    ↓
Backend validates query via Oracle-safe explain / parse strategy
    ↓
Backend executes the query
    ↓
If Oracle returns an error, the agent retries with an automatic SQL repair
    ↓
Local LLM writes final Markdown answer in English
```

### Output

- Executive summary
- Key metrics
- SQL used
- Preview data table
- Insights and recommendations
- Actions performed
- Confidence score

### Backend endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /api/oracle/test` | Test Oracle connectivity and preview accessible tables |
| `POST /api/chat/oracle-analyst-agent` | Oracle SQL workflow: discover tables, inspect schema, validate SQL, execute query, summarize result |

---

## File Management Agent

The File Management agent is a backend Python-only ReAct loop focused on safe filesystem work.

### Capabilities

- Navigation: list directories, inspect paths, search files
- Reading: text files, CSV/TSV summaries, Word, Parquet, Excel sheets
- Creation: directories, text files, Excel workbooks
- Edition: overwrite files, update Excel sheets/cells, append rows
- Move / delete: explicit confirmation required before execution
- Tabular answers: when the user asks for rows, columns, summaries, or previews in table form, the agent can return a compact Markdown table

### Safety

- Optional sandboxing through a configurable `base_path`
- Path traversal blocked through resolved paths
- Confirmation flow for destructive or overwrite-style actions
- Iteration cap to avoid infinite loops
- After a successful mutating action, the agent now performs a completion check and stops early when the user request is already satisfied

### Backend endpoint

| Endpoint | Description |
|----------|-------------|
| `POST /api/chat/file-manager-agent` | Runs the file-management ReAct loop and guarded tool execution |

---

## PDF Creator Agent

The PDF Creator agent is a backend Python-only export agent built to turn chat results into a clean, shareable PDF.

### Capabilities

- Reuses the latest useful assistant result in the chat
- Accepts pasted content when no reusable result is available
- Applies a professional layout aligned with the UI tone
- Requires confirmation before overwriting an existing PDF target

### Backend endpoint

| Endpoint | Description |
|----------|-------------|
| `POST /api/chat/pdf-creator-agent` | Builds a professional PDF from the latest relevant analysis or explicit content |

---

## Custom Python Agents

RAGnarok can extend itself with additional Python-defined agents from the Settings panel.

### Workflow

1. Open **Settings → Custom Agents**.
2. Add a custom agent and paste the Python code that describes its behavior.
3. Click **Analyze & build**.
4. The local LLM generates:
   - a title,
   - a user-facing description,
   - a dedicated system prompt,
   - a manager routing hint,
   - and an implementation status message.
5. Review or edit the generated metadata.
6. Enable the agent to expose it in **Agents → Other agents**.

### Runtime behavior

- Custom agents stay backend-only and use the configured local/application LLM.
- Their chat intro is generated from the saved title and description.
- The Agent Manager can delegate to them when their routing hint or title matches the user’s request.
- They can be disabled at any time to hide them from the agent picker without deleting their implementation draft.

### Backend endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /api/custom-agent/analyze` | Analyze pasted Python code with the local LLM and generate a coherent custom-agent profile |
| `POST /api/chat/custom-agent` | Execute the selected custom agent against the current conversation using its saved Python specification |

---

## Agents & Tools App Portal

The **Agents & Tools** card on the landing page now opens a dynamic application portal instead of a placeholder page.

### How it works

1. Open **Settings → App Portal**.
2. Add as many application tiles as needed.
3. For each tile, define:
   - application name,
   - target URL,
   - hover-only description.
4. Save the settings.
5. From the landing page, open **Agents & Tools** to browse the configured tiles.

### UI behaviour

- Tiles use the same Apple-inspired / liquid-glass design language as the rest of the app.
- The description appears only on hover.
- Clicking a tile opens the target application in a new browser tab.
- The portal content is persisted in the backend `DB.json` state.

---

## MCP Integration

The backend uses the **Python MCP SDK** on the Python side to communicate with MCP servers.

### Backend endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /api/mcp/test` | Connect to an MCP server, return available tools |
| `POST /api/chat/mcp` | Agentic loop: LLM calls tools via MCP until a final answer is produced (max 5 turns) |

### MCP server URL format

Your MCP server can expose either an **SSE** endpoint or a **streamable HTTP** endpoint. Typical formats:

```
http://localhost:3000/sse
http://my-mcp-server:8080/sse
http://localhost:3000/mcp
http://my-mcp-server:8080/mcp
```

### Tool call flow

```
1. Backend connects to the MCP server via the Python MCP SDK
2. Lists available tools → converts to OpenAI function-calling format
3. Sends user message + tool list to LLM
4. LLM returns tool_calls → backend calls each tool via MCP
5. Tool results injected back into conversation
6. Loop repeats until LLM responds without tool calls (max 5 turns)
7. Final answer returned with tool_calls log
```

---

## Configuration

All settings are available in-app (no `.env` file needed).

### Access

- The Settings entry is now a discreet button on the landing page, in the top-left corner.
- Access is password-protected.
- Default password: `MM@2026`
- This password can be changed inside Settings after unlocking.

### Chat UX Notes

- Introductory helper messages such as the default welcome message and agent intro cards disappear as soon as the user starts typing.
- Agent intro cards now use a more stable high-contrast rendering to avoid the washed-out / shimmering effect seen previously on some machines.
- Technical sections in many agent / LLM answers are automatically folded into expandable details blocks when possible.
- When a new assistant answer arrives, the chat scrolls to the **top of that last answer** instead of the bottom of the full thread.
- The mode selector is opened from a compact **Tools** hammer button in the right-side floating utility dock and expands as a centered overlay.
- `Auto-ML` now opens a guided launch overlay to help users pick the right table, target, and business framing before the first run.
- `Data Cleaner`, `Anonymizer`, and custom Python agents use the same stable intro-card pattern as the other premium agent modes.

### LLM Settings

| Setting | Default | Description |
|---------|---------|-------------|
| Provider | `ollama` | `ollama` or OpenAI-compatible |
| Base URL | `http://localhost:11434` | Ollama or local API server |
| API Key | — | Required for OpenAI-compatible providers |
| Model | `llama3` | Model name (`ollama list` to see available) |
| System Prompt | — | Prepended to every conversation |
| Manager preloads functional context from RAG | `false` | When enabled, Agent Manager queries the knowledge base before routing or answering so it can reuse business definitions and field descriptions |
| Disable SSL verification for backend calls | `false` | Global override for self-signed or internal certificates used by LLM, embeddings, ClickHouse, MCP, and model discovery |

### RAG & OpenSearch Settings

| Setting | Default | Description |
|---------|---------|-------------|
| OpenSearch URL | `http://localhost:9200` | Cluster URL |
| Index | `rag_documents` | Target index name |
| Username / Password | — | Optional HTTP basic auth |
| Embedding Base URL | `http://localhost:11434/v1` | OpenAI-compatible embedding base URL or direct endpoint such as `https://host/v1/embeddings` |
| HTTP / HTTPS toggle | `http` | Quickly rewrites the embedding endpoint scheme without editing the rest of the URL |
| Embedding Model | `nomic-embed-text` | Embedding model name |
| Verify SSL certificate | `true` | Per-embedding TLS verification toggle, still overridden by the global SSL disable switch |
| Chunk Size | `512` | Max words per document chunk |
| Chunk Overlap | `50` | Sentence overlap between chunks |
| KNN Neighbors | `50` | Nearest neighbours to retrieve from OpenSearch during vector search |

Embedding behavior:
- You can configure either a base URL like `https://host/v1` or a direct endpoint like `https://host/v1/embeddings` or `https://host/v2:embeddings`.
- The Settings modal can test the embedding connection against the real endpoint and verify compatibility with the configured OpenSearch index.
- Model discovery stays available for direct `/embeddings` URLs by deriving the matching `/models` endpoint when the provider supports it.

### Oracle SQL Settings

| Setting | Default | Description |
|---------|---------|-------------|
| Connection ID | `oracle_default` | Stable identifier for a saved Oracle connection |
| Label | `Default Oracle` | Display name shown in Settings |
| Host / Port | `localhost:1521` | Oracle network endpoint when not using a full DSN |
| Service Name / SID | empty | Oracle target instance identifier |
| Full DSN | empty | Optional DSN overriding host + port + service/SID |
| Username / Password | — | Oracle credentials |
| Row limit | `1000` | Safety cap applied to Oracle query results |
| Max retries | `3` | Compatibility retry setting for Oracle workflows |
| Max iterations | `8` | UI-configurable compatibility field, runtime stays capped for safety |
| Toolkit ID | empty | Optional toolkit selector for future extension |
| System Prompt | built-in | Customises the Oracle SQL behaviour |

### ClickHouse SQL Settings

| Setting | Default | Description |
|---------|---------|-------------|
| Host | `localhost` | ClickHouse HTTP host |
| Port | `8123` | ClickHouse HTTP port |
| Database | `default` | Target database used by the agent |
| Username | `default` | ClickHouse username |
| Password | — | ClickHouse password |
| Use HTTPS | `false` | Switches the ClickHouse connection to HTTPS |
| Verify SSL certificate | `true` | Controls TLS verification for HTTPS connections |
| HTTP Path | empty | Optional custom ClickHouse HTTP path |
| Default Query Limit | `200` | Safety cap applied to row-returning ClickHouse queries |

Use **Test ClickHouse** in the dedicated ClickHouse settings page to verify the connection and preview the first available tables.

The **Data Analyst** agent reuses the same ClickHouse connection and query limit settings. If OpenSearch and embeddings are configured, it can also add knowledge-base search as one of its analytical steps.

### App Portal Settings

Each app tile entry contains:

| Field | Description |
|-------|-------------|
| Application name | Visible title on the Agents & Tools portal tile |
| URL | External link opened in a new browser tab |
| Hover description | Text revealed only when the tile is hovered |

### Custom Agents Settings

Each custom Python agent entry contains:

| Field | Description |
|-------|-------------|
| Agent title | Name displayed in the sub-agent picker and chat intro |
| User-facing description | Intro text shown when the agent is selected |
| Python code | Implementation draft analyzed by the local LLM |
| Generated system prompt | Final behavior prompt derived from the uploaded code |
| Manager routing hint | Short hint used by Agent Manager to decide when to delegate |
| Badge color | Visual tone used in the sub-agent menu |
| Enabled | Controls whether the agent is visible in the agent picker |

### File Management Settings

| Setting | Default | Description |
|---------|---------|-------------|
| Base Path | empty | Optional sandbox root for all file operations |
| Max Iterations | `10` | ReAct loop cap, hard-limited in backend |
| System Prompt | built-in | Customises the file-management agent behaviour |

### DB Backup Settings

| Action | Description |
|--------|-------------|
| Export backup | Download the current `DB.json` content as a JSON file |
| Import backup | Replace the current backend state with a previous JSON backup |
| Resync now | Reload the frontend from the latest backend DB state |

### MCP Tools Settings

Each MCP tool entry has:

| Field | Description |
|-------|-------------|
| Label | Display name shown in the chat mode selector |
| URL (SSE or HTTP) | Full MCP endpoint URL for either SSE or streamable HTTP |

### Backend Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8000` | Backend listen port |
| `BACKEND_PORT` | `8000` | Used by Vite proxy in dev |

---

## Production Build

Build the frontend into `dist/`, then serve everything through the Python backend:

```bash
npm run build       # outputs to dist/
python server.py    # serves /api/* + static files on port 8000
```

Access the app at `http://localhost:8000`.

---

## Privacy

ODIN AI Portal is designed to be **100% local**:

- No data is sent to any cloud service unless you explicitly configure a remote OpenAI-compatible, OpenSearch or ClickHouse endpoint.
- No telemetry, no analytics, no tracking.
- Models can run fully on your own hardware via Ollama.
- Durable application state is stored locally in backend-managed `DB.json`.
- In shared deployments, conversations and UI preferences are isolated per browser/client while the application configuration remains shared.
- The browser keeps a fallback cache, but the backend DB is the primary state source.
- OpenSearch and ClickHouse can run on your own infrastructure.
- Settings access is protected locally by an application password, configurable from the UI once unlocked.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React 19, TypeScript, Vite, Tailwind CSS |
| Markdown | ReactMarkdown, remark-gfm, remark-breaks, rehype-raw, react-syntax-highlighter |
| Backend | Python 3.11, FastAPI, Uvicorn |
| Vector store | OpenSearch (`opensearch-py`) |
| SQL analytics | ClickHouse over HTTP (`httpx`) |
| MCP client | Python MCP SDK (`mcp`) |
| HTTP client | `httpx` (async) |
| Oracle driver | `oracledb` |
| LLM / Embeddings | Ollama or any OpenAI-compatible API |

---

## License

MIT
