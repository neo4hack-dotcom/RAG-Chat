# ODIN AI Portal — RAGnarok ⚡

A privacy-first AI workspace combining **pure LLM chat**, **Retrieval-Augmented Generation (RAG)** with OpenSearch, **guided agents** including a **ClickHouse SQL agent**, and **MCP (Model Context Protocol)** tool integration — powered by Ollama or any OpenAI-compatible server, with durable app state persisted by the Python backend.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Interaction Modes](#interaction-modes)
- [State Persistence & Backup](#state-persistence--backup)
- [RAG Pipeline](#rag-pipeline)
- [ClickHouse Query Agent](#clickhouse-query-agent)
- [MCP Integration](#mcp-integration)
- [Configuration](#configuration)
- [Production Build](#production-build)
- [Privacy](#privacy)

---

## Features

| Category | Details |
|----------|---------|
| **4 chat modes** | Pure LLM, RAG, Multi-Agent, MCP Tools |
| **4 agent roles** | Agent Manager, Data Analyst, Researcher, ClickHouse Query |
| **OpenSearch backend** | kNN vector search (HNSW/cosinesimil), index setup & document ingest from the UI |
| **ClickHouse agent** | Guided table/schema discovery, ambiguity clarification, safe read-only SQL generation, English final answer |
| **MCP tools** | Connect any MCP server via SSE, test connection, real agentic tool-call loop |
| **Backend persistence** | App config, conversations and durable preferences stored in backend-managed `DB.json` |
| **Backup workflow** | Export/import DB backups and force a resync from the latest backend state in Settings |
| **Markdown & HTML** | Syntax-highlighted code blocks, proper tables, raw HTML from LLMs, copy button |
| **Apple-inspired landing** | Animated cards, contact modal, page routing |
| **Dark mode** | Full dark/light toggle persisted through backend state sync |
| **File attachments** | Images, PDFs, text files alongside messages |
| **Conversation history** | Multi-session sidebar, persisted in backend state with browser fallback cache |
| **Settings panel** | All parameters configurable in-app, no `.env` required |

---

## Architecture

```
┌────────────────────────────────────────────────────────┐
│  Browser  (React 19 + Vite + Tailwind CSS)             │
│                                                        │
│  Landing Page ──► RAGnarok Chat                        │
│                   ┌──────────────────────────────────┐ │
│                   │  Pure LLM │ RAG │ Agents │ MCP   │ │
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
                                               │  any SSE MCP tool │
                                               └───────────────────┘
```

- **Frontend** — React 19 + TypeScript + Tailwind CSS, bundled with Vite.
- **Backend** — Python FastAPI (`server.py`) — state persistence, RAG pipeline, ClickHouse agent, MCP client, OpenSearch management.
- **Vector store** — OpenSearch with `opensearch-py` and `mcp` Python packages.
- **Analytics store** — ClickHouse over HTTP for the SQL agent.
- **LLM / Embeddings** — Ollama (local) or any OpenAI-compatible API.

---

## Requirements

### System

- **Python 3.11+**
- **Node.js 18+**
- **[Ollama](https://ollama.com)** (or any OpenAI-compatible server)
- **[OpenSearch](https://opensearch.org)** running locally or remotely (for RAG mode)
- **[ClickHouse](https://clickhouse.com)** running locally or remotely (optional, for the ClickHouse Query agent)

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

On first run, the backend also creates a local `DB.json` file at the project root to persist app state, chat history and configuration.

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

Multi-agent orchestration with four roles:

| Role | Behaviour |
|------|-----------|
| **Agent Manager** | Orchestrates sub-agents, synthesises final answer, shows thinking steps |
| **Data Analyst** | Focuses on structured analysis and data interpretation |
| **Researcher** | Deep research with broad context gathering |
| **ClickHouse Query** | Guides the user through table selection, schema inspection, field/date clarification, then runs safe read-only SQL and answers in English |

#### ClickHouse Query agent quick flow

1. Configure ClickHouse in **Settings → RAG & OpenSearch → ClickHouse Query Agent**.
2. Switch to **Agents** mode and select **ClickHouse Query**.
3. The agent first proposes the list of available tables.
4. Once the table is selected, it inspects the schema.
5. If multiple fields or date columns are plausible, it asks the user to choose with task-list style options.
6. It generates a read-only ClickHouse query, validates it, executes it, and returns:
   - a short final answer in English,
   - the executed SQL,
   - a concise reasoning summary.

The ClickHouse agent uses the backend only and always relies on the configured local/application LLM endpoint for planning and summarisation.

---

## State Persistence & Backup

RAGnarok now uses a backend-managed `DB.json` file as its durable source of truth.

What is stored there:

- application configuration,
- conversation history,
- durable UI preferences such as dark mode, selected workflow and current conversation,
- agent state needed for guided workflows like the ClickHouse Query agent.

### Sync behaviour

- On startup, the frontend fetches the latest state from the backend.
- If the backend DB is empty but the browser still has legacy data, the app migrates that state into `DB.json`.
- The app re-syncs when the window regains focus or becomes visible again.
- The browser still keeps a lightweight fallback cache, but the backend DB is the source of truth.

### Backup tools

In **Settings → DB Backup**, you can:

- export the current backend DB as JSON,
- import a previous JSON backup,
- force a manual resync from the backend.

### MCP Tools

Connect any [Model Context Protocol](https://modelcontextprotocol.io) server and have the LLM call its tools autonomously:

1. Open **Settings → MCP Tools**.
2. Add a tool: give it a label and its SSE URL (e.g. `http://localhost:3000/sse`).
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

---

## ClickHouse Query Agent

The ClickHouse Query agent is designed to follow a conservative analytics workflow instead of guessing columns blindly.

### Workflow

```text
User enters request
    ↓
Agent lists available ClickHouse tables
    ↓
User selects one table
    ↓
Backend inspects system.columns for that table
    ↓
Local LLM maps request to likely business fields
    ↓
If ambiguous: user chooses field and/or date column
    ↓
Local LLM generates one safe read-only SQL query
    ↓
Backend validates and executes the query in ClickHouse
    ↓
Local LLM writes final answer in English
```

### Safety / best-practice behaviour

- Only read-only `SELECT` / `WITH ... SELECT` style queries are accepted.
- Multi-statement SQL is rejected.
- Destructive keywords such as `DROP`, `ALTER`, `DELETE`, `INSERT`, `TRUNCATE`, etc. are blocked.
- The backend enforces a result limit for row-oriented queries.
- The agent tries to repair SQL once if ClickHouse returns an execution error.
- The response stays short and includes both the SQL and a reasoning summary for auditability.

### Backend endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /api/clickhouse/test` | Test ClickHouse connectivity and preview available tables |
| `POST /api/chat/clickhouse-agent` | Guided ClickHouse agent workflow: discover tables, inspect schema, clarify fields/dates, generate and execute SQL |

---

## MCP Integration

The backend uses the **`mcp` Python SDK** to communicate with MCP servers over Server-Sent Events (SSE).

### Backend endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /api/mcp/test` | Connect to an MCP server, return available tools |
| `POST /api/chat/mcp` | Agentic loop: LLM calls tools via MCP until a final answer is produced (max 5 turns) |

### MCP server URL format

Your MCP server must expose an SSE endpoint. Typical formats:

```
http://localhost:3000/sse
http://my-mcp-server:8080/sse
```

### Tool call flow

```
1. Backend connects to MCP server via SSE
2. Lists available tools → converts to OpenAI function-calling format
3. Sends user message + tool list to LLM
4. LLM returns tool_calls → backend calls each tool via MCP
5. Tool results injected back into conversation
6. Loop repeats until LLM responds without tool calls (max 5 turns)
7. Final answer returned with tool_calls log
```

---

## Configuration

All settings are available in-app via the **⚙ Settings** panel (no `.env` file needed).

### LLM Settings

| Setting | Default | Description |
|---------|---------|-------------|
| Provider | `ollama` | `ollama` or OpenAI-compatible |
| Base URL | `http://localhost:11434` | Ollama or local API server |
| API Key | — | Required for OpenAI-compatible providers |
| Model | `llama3` | Model name (`ollama list` to see available) |
| System Prompt | — | Prepended to every conversation |

### RAG & OpenSearch Settings

| Setting | Default | Description |
|---------|---------|-------------|
| OpenSearch URL | `http://localhost:9200` | Cluster URL |
| Index | `rag_documents` | Target index name |
| Username / Password | — | Optional HTTP basic auth |
| Embedding Base URL | `http://localhost:11434/v1` | OpenAI-compatible embedding endpoint |
| Embedding Model | `nomic-embed-text` | Embedding model name |
| Chunk Size | `512` | Max words per document chunk |
| Chunk Overlap | `50` | Sentence overlap between chunks |
| KNN Neighbors | `50` | Nearest neighbours to retrieve |

### ClickHouse Query Agent Settings

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

Use **Test ClickHouse** in the settings panel to verify the connection and preview the first available tables.

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
| URL (SSE) | Full SSE endpoint of the MCP server |

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
- The browser keeps a fallback cache, but the backend DB is the primary state source.
- OpenSearch and ClickHouse can run on your own infrastructure.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React 19, TypeScript, Vite, Tailwind CSS |
| Markdown | ReactMarkdown, remark-gfm, remark-breaks, rehype-raw, react-syntax-highlighter |
| Backend | Python 3.11, FastAPI, Uvicorn |
| Vector store | OpenSearch (`opensearch-py`) |
| SQL analytics | ClickHouse over HTTP (`httpx`) |
| MCP client | `mcp` Python SDK (SSE transport) |
| HTTP client | `httpx` (async) |
| LLM / Embeddings | Ollama or any OpenAI-compatible API |

---

## License

MIT
