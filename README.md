# ODIN AI Portal — RAGnarok ⚡

A fully local, privacy-first AI platform combining **pure LLM chat**, **Retrieval-Augmented Generation (RAG)** with OpenSearch, **multi-agent orchestration**, and **MCP (Model Context Protocol)** tool integration — all powered by Ollama or any OpenAI-compatible server.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Interaction Modes](#interaction-modes)
- [RAG Pipeline](#rag-pipeline)
- [MCP Integration](#mcp-integration)
- [Configuration](#configuration)
- [Production Build](#production-build)
- [Privacy](#privacy)

---

## Features

| Category | Details |
|----------|---------|
| **4 chat modes** | Pure LLM, RAG, Multi-Agent, MCP Tools |
| **OpenSearch backend** | kNN vector search (HNSW/cosinesimil), index setup & document ingest from the UI |
| **MCP tools** | Connect any MCP server via SSE, test connection, real agentic tool-call loop |
| **Markdown & HTML** | Syntax-highlighted code blocks, proper tables, raw HTML from LLMs, copy button |
| **Apple-inspired landing** | Animated cards, contact modal, page routing |
| **Dark mode** | Full dark/light toggle persisted in `localStorage` |
| **File attachments** | Images, PDFs, text files alongside messages |
| **Conversation history** | Multi-session sidebar, persisted in `localStorage` |
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
                           │  ┌────▼──────────▼────────┐
                           │  │     server.py (FastAPI) │
                           │  │                         │
                           │  │  /api/opensearch/test   │
                           │  │  /api/opensearch/setup  │
                           │  │  /api/documents/ingest  │
                           │  │  /api/chat/rag          │
                           │  │  /api/mcp/test          │
                           │  │  /api/chat/mcp          │
                           │  └────┬──────────┬─────────┘
                           │       │          │
                    ┌──────▼───────▼─┐   ┌────▼─────────────┐
                    │  Ollama / LLM  │   │  OpenSearch       │
                    │  llama3        │   │  kNN index        │
                    │  nomic-embed   │   │  (HNSW cosine)    │
                    └────────────────┘   └──────────────────┘
                                              MCP Server
                                         ┌────▼──────────────┐
                                         │  any SSE MCP tool │
                                         └───────────────────┘
```

- **Frontend** — React 19 + TypeScript + Tailwind CSS, bundled with Vite.
- **Backend** — Python FastAPI (`server.py`) — RAG pipeline, MCP client, OpenSearch management.
- **Vector store** — OpenSearch with `opensearch-py` and `mcp` Python packages.
- **LLM / Embeddings** — Ollama (local) or any OpenAI-compatible API.

---

## Requirements

### System

- **Python 3.11+**
- **Node.js 18+**
- **[Ollama](https://ollama.com)** (or any OpenAI-compatible server)
- **[OpenSearch](https://opensearch.org)** running locally or remotely (for RAG mode)

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

Multi-agent orchestration with three roles:

| Role | Behaviour |
|------|-----------|
| **Agent Manager** | Orchestrates sub-agents, synthesises final answer, shows thinking steps |
| **Data Analyst** | Focuses on structured analysis and data interpretation |
| **Researcher** | Deep research with broad context gathering |

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

- No data is sent to any cloud service.
- No telemetry, no analytics, no tracking.
- All models run on your own hardware via Ollama.
- Conversation history is stored only in your browser's `localStorage`.
- OpenSearch runs on your own infrastructure.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React 19, TypeScript, Vite, Tailwind CSS |
| Markdown | ReactMarkdown, remark-gfm, remark-breaks, rehype-raw, react-syntax-highlighter |
| Backend | Python 3.11, FastAPI, Uvicorn |
| Vector store | OpenSearch (`opensearch-py`) |
| MCP client | `mcp` Python SDK (SSE transport) |
| HTTP client | `httpx` (async) |
| LLM / Embeddings | Ollama or any OpenAI-compatible API |

---

## License

MIT
