# RAGnarok ⚡

A fully local, privacy-first AI chat interface that combines **pure LLM conversations**, **Retrieval-Augmented Generation (RAG)**, and **multi-agent orchestration** — all powered by [Ollama](https://ollama.com) with no cloud services or external API keys required.

---

## Features

### Three Interaction Modes
| Mode | Description |
|------|-------------|
| **Pure LLM** | Direct chat with any model running in Ollama or an OpenAI-compatible local server (e.g. LM Studio). |
| **RAG Knowledge** | Upload documents and query them with full semantic search. Answers are grounded in your files and include cited sources. |
| **Agents** | Multi-agent orchestration UI with Manager, Analyst, and Researcher roles. |

### RAG Pipeline (fully local)
1. **HyDE** — generates a hypothetical answer to expand query intent before embedding.
2. **Hybrid Search** — combines cosine vector similarity (70 %) with keyword matching (30 %) for best recall.
3. **LLM Reranking** — a fast local model scores the top 10 candidates and re-ranks them.
4. **Cited Generation** — the final answer is produced with `[1]`, `[2]` source citations and a confidence score.

### UI Highlights
- Glassmorphism design with smooth animations.
- Collapsible sidebar with persistent conversation history (stored in `localStorage`).
- File attachments (images, PDFs, text) sent alongside messages.
- Inline source inspector for RAG answers.
- Configurable system prompt, model, and all RAG parameters from the settings panel.
- Markdown rendering with tables, code blocks, and interactive task-list checkboxes.

---

## Architecture

```
┌─────────────────────────────────────────┐
│  Browser (React + Vite)                 │
│  ┌─────────┐  ┌──────┐  ┌───────────┐  │
│  │ Pure LLM│  │ RAG  │  │  Agents   │  │
│  └────┬────┘  └──┬───┘  └─────┬─────┘  │
│       │          │             │        │
└───────┼──────────┼─────────────┼────────┘
        │          │             │
        │    /api/chat/rag       │
        │          │             │
        │   ┌──────▼──────┐     │
        │   │ server.py   │     │
        │   │  FastAPI    │     │
        │   └──────┬──────┘     │
        │          │            │
   Direct call  Ollama API   Direct call
        │          │            │
   ┌────▼──────────▼────────────▼────┐
   │            Ollama               │
   │  llama3 / nomic-embed-text /…   │
   └─────────────────────────────────┘
```

- **Frontend** — React 19 + TypeScript + Tailwind CSS, bundled with Vite.
- **Backend** — Python FastAPI server (`server.py`) that handles the RAG pipeline.
- **LLM / Embeddings** — Ollama runs all models locally. No internet connection needed after pulling models.

---

## Requirements

### System
- **Python 3.11+**
- **Node.js 18+**
- **[Ollama](https://ollama.com)** installed and running

### Ollama models (pull before starting)
```bash
ollama pull llama3           # Chat / HyDE / reranking
ollama pull nomic-embed-text # Text embeddings for RAG
```

Any other Ollama model works — just update `LLM_MODEL` and `EMBEDDING_MODEL` in your `.env`.

---

## Getting Started

### 1. Clone the repository
```bash
git clone <repo-url>
cd RAG-Chat
```

### 2. Set up the Python backend
```bash
pip install -r requirements.txt
```

### 3. Configure environment (optional)
```bash
cp .env.example .env
# Edit .env to change models or ports if needed
```

### 4. Start the Python backend
```bash
python server.py
# → http://localhost:8000
```

### 5. Install frontend dependencies and start the dev server
```bash
npm install
npm run dev
# → http://localhost:5173
```

Open [http://localhost:5173](http://localhost:5173) in your browser.

---

## Production Build

Build the frontend static files, then serve everything through the Python server:

```bash
npm run build          # outputs to dist/
python server.py       # serves API + static files on port 8000
```

---

## Configuration

All settings are accessible in-app via the **⚙ Settings** panel.

### LLM Settings
| Setting | Default | Description |
|---------|---------|-------------|
| Provider | `ollama` | `ollama` or OpenAI-compatible |
| Base URL | `http://localhost:11434` | Ollama or local API server URL |
| Model | `llama3` | Model name as listed in `ollama list` |
| System Prompt | — | Custom instructions prepended to every conversation |

### RAG & Elasticsearch Settings
| Setting | Default | Description |
|---------|---------|-------------|
| Embedding Base URL | `http://localhost:11434/v1` | OpenAI-compatible embedding endpoint |
| Embedding Model | `nomic-embed-text` | Local embedding model name |
| Chunk Size | `512` | Max words per document chunk |
| Chunk Overlap | `50` | Sentence overlap between chunks |
| KNN Neighbors | `50` | Number of nearest neighbors to retrieve |

### Backend Environment Variables
| Variable | Default | Description |
|----------|---------|-------------|
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama server URL |
| `LLM_MODEL` | `llama3` | Model for chat and reranking |
| `EMBEDDING_MODEL` | `nomic-embed-text` | Model for embeddings |
| `PORT` | `8000` | Python backend listen port |

---

## Privacy

RAGnarok is designed to be **100 % local**:
- No data is sent to any cloud service.
- No telemetry or analytics.
- All models run on your own hardware via Ollama.
- Conversation history is stored only in your browser's `localStorage`.

---

## License

MIT
