"""
RAGnarok — FastAPI Backend (OpenSearch edition)
Embeddings via Ollama/OpenAI-compatible endpoint.
Vector storage and kNN search via opensearch-py.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
import httpx
import json
import re
import uuid
import os
import asyncio
from typing import Optional
from pathlib import Path
from opensearchpy import OpenSearch
from urllib.parse import urlparse
from mcp import ClientSession
from mcp.client.sse import sse_client

app = FastAPI(title="RAGnarok API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── OpenSearch client factory ─────────────────────────────────────────────────

def get_os_client(url: str, username: str = None, password: str = None) -> OpenSearch:
    parsed = urlparse(url)
    use_ssl = parsed.scheme == "https"
    host = parsed.hostname or "localhost"
    port = parsed.port or (443 if use_ssl else 9200)
    auth = (username, password) if username and password else None
    return OpenSearch(
        hosts=[{"host": host, "port": port}],
        http_auth=auth,
        use_ssl=use_ssl,
        verify_certs=False,
        ssl_show_warn=False,
    )


# ── Text utilities ────────────────────────────────────────────────────────────

def keyword_score(query: str, text: str) -> float:
    terms = [t for t in re.split(r"\W+", query.lower()) if len(t) > 2]
    if not terms:
        return 0.0
    text_lower = text.lower()
    return sum(1 for t in terms if t in text_lower) / len(terms)


def chunk_text(text: str, max_words: int = 200, overlap_sentences: int = 2) -> list[str]:
    sentences = re.findall(r"[^.!?]+[.!?]+", text) or [text]
    result: list[str] = []
    current: list[str] = []
    current_words = 0
    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue
        word_count = len(sentence.split())
        if current_words + word_count > max_words and current:
            result.append(" ".join(current))
            overlap = current[-overlap_sentences:]
            current = overlap + [sentence]
            current_words = sum(len(s.split()) for s in current)
        else:
            current.append(sentence)
            current_words += word_count
    if current:
        result.append(" ".join(current))
    return result or [text]


# ── Embedding helper ──────────────────────────────────────────────────────────

async def get_embedding(
    text: str,
    base_url: str,
    model: str,
    api_key: str = None,
) -> list[float]:
    """Get a vector embedding via an OpenAI-compatible /embeddings endpoint.

    base_url may be a base path (e.g. ``http://host/v1``) — ``/embeddings`` is
    appended automatically — or the full endpoint URL already ending with
    ``/embeddings`` (e.g. ``http://host/v1/openai/embeddings``), used as-is.
    """
    stripped = base_url.rstrip("/")
    url = stripped if stripped.endswith("/embeddings") else stripped + "/embeddings"
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(url, json={"model": model, "input": text}, headers=headers)
        resp.raise_for_status()
        return resp.json()["data"][0]["embedding"]


# ── LLM helper ────────────────────────────────────────────────────────────────

async def llm_chat(
    messages: list[dict],
    base_url: str,
    model: str,
    provider: str = "ollama",
    api_key: str = None,
    response_format: str = None,
) -> str:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    if provider == "ollama":
        payload: dict = {"model": model, "messages": messages, "stream": False}
        if response_format == "json":
            payload["format"] = "json"
        endpoint = base_url.rstrip("/") + "/api/chat"
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(endpoint, json=payload, headers=headers)
            resp.raise_for_status()
            return resp.json().get("message", {}).get("content", "")
    else:
        payload = {"model": model, "messages": messages}
        if response_format == "json":
            payload["response_format"] = {"type": "json_object"}
        endpoint = base_url.rstrip("/") + "/chat/completions"
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(endpoint, json=payload, headers=headers)
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]


# ── Pydantic models ───────────────────────────────────────────────────────────

class MCPTestRequest(BaseModel):
    url: str


class MCPChatRequest(BaseModel):
    message: str
    history: list[dict] = []
    mcp_url: str
    llm_base_url: str = "http://localhost:11434"
    llm_model: str = "llama3"
    llm_api_key: Optional[str] = None
    llm_provider: str = "ollama"
    system_prompt: str = ""


class OSConfig(BaseModel):
    url: str
    index: str = "rag_documents"
    username: Optional[str] = None
    password: Optional[str] = None


class TestConnectionRequest(BaseModel):
    url: str
    username: Optional[str] = None
    password: Optional[str] = None


class SetupIndexRequest(BaseModel):
    opensearch: OSConfig
    embedding_dimension: int = 768


class IngestRequest(BaseModel):
    text: str
    doc_name: str
    opensearch: OSConfig
    embedding_base_url: str = "http://localhost:11434/v1"
    embedding_api_key: Optional[str] = None
    embedding_model: str = "nomic-embed-text"
    chunk_size: int = 200
    chunk_overlap: int = 2
    embedding_dimension: int = 768


class ChatRequest(BaseModel):
    message: str
    history: list[dict] = []
    opensearch: OSConfig
    embedding_base_url: str = "http://localhost:11434/v1"
    embedding_api_key: Optional[str] = None
    embedding_model: str = "nomic-embed-text"
    knn_neighbors: int = 10
    llm_base_url: str = "http://localhost:11434"
    llm_model: str = "llama3"
    llm_api_key: Optional[str] = None
    llm_provider: str = "ollama"


class EmbeddingTestRequest(BaseModel):
    embedding_base_url: str
    embedding_model: str
    embedding_api_key: Optional[str] = None
    opensearch: Optional[OSConfig] = None


# ── MCP endpoints ─────────────────────────────────────────────────────────────

@app.post("/api/mcp/test")
async def test_mcp_connection(req: MCPTestRequest):
    """Connect to an MCP server via SSE and return its available tools."""
    try:
        async with sse_client(req.url) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools_result = await session.list_tools()
                tools = [
                    {"name": t.name, "description": t.description or ""}
                    for t in tools_result.tools
                ]
        return {"status": "ok", "tools": tools, "tool_count": len(tools)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def _mcp_tool_to_openai(tool) -> dict:
    """Convert an MCP tool definition to OpenAI function-calling format."""
    schema = {}
    if hasattr(tool, "inputSchema") and tool.inputSchema:
        schema = tool.inputSchema if isinstance(tool.inputSchema, dict) else {}
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description or "",
            "parameters": schema or {"type": "object", "properties": {}},
        },
    }


@app.post("/api/chat/mcp")
async def chat_mcp(req: MCPChatRequest):
    """Agentic loop: connects to MCP server, lets LLM call tools, returns final answer."""
    try:
        async with sse_client(req.mcp_url) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools_result = await session.list_tools()
                openai_tools = [_mcp_tool_to_openai(t) for t in tools_result.tools]

                # Build initial messages
                messages: list[dict] = []
                if req.system_prompt:
                    messages.append({"role": "system", "content": req.system_prompt})
                for m in req.history:
                    role = "user" if m.get("role") == "user" else "assistant"
                    messages.append({"role": role, "content": m.get("content", "")})
                messages.append({"role": "user", "content": req.message})

                tool_calls_log: list[dict] = []
                MAX_TURNS = 5

                for _ in range(MAX_TURNS):
                    # Call LLM with tools
                    headers = {"Content-Type": "application/json"}
                    if req.llm_api_key:
                        headers["Authorization"] = f"Bearer {req.llm_api_key}"

                    if req.llm_provider == "ollama":
                        endpoint = req.llm_base_url.rstrip("/") + "/api/chat"
                        payload: dict = {
                            "model": req.llm_model,
                            "messages": messages,
                            "stream": False,
                            "tools": openai_tools,
                        }
                        async with httpx.AsyncClient(timeout=120.0) as client:
                            resp = await client.post(endpoint, json=payload, headers=headers)
                            resp.raise_for_status()
                        data = resp.json()
                        llm_msg = data.get("message", {})
                        content = llm_msg.get("content", "")
                        raw_tool_calls = llm_msg.get("tool_calls", [])
                    else:
                        endpoint = req.llm_base_url.rstrip("/") + "/chat/completions"
                        payload = {
                            "model": req.llm_model,
                            "messages": messages,
                            "tools": openai_tools,
                        }
                        async with httpx.AsyncClient(timeout=120.0) as client:
                            resp = await client.post(endpoint, json=payload, headers=headers)
                            resp.raise_for_status()
                        data = resp.json()
                        choice = data["choices"][0]["message"]
                        content = choice.get("content") or ""
                        raw_tool_calls = choice.get("tool_calls", [])

                    # No tool calls → final answer
                    if not raw_tool_calls:
                        return {
                            "answer": content,
                            "tool_calls": tool_calls_log,
                        }

                    # Append assistant message with tool calls
                    messages.append({
                        "role": "assistant",
                        "content": content,
                        "tool_calls": raw_tool_calls,
                    })

                    # Execute each tool call via MCP
                    for tc in raw_tool_calls:
                        # Normalise across Ollama / OpenAI formats
                        if isinstance(tc, dict) and "function" in tc:
                            fn = tc["function"]
                            tool_name = fn.get("name", "")
                            raw_args = fn.get("arguments", "{}")
                            tool_id = tc.get("id", tool_name)
                        else:
                            # Ollama sometimes returns {"name":..., "arguments":...} directly
                            tool_name = tc.get("name", "")
                            raw_args = tc.get("arguments", "{}")
                            tool_id = tool_name

                        tool_args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args

                        try:
                            result = await session.call_tool(tool_name, tool_args)
                            tool_output = "\n".join(
                                c.text for c in result.content if hasattr(c, "text")
                            ) or str(result.content)
                        except Exception as e:
                            tool_output = f"[Tool error] {e}"

                        tool_calls_log.append({
                            "tool": tool_name,
                            "args": tool_args,
                            "result": tool_output,
                        })

                        messages.append({
                            "role": "tool",
                            "tool_call_id": tool_id,
                            "name": tool_name,
                            "content": tool_output,
                        })

                # Safety net: ask LLM for a final answer without tools
                messages.append({
                    "role": "user",
                    "content": "Please summarise the results above as a final answer.",
                })
                final = await llm_chat(
                    messages, req.llm_base_url, req.llm_model, req.llm_provider, req.llm_api_key
                )
                return {"answer": final, "tool_calls": tool_calls_log}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ── OpenSearch management endpoints ──────────────────────────────────────────

@app.post("/api/opensearch/test")
async def test_opensearch(req: TestConnectionRequest):
    """Test connectivity to an OpenSearch cluster."""
    def _ping():
        client = get_os_client(req.url, req.username, req.password)
        return client.info()

    try:
        info = await asyncio.to_thread(_ping)
        cluster_name = info.get("cluster_name", "OpenSearch")
        version = info.get("version", {}).get("number", "unknown")
        return {"status": "ok", "cluster_name": cluster_name, "version": version}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/opensearch/setup-index")
async def setup_index(req: SetupIndexRequest):
    """Create a kNN-enabled index if it does not already exist."""
    def _setup():
        client = get_os_client(
            req.opensearch.url, req.opensearch.username, req.opensearch.password
        )
        index = req.opensearch.index
        if client.indices.exists(index=index):
            return {"status": "exists", "index": index}

        mapping = {
            "settings": {"index.knn": True},
            "mappings": {
                "properties": {
                    "chunk_id":  {"type": "keyword"},
                    "doc_id":    {"type": "keyword"},
                    "doc_name":  {"type": "keyword"},
                    "text":      {"type": "text", "analyzer": "standard"},
                    "embedding": {
                        "type":      "knn_vector",
                        "dimension": req.embedding_dimension,
                        "method": {
                            "name":       "hnsw",
                            "space_type": "cosinesimil",
                            "engine":     "nmslib",
                            "parameters": {"ef_construction": 128, "m": 24},
                        },
                    },
                }
            },
        }
        client.indices.create(index=index, body=mapping)
        return {"status": "created", "index": index}

    try:
        result = await asyncio.to_thread(_setup)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ── Embedding test endpoint ───────────────────────────────────────────────────

@app.post("/api/embedding/test")
async def test_embedding(req: EmbeddingTestRequest):
    """Generate a real test embedding and optionally verify dimension compatibility with the OpenSearch index."""
    try:
        vector = await get_embedding(
            "embedding connectivity test", req.embedding_base_url, req.embedding_model, req.embedding_api_key
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Embedding error: {e}")

    dimension = len(vector)
    result: dict = {"status": "ok", "dimension": dimension, "model": req.embedding_model}

    if req.opensearch:
        def _check_mapping():
            client = get_os_client(req.opensearch.url, req.opensearch.username, req.opensearch.password)
            index = req.opensearch.index
            if not client.indices.exists(index=index):
                return None
            mapping = client.indices.get_mapping(index=index)
            props = mapping.get(index, {}).get("mappings", {}).get("properties", {})
            return props.get("embedding", {}).get("dimension")

        try:
            index_dim = await asyncio.to_thread(_check_mapping)
            if index_dim is None:
                result["opensearch"] = {
                    "status": "no_index",
                    "message": f"Index '{req.opensearch.index}' does not exist yet — use Setup Index.",
                }
            elif index_dim == dimension:
                result["opensearch"] = {"status": "compatible", "index_dimension": index_dim}
            else:
                result["opensearch"] = {
                    "status": "incompatible",
                    "index_dimension": index_dim,
                    "message": (
                        f"Dimension mismatch: model produces {dimension}‑D vectors "
                        f"but index expects {index_dim}‑D. Delete and re-create the index."
                    ),
                }
        except Exception as e:
            result["opensearch"] = {"status": "error", "message": str(e)}

    return result


# ── Document ingest endpoint ──────────────────────────────────────────────────

@app.post("/api/documents/ingest")
async def ingest_document(req: IngestRequest):
    """Chunk a document, embed each chunk, and index into OpenSearch."""
    chunks = chunk_text(req.text, max_words=req.chunk_size, overlap_sentences=req.chunk_overlap)
    doc_id = str(uuid.uuid4())

    try:
        embeddings = []
        for chunk in chunks:
            vec = await get_embedding(
                chunk, req.embedding_base_url, req.embedding_model, req.embedding_api_key
            )
            embeddings.append(vec)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Embedding error: {e}")

    def _index():
        client = get_os_client(
            req.opensearch.url, req.opensearch.username, req.opensearch.password
        )
        index = req.opensearch.index

        if not client.indices.exists(index=index):
            client.indices.create(index=index, body={
                "settings": {"index.knn": True},
                "mappings": {
                    "properties": {
                        "chunk_id":  {"type": "keyword"},
                        "doc_id":    {"type": "keyword"},
                        "doc_name":  {"type": "keyword"},
                        "text":      {"type": "text", "analyzer": "standard"},
                        "embedding": {
                            "type":      "knn_vector",
                            "dimension": req.embedding_dimension,
                        },
                    }
                },
            })

        indexed = 0
        for i, (chunk, vec) in enumerate(zip(chunks, embeddings)):
            client.index(index=index, body={
                "chunk_id":  f"{doc_id}_{i}",
                "doc_id":    doc_id,
                "doc_name":  req.doc_name,
                "text":      chunk,
                "embedding": vec,
            })
            indexed += 1
        return indexed

    try:
        count = await asyncio.to_thread(_index)
        return {"status": "ok", "doc_id": doc_id, "chunks_indexed": count}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ── RAG chat endpoint ─────────────────────────────────────────────────────────

@app.post("/api/chat/rag")
async def chat_rag(req: ChatRequest):
    message = req.message
    history = req.history

    # 1. HyDE — generate a hypothetical answer to improve semantic recall
    try:
        hyde_answer = await llm_chat(
            [{"role": "user", "content": (
                "Write a concise factual answer for semantic search. "
                "No filler, just key facts:\n\n" + message
            )}],
            req.llm_base_url, req.llm_model, req.llm_provider, req.llm_api_key,
        )
        search_text = hyde_answer or message
    except Exception:
        search_text = message

    # 2. Embed the query
    try:
        query_vector = await get_embedding(
            search_text, req.embedding_base_url, req.embedding_model, req.embedding_api_key
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Embedding error: {e}")

    # 3. kNN search in OpenSearch
    def _search():
        client = get_os_client(
            req.opensearch.url, req.opensearch.username, req.opensearch.password
        )
        index = req.opensearch.index
        if not client.indices.exists(index=index):
            return []

        response = client.search(
            index=index,
            body={
                "size": req.knn_neighbors,
                "query": {
                    "knn": {
                        "embedding": {
                            "vector": query_vector,
                            "k": req.knn_neighbors,
                        }
                    }
                },
                "_source": ["chunk_id", "doc_id", "doc_name", "text"],
            },
        )
        return [
            {
                "id":       h["_id"],
                "chunk_id": h["_source"].get("chunk_id", h["_id"]),
                "doc_id":   h["_source"].get("doc_id", ""),
                "doc_name": h["_source"].get("doc_name", "document"),
                "text":     h["_source"].get("text", ""),
                "score":    h.get("_score", 0.0),
            }
            for h in response.get("hits", {}).get("hits", [])
        ]

    try:
        results = await asyncio.to_thread(_search)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"OpenSearch error: {e}")

    # Fallback — index empty or unreachable
    if not results:
        try:
            answer = await llm_chat(
                [
                    {"role": "system", "content": (
                        "You are a helpful assistant. No documents were found in the knowledge base. "
                        "Answer from general knowledge and mention that no documents are indexed."
                    )},
                    {"role": "user", "content": message},
                ],
                req.llm_base_url, req.llm_model, req.llm_provider, req.llm_api_key,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"LLM error: {e}")
        return {"answer": answer, "sources": [], "confidence": 0.0}

    # 4. Keyword boost (Python-side)
    for r in results:
        kw = keyword_score(message, r["text"])
        r["score"] = r["score"] * 0.7 + kw * 0.3
    results.sort(key=lambda x: x["score"], reverse=True)
    top = results[:10]

    # 5. LLM reranking
    try:
        rerank_prompt = (
            f"Score each chunk's relevance to the query 0–10.\n"
            f"Return JSON array only: [{{\"index\": 0, \"relevanceScore\": 8}}, ...]\n\n"
            f"Query: {message}\n\nChunks:\n"
            + "\n\n".join(f"[Chunk {i}]\n{c['text']}" for i, c in enumerate(top))
        )
        rerank_text = await llm_chat(
            [{"role": "user", "content": rerank_prompt}],
            req.llm_base_url, req.llm_model, req.llm_provider, req.llm_api_key,
            response_format="json",
        )
        json_match = re.search(r"\[.*\]", rerank_text, re.DOTALL)
        if json_match:
            for s in json.loads(json_match.group()):
                idx = s.get("index", -1)
                if 0 <= idx < len(top):
                    llm_score = s.get("relevanceScore", 0) / 10.0
                    top[idx]["score"] = top[idx]["score"] * 0.3 + llm_score * 0.7
        top.sort(key=lambda x: x["score"], reverse=True)
        top = [c for c in top if c["score"] > 0.3][:5]
    except Exception as e:
        print(f"Reranking skipped: {e}")
        top = [c for c in top if c["score"] > 0.2][:5]

    # 6. Generate answer with citations
    context = "\n\n".join(
        f"[Source {i + 1}: {c['doc_name']}]\n{c['text']}"
        for i, c in enumerate(top)
    )
    messages_payload = [
        {"role": "system", "content": (
            "You are a helpful assistant. Use the retrieved context to answer.\n"
            "Cite sources using [1], [2], etc. If the answer is not in the context, say so.\n\n"
            f"Context:\n{context}"
        )}
    ]
    for m in history:
        role = "user" if m.get("role") == "user" else "assistant"
        messages_payload.append({"role": role, "content": m.get("content", "")})
    messages_payload.append({"role": "user", "content": message})

    try:
        answer = await llm_chat(
            messages_payload,
            req.llm_base_url, req.llm_model, req.llm_provider, req.llm_api_key,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LLM error: {e}")

    return {
        "answer": answer,
        "sources": [
            {
                "id":      c["chunk_id"],
                "docName": c["doc_name"],
                "text":    c["text"],
                "score":   c["score"],
            }
            for c in top
        ],
        "confidence": top[0]["score"] if top else 0.0,
    }


# ── Static file serving ───────────────────────────────────────────────────────

dist_path = Path(__file__).parent / "dist"
if dist_path.exists():
    app.mount("/assets", StaticFiles(directory=str(dist_path / "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        file = dist_path / full_path
        if file.is_file():
            return FileResponse(str(file))
        return FileResponse(str(dist_path / "index.html"))


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    print(f"RAGnarok backend · http://localhost:{port}")
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=True)
