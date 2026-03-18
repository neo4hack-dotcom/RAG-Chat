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
    """Get a vector embedding via an OpenAI-compatible /embeddings endpoint."""
    url = base_url.rstrip("/") + "/embeddings"
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

    answer = await llm_chat(
        messages_payload,
        req.llm_base_url, req.llm_model, req.llm_provider, req.llm_api_key,
    )

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
