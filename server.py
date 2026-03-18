"""
RAGnarok — Python FastAPI Backend
100% local: uses Ollama for LLM inference and embeddings.
No external API keys required.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
import httpx
import json
import re
import math
import uuid
import os
from typing import Optional, List, Any
from pathlib import Path

app = FastAPI(title="RAGnarok API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Configuration ────────────────────────────────────────────────────────────
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
LLM_MODEL       = os.getenv("LLM_MODEL",        "llama3")
EMBEDDING_MODEL  = os.getenv("EMBEDDING_MODEL",  "nomic-embed-text")

# ── In-memory RAG store ──────────────────────────────────────────────────────
documents: list[dict] = []   # DocumentMeta
chunks:    list[dict] = []   # DocumentChunk (with embedding vector)

# ── Utility functions ────────────────────────────────────────────────────────

def cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def keyword_score(query: str, text: str) -> float:
    terms = [t for t in re.split(r"\W+", query.lower()) if len(t) > 2]
    text_lower = text.lower()
    score = sum(1 for t in terms if t in text_lower)
    return score / (len(terms) or 1)


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
    return result


# ── Ollama helpers ───────────────────────────────────────────────────────────

async def ollama_chat(model: str, messages: list[dict], fmt: Optional[str] = None) -> dict:
    payload: dict[str, Any] = {"model": model, "messages": messages, "stream": False}
    if fmt:
        payload["format"] = fmt
    async with httpx.AsyncClient(timeout=120.0) as client:
        response = await client.post(f"{OLLAMA_BASE_URL}/api/chat", json=payload)
        response.raise_for_status()
        return response.json()


async def ollama_embed(model: str, text: str) -> list[float]:
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            f"{OLLAMA_BASE_URL}/api/embed",
            json={"model": model, "input": text},
        )
        response.raise_for_status()
        data = response.json()
        embeddings = data.get("embeddings", [])
        if not embeddings:
            raise HTTPException(status_code=500, detail="Ollama returned no embeddings")
        return embeddings[0]


# ── Request models ───────────────────────────────────────────────────────────

class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    message: str
    history: list[dict] = []


# ── RAG endpoint ─────────────────────────────────────────────────────────────

@app.post("/api/chat/rag")
async def chat_rag(req: ChatRequest):
    message = req.message
    history = req.history

    # 1. HyDE — generate a hypothetical answer to improve semantic search
    try:
        hyde_resp = await ollama_chat(
            LLM_MODEL,
            [
                {
                    "role": "user",
                    "content": (
                        "Write a concise, factual hypothetical answer to help with semantic search. "
                        "No filler words, just the key facts:\n\n" + message
                    ),
                }
            ],
        )
        expanded_query = hyde_resp.get("message", {}).get("content", message)
    except Exception:
        expanded_query = message

    # 2. Embed the expanded query
    query_vector = await ollama_embed(EMBEDDING_MODEL, expanded_query)

    # 3. Hybrid search (vector cosine + keyword BM25-like)
    scored = []
    for chunk in chunks:
        vec_score = cosine_similarity(query_vector, chunk["embedding"])
        kw_score  = keyword_score(message, chunk["text"])
        hybrid    = vec_score * 0.7 + kw_score * 0.3
        scored.append({**chunk, "score": hybrid, "vecScore": vec_score, "kwScore": kw_score})

    scored.sort(key=lambda c: c["score"], reverse=True)
    top_chunks = scored[:10]

    # 4. LLM reranking
    if top_chunks:
        try:
            rerank_prompt = (
                f"Score the relevance of each chunk to the query on a scale of 0–10.\n"
                f"Return a JSON array: [{{\"index\": 0, \"relevanceScore\": 8}}, ...]\n\n"
                f"Query: {message}\n\nChunks:\n"
                + "\n\n".join(
                    f"[Chunk {i}]\n{c['text']}" for i, c in enumerate(top_chunks)
                )
            )
            rerank_resp = await ollama_chat(
                LLM_MODEL,
                [{"role": "user", "content": rerank_prompt}],
                fmt="json",
            )
            rerank_text = rerank_resp.get("message", {}).get("content", "[]")
            json_match = re.search(r"\[.*\]", rerank_text, re.DOTALL)
            if json_match:
                scores = json.loads(json_match.group())
                for s in scores:
                    idx = s.get("index", -1)
                    if 0 <= idx < len(top_chunks):
                        llm_score = s.get("relevanceScore", 0) / 10.0
                        top_chunks[idx]["score"] = (
                            top_chunks[idx]["score"] * 0.3 + llm_score * 0.7
                        )
            top_chunks.sort(key=lambda c: c["score"], reverse=True)
            top_chunks = [c for c in top_chunks if c["score"] > 0.3][:5]
        except Exception as e:
            print(f"Reranking failed (using hybrid scores): {e}")
            top_chunks = [c for c in top_chunks if c["score"] > 0.2][:5]

    # 5. Generate answer with citations
    context_text = "\n\n".join(
        f"[Source {i + 1}: {c['docName']}]\n{c['text']}"
        for i, c in enumerate(top_chunks)
    )
    system_prompt = (
        "You are a helpful assistant. Use the retrieved context below to answer the user's question.\n"
        "Always cite your sources using [1], [2], etc. based on the Source number provided.\n"
        "If the answer is not in the context, say you don't know based on the provided documents.\n\n"
        f"Context:\n{context_text}"
    )

    messages_payload = [{"role": "system", "content": system_prompt}]
    for m in history:
        role = "user" if m.get("role") == "user" else "assistant"
        messages_payload.append({"role": role, "content": m.get("content", "")})
    messages_payload.append({"role": "user", "content": message})

    gen_resp = await ollama_chat(LLM_MODEL, messages_payload)
    answer = gen_resp.get("message", {}).get("content", "")

    return {
        "answer": answer,
        "sources": [
            {
                "id":      c["id"],
                "docName": c["docName"],
                "text":    c["text"],
                "score":   c["score"],
            }
            for c in top_chunks
        ],
        "confidence": top_chunks[0]["score"] if top_chunks else 0,
    }


# ── Static file serving (production build) ───────────────────────────────────

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
    print(f"RAGnarok backend running on http://localhost:{port}")
    print(f"  Ollama URL   : {OLLAMA_BASE_URL}")
    print(f"  LLM model    : {LLM_MODEL}")
    print(f"  Embed model  : {EMBEDDING_MODEL}")
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=True)
