"""
RAGnarok — FastAPI Backend (OpenSearch edition)
Embeddings via Ollama/OpenAI-compatible endpoint.
Vector storage and kNN search via opensearch-py.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
import httpx
import json
import re
import uuid
import os
import asyncio
from datetime import datetime, timezone
from typing import Any, Optional
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


# ── Persistent app database ───────────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "DB.json"
DB_LOCK = asyncio.Lock()

DEFAULT_APP_CONFIG = {
    "provider": "ollama",
    "baseUrl": "http://localhost:11434",
    "apiKey": "",
    "model": "llama3",
    "systemPrompt": (
        "You are a helpful, smart, and concise AI assistant. Format your responses "
        "beautifully using markdown. When offering choices, use markdown task lists "
        "(- [ ] Option)."
    ),
    "elasticsearchUrl": "http://localhost:9200",
    "elasticsearchIndex": "rag_documents",
    "elasticsearchUsername": "",
    "elasticsearchPassword": "",
    "embeddingBaseUrl": "http://localhost:11434/v1",
    "embeddingApiKey": "",
    "embeddingModel": "nomic-embed-text",
    "embeddingVerifySsl": True,
    "chunkSize": 512,
    "chunkOverlap": 50,
    "knnNeighbors": 50,
    "mcpTools": [
        {"id": "mcp_1", "label": "MCP Tool 1", "url": ""},
        {"id": "mcp_2", "label": "MCP Tool 2", "url": ""},
    ],
    "documentationUrl": "",
    "clickhouseHost": "localhost",
    "clickhousePort": 8123,
    "clickhouseDatabase": "default",
    "clickhouseUsername": "default",
    "clickhousePassword": "",
    "clickhouseSecure": False,
    "clickhouseVerifySsl": True,
    "clickhouseHttpPath": "",
    "clickhouseQueryLimit": 200,
}

DEFAULT_PREFERENCES = {
    "darkMode": False,
    "currentConversationId": None,
    "workflow": "LLM",
    "agentRole": "manager",
    "selectedMcpToolId": "",
    "page": "landing",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_db_state() -> dict:
    return {
        "schemaVersion": 1,
        "updatedAt": _utc_now_iso(),
        "config": json.loads(json.dumps(DEFAULT_APP_CONFIG)),
        "conversations": [],
        "preferences": json.loads(json.dumps(DEFAULT_PREFERENCES)),
    }


def _normalize_db_state(payload: Optional[dict]) -> dict:
    state = _default_db_state()
    if not isinstance(payload, dict):
        return state

    incoming_config = payload.get("config")
    if isinstance(incoming_config, dict):
        state["config"].update(incoming_config)

    incoming_conversations = payload.get("conversations")
    if isinstance(incoming_conversations, list):
        state["conversations"] = incoming_conversations

    incoming_preferences = payload.get("preferences")
    if isinstance(incoming_preferences, dict):
        state["preferences"].update(incoming_preferences)

    incoming_updated_at = payload.get("updatedAt")
    if isinstance(incoming_updated_at, str) and incoming_updated_at:
        state["updatedAt"] = incoming_updated_at

    incoming_schema_version = payload.get("schemaVersion")
    if isinstance(incoming_schema_version, int):
        state["schemaVersion"] = incoming_schema_version

    return state


def _write_db_state_sync(state: dict) -> dict:
    normalized = _normalize_db_state(state)
    normalized["updatedAt"] = _utc_now_iso()

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp_path = DB_PATH.with_suffix(".json.tmp")
    temp_path.write_text(
        json.dumps(normalized, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp_path.replace(DB_PATH)
    return normalized


def _read_db_state_sync() -> dict:
    if not DB_PATH.exists():
        return _write_db_state_sync(_default_db_state())

    try:
        raw = json.loads(DB_PATH.read_text(encoding="utf-8"))
    except Exception:
        return _write_db_state_sync(_default_db_state())

    normalized = _normalize_db_state(raw)
    if normalized != raw:
        return _write_db_state_sync(normalized)
    return normalized


async def read_db_state() -> dict:
    async with DB_LOCK:
        return await asyncio.to_thread(_read_db_state_sync)


async def write_db_state(payload: dict) -> dict:
    async with DB_LOCK:
        return await asyncio.to_thread(_write_db_state_sync, payload)


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


# ── ClickHouse helpers ────────────────────────────────────────────────────────

def quote_clickhouse_literal(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def clean_sql_text(sql: str) -> str:
    text = (sql or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9_-]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    return text.strip().rstrip(";").strip()


def is_safe_read_only_sql(sql: str) -> bool:
    cleaned = clean_sql_text(sql).lower()
    if not cleaned:
        return False
    if ";" in cleaned:
        return False
    if not (cleaned.startswith("select") or cleaned.startswith("with")):
        return False
    forbidden = [
        "insert", "update", "delete", "alter", "drop", "truncate", "create",
        "grant", "revoke", "rename", "optimize", "system", "attach", "detach",
    ]
    return not any(re.search(rf"\b{keyword}\b", cleaned) for keyword in forbidden)


def enforce_query_limit(sql: str, limit: int) -> str:
    cleaned = clean_sql_text(sql)
    safe_limit = max(1, min(limit, 1000))
    if re.search(r"\blimit\s+\d+", cleaned, re.IGNORECASE):
        return cleaned
    return f"{cleaned}\nLIMIT {safe_limit}"


def extract_json_object(text: str) -> dict[str, Any]:
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return {}
    try:
        return json.loads(match.group())
    except Exception:
        return {}


def normalize_choice(text: str) -> str:
    cleaned = re.sub(r"^\s*i choose:\s*", "", text or "", flags=re.IGNORECASE)
    return cleaned.strip().strip("`").strip()


def resolve_user_choice(user_text: str, options: list[str]) -> Optional[str]:
    if not options:
        return None
    normalized = normalize_choice(user_text).lower()
    if not normalized:
        return None
    for option in options:
        option_lower = option.lower()
        if normalized == option_lower:
            return option
        explicit_patterns = [
            rf"^(choose|use|pick)\s+{re.escape(option_lower)}$",
            rf"^(the\s+)?{re.escape(option_lower)}\s+table$",
            rf"^table\s+{re.escape(option_lower)}$",
        ]
        if any(re.fullmatch(pattern, normalized) for pattern in explicit_patterns):
            return option
    return None


def clickhouse_url(config: "ClickHouseConfig") -> str:
    scheme = "https" if config.secure else "http"
    base = f"{scheme}://{config.host}:{config.port}"
    path = config.http_path.strip("/")
    return f"{base}/{path}" if path else base


async def execute_clickhouse_sql(
    config: "ClickHouseConfig",
    sql: str,
    readonly: bool = True,
    json_format: bool = True,
) -> dict[str, Any]:
    query = clean_sql_text(sql)
    if not query:
        raise HTTPException(status_code=400, detail="Empty ClickHouse query.")

    if readonly and not is_safe_read_only_sql(query):
        raise HTTPException(status_code=400, detail="Only read-only SELECT queries are allowed.")

    suffix = " FORMAT JSON" if json_format and "format json" not in query.lower() else ""
    final_query = query + suffix
    params = {
        "database": config.database,
        "readonly": 1 if readonly else 0,
        "wait_end_of_query": 1,
        "result_overflow_mode": "break",
        "max_result_rows": max(1, min(config.query_limit, 5000)),
    }
    auth = (config.username, config.password) if config.username else None

    async with httpx.AsyncClient(timeout=60.0, verify=config.verify_ssl) as client:
        response = await client.post(
            clickhouse_url(config),
            params=params,
            content=final_query.encode("utf-8"),
            auth=auth,
            headers={"Content-Type": "text/plain; charset=utf-8"},
        )
        response.raise_for_status()
        if json_format:
            return response.json()
        return {"raw": response.text}


async def list_clickhouse_tables(config: "ClickHouseConfig") -> list[str]:
    result = await execute_clickhouse_sql(
        config,
        (
            "SELECT name FROM system.tables "
            f"WHERE database = {quote_clickhouse_literal(config.database)} "
            "ORDER BY name"
        ),
    )
    return [row.get("name", "") for row in result.get("data", []) if row.get("name")]


async def describe_clickhouse_table(
    config: "ClickHouseConfig",
    table_name: str,
) -> list[dict[str, str]]:
    result = await execute_clickhouse_sql(
        config,
        (
            "SELECT name, type, default_kind, default_expression "
            "FROM system.columns "
            f"WHERE database = {quote_clickhouse_literal(config.database)} "
            f"AND table = {quote_clickhouse_literal(table_name)} "
            "ORDER BY position"
        ),
    )
    return [
        {
            "name": row.get("name", ""),
            "type": row.get("type", ""),
            "default_kind": row.get("default_kind", "") or "",
            "default_expression": row.get("default_expression", "") or "",
        }
        for row in result.get("data", [])
        if row.get("name")
    ]


def find_date_columns(schema: list[dict[str, str]]) -> list[str]:
    date_like = []
    for column in schema:
        col_type = column.get("type", "").lower()
        if any(token in col_type for token in ["date", "time"]):
            date_like.append(column.get("name", ""))
    return [name for name in date_like if name]


def match_schema_columns(candidates: list[str], schema: list[dict[str, str]]) -> list[str]:
    lookup = {
        column.get("name", "").lower(): column.get("name", "")
        for column in schema
        if column.get("name")
    }
    matched: list[str] = []
    for candidate in candidates:
        name = lookup.get(candidate.lower())
        if name and name not in matched:
            matched.append(name)
    return matched

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
    verify_ssl: bool = True,
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
    async with httpx.AsyncClient(timeout=60.0, verify=verify_ssl) as client:
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


# ── ClickHouse agent LLM helpers ──────────────────────────────────────────────

async def analyze_clickhouse_schema(
    user_request: str,
    table_name: str,
    schema: list[dict[str, str]],
    llm_base_url: str,
    llm_model: str,
    llm_provider: str,
    llm_api_key: Optional[str] = None,
) -> dict[str, Any]:
    schema_lines = "\n".join(
        f"- {column['name']}: {column['type']}"
        for column in schema[:120]
    )
    prompt = f"""
You are a ClickHouse query planner.
Analyze the user's request against a single ClickHouse table schema.
Return JSON only with this exact shape:
{{
  "field_candidates": ["col_a", "col_b"],
  "field_choice_required": true,
  "field_choice_prompt": "Which field should I use for ...?",
  "date_candidates": ["created_at", "updated_at"],
  "date_choice_required": true,
  "date_choice_prompt": "Which date column should I use?",
  "needs_date_column": true,
  "reasoning": "short explanation"
}}

Rules:
- Keep prompts in English.
- Only reference columns that exist in the schema.
- If the best field is obvious, return one field candidate and set field_choice_required to false.
- If the request does not need a date column, set needs_date_column to false and leave date_candidates empty.
- If there are multiple plausible date columns and a date context is needed, set date_choice_required to true.

Table: {table_name}
Schema:
{schema_lines}

User request:
{user_request}
""".strip()

    raw = await llm_chat(
        [{"role": "user", "content": prompt}],
        llm_base_url,
        llm_model,
        llm_provider,
        llm_api_key,
        response_format="json",
    )
    parsed = extract_json_object(raw)
    return {
        "field_candidates": [
            item for item in parsed.get("field_candidates", [])
            if isinstance(item, str)
        ],
        "field_choice_required": bool(parsed.get("field_choice_required", False)),
        "field_choice_prompt": str(parsed.get("field_choice_prompt", "") or ""),
        "date_candidates": [
            item for item in parsed.get("date_candidates", [])
            if isinstance(item, str)
        ],
        "date_choice_required": bool(parsed.get("date_choice_required", False)),
        "date_choice_prompt": str(parsed.get("date_choice_prompt", "") or ""),
        "needs_date_column": bool(parsed.get("needs_date_column", False)),
        "reasoning": str(parsed.get("reasoning", "") or ""),
    }


async def generate_clickhouse_sql(
    user_request: str,
    table_name: str,
    schema: list[dict[str, str]],
    selected_field: Optional[str],
    selected_date_field: Optional[str],
    llm_base_url: str,
    llm_model: str,
    llm_provider: str,
    llm_api_key: Optional[str],
    query_limit: int,
    error_feedback: str = "",
) -> dict[str, str]:
    schema_lines = "\n".join(
        f"- {column['name']}: {column['type']}"
        for column in schema[:120]
    )
    prompt = f"""
You are a senior ClickHouse analytics engineer.
Generate exactly one safe read-only SQL query for the user's request.
Return JSON only with:
{{
  "sql": "SELECT ...",
  "reasoning": "short explanation"
}}

Constraints:
- Use only table `{table_name}`.
- Use only columns from the schema below.
- Output English-friendly aliases when useful.
- Never use INSERT, UPDATE, DELETE, ALTER, DROP, CREATE, TRUNCATE, SYSTEM or multiple statements.
- Prefer explicit column names.
- Add a LIMIT when returning raw rows.
- Keep the SQL compatible with ClickHouse.
- If a date column is selected, prefer it when time filtering, grouping or ordering matters.

Selected field: {selected_field or "None"}
Selected date field: {selected_date_field or "None"}
Maximum row limit: {query_limit}
Schema:
{schema_lines}

User request:
{user_request}

{f"Previous execution error to fix: {error_feedback}" if error_feedback else ""}
""".strip()

    raw = await llm_chat(
        [{"role": "user", "content": prompt}],
        llm_base_url,
        llm_model,
        llm_provider,
        llm_api_key,
        response_format="json",
    )
    parsed = extract_json_object(raw)
    sql = clean_sql_text(str(parsed.get("sql", "") or ""))
    reasoning = str(parsed.get("reasoning", "") or "")
    return {"sql": enforce_query_limit(sql, query_limit), "reasoning": reasoning}


async def summarize_clickhouse_result(
    user_request: str,
    executed_sql: str,
    reasoning: str,
    result_rows: list[dict[str, Any]],
    llm_base_url: str,
    llm_model: str,
    llm_provider: str,
    llm_api_key: Optional[str],
) -> str:
    preview = json.dumps(result_rows[:10], ensure_ascii=False, indent=2)
    prompt = f"""
You are summarizing a ClickHouse query result for an end user.
Write the full answer in English and keep it concise.
Use exactly these sections in markdown:
## Answer
One short, precise answer.

## SQL
```sql
{executed_sql}
```

## Reasoning
One short explanation of how the query was chosen.

Context:
- User request: {user_request}
- Planner reasoning: {reasoning or "No extra reasoning provided."}
- Result rows preview:
{preview}
""".strip()

    return await llm_chat(
        [{"role": "user", "content": prompt}],
        llm_base_url,
        llm_model,
        llm_provider,
        llm_api_key,
    )

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


class ClickHouseConfig(BaseModel):
    host: str = "localhost"
    port: int = 8123
    database: str = "default"
    username: str = "default"
    password: str = ""
    secure: bool = False
    verify_ssl: bool = True
    http_path: str = ""
    query_limit: int = 200


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
    embedding_verify_ssl: bool = True
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
    embedding_verify_ssl: bool = True
    knn_neighbors: int = 10
    llm_base_url: str = "http://localhost:11434"
    llm_model: str = "llama3"
    llm_api_key: Optional[str] = None
    llm_provider: str = "ollama"


class ClickHouseTestRequest(BaseModel):
    clickhouse: ClickHouseConfig


class ClickHouseAgentState(BaseModel):
    stage: str = "idle"
    pending_request: str = ""
    available_tables: list[str] = Field(default_factory=list)
    selected_table: Optional[str] = None
    schema: list[dict] = Field(default_factory=list)
    candidate_fields: list[str] = Field(default_factory=list)
    date_fields: list[str] = Field(default_factory=list)
    selected_field: Optional[str] = None
    selected_date_field: Optional[str] = None
    clarification_prompt: str = ""
    clarification_options: list[str] = Field(default_factory=list)


class ClickHouseAgentRequest(BaseModel):
    message: str
    history: list[dict] = []
    clickhouse: ClickHouseConfig
    llm_base_url: str = "http://localhost:11434"
    llm_model: str = "llama3"
    llm_api_key: Optional[str] = None
    llm_provider: str = "ollama"
    agent_state: ClickHouseAgentState = Field(default_factory=ClickHouseAgentState)


class EmbeddingTestRequest(BaseModel):
    embedding_base_url: str
    embedding_model: str
    embedding_api_key: Optional[str] = None
    embedding_verify_ssl: bool = True
    opensearch: Optional[OSConfig] = None


class PersistedPreferences(BaseModel):
    darkMode: bool = False
    currentConversationId: Optional[str] = None
    workflow: str = "LLM"
    agentRole: str = "manager"
    selectedMcpToolId: str = ""
    page: str = "landing"


class PersistedStateRequest(BaseModel):
    config: dict = Field(default_factory=lambda: json.loads(json.dumps(DEFAULT_APP_CONFIG)))
    conversations: list[dict] = Field(default_factory=list)
    preferences: PersistedPreferences = Field(default_factory=PersistedPreferences)


# ── App state persistence endpoints ───────────────────────────────────────────

@app.get("/api/db/state")
async def get_app_state():
    return await read_db_state()


@app.put("/api/db/state")
async def save_app_state(req: PersistedStateRequest):
    payload = {
        "config": req.config,
        "conversations": req.conversations,
        "preferences": req.preferences.model_dump(),
    }
    return await write_db_state(payload)


@app.get("/api/db/export")
async def export_app_state():
    state = await read_db_state()
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return JSONResponse(
        content=state,
        headers={
            "Content-Disposition": f'attachment; filename="ragnarok-db-backup-{timestamp}.json"'
        },
    )


@app.post("/api/db/import")
async def import_app_state(payload: dict):
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid JSON payload for DB import.")
    return await write_db_state(payload)


# ── ClickHouse endpoints ──────────────────────────────────────────────────────

@app.post("/api/clickhouse/test")
async def test_clickhouse_connection(req: ClickHouseTestRequest):
    try:
        info = await execute_clickhouse_sql(
            req.clickhouse,
            "SELECT currentDatabase() AS database_name, version() AS version",
        )
        tables = await list_clickhouse_tables(req.clickhouse)
        first_row = info.get("data", [{}])[0] if info.get("data") else {}
        return {
            "status": "ok",
            "database": first_row.get("database_name", req.clickhouse.database),
            "version": first_row.get("version", "unknown"),
            "tables": tables[:20],
            "table_count": len(tables),
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def build_choice_markdown(title: str, prompt: str, options: list[str]) -> str:
    bullet_list = "\n".join(f"- [ ] {option}" for option in options)
    return f"## {title}\n{prompt}\n\n{bullet_list}"


def reset_clickhouse_clarification(state: ClickHouseAgentState) -> None:
    state.clarification_prompt = ""
    state.clarification_options = []


@app.post("/api/chat/clickhouse-agent")
async def chat_clickhouse_agent(req: ClickHouseAgentRequest):
    state = req.agent_state
    user_message = (req.message or "").strip()

    try:
        state.available_tables = state.available_tables or await list_clickhouse_tables(req.clickhouse)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"ClickHouse connection error: {e}")

    if not state.available_tables:
        raise HTTPException(status_code=400, detail="No tables were found in the configured ClickHouse database.")

    explicit_table_switch = resolve_user_choice(user_message, state.available_tables)
    if state.selected_table and explicit_table_switch and explicit_table_switch != state.selected_table:
        state.selected_table = explicit_table_switch
        state.schema = []
        state.pending_request = ""
        state.candidate_fields = []
        state.date_fields = []
        state.selected_field = None
        state.selected_date_field = None
        state.stage = "ready"
        reset_clickhouse_clarification(state)

    if state.stage == "ready" and user_message and not explicit_table_switch:
        state.pending_request = user_message
        state.candidate_fields = []
        state.date_fields = []
        state.selected_field = None
        state.selected_date_field = None
        reset_clickhouse_clarification(state)

    if not state.selected_table:
        selected_table = resolve_user_choice(user_message, state.available_tables)
        if selected_table:
            state.selected_table = selected_table
            state.stage = "ready"
            reset_clickhouse_clarification(state)
            if not state.pending_request:
                state.pending_request = ""
        else:
            if user_message and not state.pending_request:
                state.pending_request = user_message
            state.stage = "awaiting_table"
            return {
                "answer": build_choice_markdown(
                    "ClickHouse Query Agent",
                    "Choose the table you want me to inspect before I build the SQL for your request.",
                    state.available_tables,
                ),
                "agent_state": state.model_dump(),
                "steps": [
                    {
                        "id": "ch-tables",
                        "title": "Loaded ClickHouse tables",
                        "status": "success",
                        "details": f"Found {len(state.available_tables)} table(s) in database `{req.clickhouse.database}`.",
                    },
                    {
                        "id": "ch-await-table",
                        "title": "Waiting for table selection",
                        "status": "running",
                        "details": "The user must choose one table before the agent can inspect the schema.",
                    },
                ],
            }

    if state.selected_table and not state.pending_request:
        state.pending_request = user_message

    if not state.pending_request:
        return {
            "answer": (
                f"## ClickHouse Query Agent\nI am focused on table `{state.selected_table}`.\n\n"
                "Please tell me what you want to know, and I will inspect the schema before writing SQL."
            ),
            "agent_state": state.model_dump(),
            "steps": [
                {
                    "id": "ch-await-request",
                    "title": "Waiting for analytical request",
                    "status": "running",
                    "details": "The table is selected, but the user has not asked a data question yet.",
                }
            ],
        }

    if not state.schema:
        try:
            state.schema = await describe_clickhouse_table(req.clickhouse, state.selected_table)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to inspect schema for {state.selected_table}: {e}")

    if not state.schema:
        raise HTTPException(status_code=400, detail=f"Table '{state.selected_table}' has no readable columns.")

    if state.stage == "awaiting_field":
        selected_field = resolve_user_choice(user_message, state.clarification_options)
        if not selected_field:
            return {
                "answer": build_choice_markdown(
                    "Field Clarification",
                    state.clarification_prompt or "Please choose the field I should use.",
                    state.clarification_options,
                ),
                "agent_state": state.model_dump(),
                "steps": [
                    {
                        "id": "ch-await-field",
                        "title": "Waiting for field clarification",
                        "status": "running",
                        "details": "The request can map to multiple plausible columns.",
                    }
                ],
            }
        state.selected_field = selected_field
        state.stage = "ready"
        reset_clickhouse_clarification(state)

    if state.stage == "awaiting_date":
        selected_date = resolve_user_choice(user_message, state.clarification_options)
        if not selected_date:
            return {
                "answer": build_choice_markdown(
                    "Date Clarification",
                    state.clarification_prompt or "Please choose the date column I should use.",
                    state.clarification_options,
                ),
                "agent_state": state.model_dump(),
                "steps": [
                    {
                        "id": "ch-await-date",
                        "title": "Waiting for date clarification",
                        "status": "running",
                        "details": "The request needs a date column and multiple date-like columns are available.",
                    }
                ],
            }
        state.selected_date_field = selected_date
        state.stage = "ready"
        reset_clickhouse_clarification(state)

    analysis = await analyze_clickhouse_schema(
        state.pending_request,
        state.selected_table,
        state.schema,
        req.llm_base_url,
        req.llm_model,
        req.llm_provider,
        req.llm_api_key,
    )

    state.candidate_fields = match_schema_columns(analysis["field_candidates"], state.schema)
    detected_date_fields = match_schema_columns(analysis["date_candidates"], state.schema)
    heuristic_dates = find_date_columns(state.schema)
    state.date_fields = detected_date_fields or heuristic_dates

    if analysis["field_choice_required"] and len(state.candidate_fields) > 1 and not state.selected_field:
        state.stage = "awaiting_field"
        state.clarification_prompt = analysis["field_choice_prompt"] or "Which field should I use for this request?"
        state.clarification_options = state.candidate_fields
        return {
            "answer": build_choice_markdown(
                "Field Clarification",
                state.clarification_prompt,
                state.clarification_options,
            ),
            "agent_state": state.model_dump(),
            "steps": [
                {
                    "id": "ch-schema",
                    "title": "Inspected table schema",
                    "status": "success",
                    "details": f"Loaded {len(state.schema)} columns from `{state.selected_table}`.",
                },
                {
                    "id": "ch-field-clarification",
                    "title": "Need field confirmation",
                    "status": "running",
                    "details": analysis["reasoning"] or "Several columns look plausible for the request.",
                },
            ],
        }

    if not state.selected_field and len(state.candidate_fields) == 1:
        state.selected_field = state.candidate_fields[0]

    if analysis["needs_date_column"] and len(state.date_fields) > 1 and not state.selected_date_field:
        state.stage = "awaiting_date"
        state.clarification_prompt = analysis["date_choice_prompt"] or "Which date column should I use for this request?"
        state.clarification_options = state.date_fields
        return {
            "answer": build_choice_markdown(
                "Date Clarification",
                state.clarification_prompt,
                state.clarification_options,
            ),
            "agent_state": state.model_dump(),
            "steps": [
                {
                    "id": "ch-schema",
                    "title": "Inspected table schema",
                    "status": "success",
                    "details": f"Loaded {len(state.schema)} columns from `{state.selected_table}`.",
                },
                {
                    "id": "ch-date-clarification",
                    "title": "Need date confirmation",
                    "status": "running",
                    "details": "More than one date-like column can satisfy the request.",
                },
            ],
        }

    if not state.selected_date_field and len(state.date_fields) == 1:
        state.selected_date_field = state.date_fields[0]

    generated = await generate_clickhouse_sql(
        state.pending_request,
        state.selected_table,
        state.schema,
        state.selected_field,
        state.selected_date_field,
        req.llm_base_url,
        req.llm_model,
        req.llm_provider,
        req.llm_api_key,
        req.clickhouse.query_limit,
    )

    sql = generated["sql"]
    if not is_safe_read_only_sql(sql):
        raise HTTPException(status_code=400, detail="The generated SQL was rejected because it is not a safe read-only query.")

    try:
        await execute_clickhouse_sql(req.clickhouse, f"EXPLAIN SYNTAX {sql}", readonly=False, json_format=False)
        result = await execute_clickhouse_sql(req.clickhouse, sql)
    except Exception as first_error:
        repaired = await generate_clickhouse_sql(
            state.pending_request,
            state.selected_table,
            state.schema,
            state.selected_field,
            state.selected_date_field,
            req.llm_base_url,
            req.llm_model,
            req.llm_provider,
            req.llm_api_key,
            req.clickhouse.query_limit,
            error_feedback=str(first_error),
        )
        sql = repaired["sql"]
        if not is_safe_read_only_sql(sql):
            raise HTTPException(status_code=400, detail="The repaired SQL was rejected because it is not read-only.")
        generated["reasoning"] = repaired["reasoning"] or generated["reasoning"]
        result = await execute_clickhouse_sql(req.clickhouse, sql)

    answer = await summarize_clickhouse_result(
        state.pending_request,
        sql,
        generated["reasoning"],
        result.get("data", []),
        req.llm_base_url,
        req.llm_model,
        req.llm_provider,
        req.llm_api_key,
    )

    state.stage = "ready"
    state.pending_request = ""
    reset_clickhouse_clarification(state)

    return {
        "answer": answer,
        "agent_state": state.model_dump(),
        "sql": sql,
        "steps": [
            {
                "id": "ch-selected-table",
                "title": "Selected ClickHouse table",
                "status": "success",
                "details": f"Using table `{state.selected_table}`.",
            },
            {
                "id": "ch-inspect-schema",
                "title": "Inspected schema",
                "status": "success",
                "details": f"Loaded {len(state.schema)} columns to map the request safely.",
            },
            {
                "id": "ch-generate-sql",
                "title": "Generated safe SQL",
                "status": "success",
                "details": generated["reasoning"] or "Built a read-only ClickHouse query with the configured local LLM.",
            },
            {
                "id": "ch-execute",
                "title": "Executed query",
                "status": "success",
                "details": f"Returned {len(result.get('data', []))} row(s).",
            },
        ],
    }


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
            "embedding connectivity test", req.embedding_base_url, req.embedding_model,
            req.embedding_api_key, verify_ssl=req.embedding_verify_ssl
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
                chunk, req.embedding_base_url, req.embedding_model,
                req.embedding_api_key, verify_ssl=req.embedding_verify_ssl
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
            search_text, req.embedding_base_url, req.embedding_model,
            req.embedding_api_key, verify_ssl=req.embedding_verify_ssl
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
