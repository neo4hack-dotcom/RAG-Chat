"""
RAGnarok — FastAPI Backend (OpenSearch edition)
Embeddings via Ollama/OpenAI-compatible endpoint.
Vector storage and kNN search via opensearch-py.
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import AliasChoices, BaseModel, ConfigDict, Field
from contextlib import asynccontextmanager
import httpx
import json
import re
import uuid
import os
import asyncio
import fnmatch
import hashlib
import csv
import shutil
import math
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from opensearchpy import OpenSearch
from urllib.parse import urlparse
from mcp import ClientSession
from mcp.client.sse import sse_client


@asynccontextmanager
async def ragnarok_lifespan(app: FastAPI):
    await read_db_state()
    stop_event = asyncio.Event()
    app.state.planning_scheduler_stop = stop_event
    app.state.planning_scheduler_task = asyncio.create_task(planning_scheduler_loop(stop_event))
    try:
        yield
    finally:
        stop_event = getattr(app.state, "planning_scheduler_stop", None)
        task = getattr(app.state, "planning_scheduler_task", None)
        if stop_event is not None:
            stop_event.set()
        if task is not None:
            try:
                await task
            except Exception:
                pass


app = FastAPI(title="RAGnarok API", lifespan=ragnarok_lifespan)

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
    "settingsAccessPassword": "MM@2026",
    "clickhouseHost": "localhost",
    "clickhousePort": 8123,
    "clickhouseDatabase": "default",
    "clickhouseUsername": "default",
    "clickhousePassword": "",
    "clickhouseSecure": False,
    "clickhouseVerifySsl": True,
    "clickhouseHttpPath": "",
    "clickhouseQueryLimit": 200,
    "fileManagerConfig": {
        "basePath": "",
        "maxIterations": 10,
        "systemPrompt": (
            "You are the File Management agent. Reply in English by default. Use "
            "filesystem tools instead of guessing, keep answers short and factual, "
            "and ask for confirmation before destructive or overwrite actions."
        ),
    },
}

DEFAULT_PREFERENCES = {
    "darkMode": False,
    "currentConversationId": None,
    "workflow": "LLM",
    "agentRole": "manager",
    "selectedMcpToolId": "",
    "page": "landing",
}

AGENT_ROLES = {"manager", "clickhouse_query", "file_management", "data_quality_tables"}
PLANNER_AGENT_ROLES = {"manager", "clickhouse_query", "file_management"}
PLANNER_TRIGGER_KINDS = {
    "once",
    "daily",
    "weekly",
    "interval",
    "clickhouse_watch",
    "file_watch",
}
PLANNER_WEEKDAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
PLANNER_WATCH_MODES = {"returns_rows", "count_increases", "result_changes"}
PLANNER_MAX_RUNS = 60
PLANNER_MAX_KNOWN_FILES = 2000
PLANNER_LOOP_INTERVAL_SECONDS = 20
CHAT_MEMORY_MIN_STEPS = 5
CHAT_MEMORY_MAX_STEPS = 10
DATA_QUALITY_MAX_GUIDED_TABLES = 20
DATA_QUALITY_MAX_SAMPLE_ROWS = 2_000_000
DATA_QUALITY_DEFAULT_SAMPLE_SIZE = 50_000
DATA_QUALITY_STRING_SENTINELS = ["n/a", "na", "null", "none", "unknown", "-1", "9999", "99999"]


def _default_planning_state() -> dict:
    return {
        "plans": [],
        "runs": [],
    }


def _default_planning_trigger() -> dict:
    return {
        "kind": "daily",
        "timezone": "UTC",
        "oneTimeAt": "",
        "timeOfDay": "09:00",
        "weekdays": ["mon"],
        "intervalMinutes": 60,
        "pollMinutes": 5,
        "watchSql": "",
        "watchMode": "result_changes",
        "directory": "",
        "pattern": "*",
        "recursive": False,
    }


def _default_planning_runtime() -> dict:
    return {
        "lastCheckedAt": None,
        "lastSeenFingerprint": "",
        "lastSeenMetric": None,
        "knownFiles": [],
    }


def _normalize_planning_trigger(trigger: Optional[dict]) -> dict:
    normalized = _default_planning_trigger()
    if not isinstance(trigger, dict):
        return normalized

    kind = trigger.get("kind")
    normalized["kind"] = kind if kind in PLANNER_TRIGGER_KINDS else normalized["kind"]
    timezone_name = str(trigger.get("timezone") or normalized["timezone"]).strip() or normalized["timezone"]
    normalized["timezone"] = timezone_name
    normalized["oneTimeAt"] = str(trigger.get("oneTimeAt") or "").strip()
    normalized["timeOfDay"] = str(trigger.get("timeOfDay") or normalized["timeOfDay"]).strip() or normalized["timeOfDay"]
    weekdays = trigger.get("weekdays")
    if isinstance(weekdays, list):
        normalized["weekdays"] = [
            day for day in weekdays
            if isinstance(day, str) and day.lower() in PLANNER_WEEKDAYS
        ] or normalized["weekdays"]
    interval_minutes = trigger.get("intervalMinutes")
    if isinstance(interval_minutes, (int, float)):
        normalized["intervalMinutes"] = max(1, int(interval_minutes))
    poll_minutes = trigger.get("pollMinutes")
    if isinstance(poll_minutes, (int, float)):
        normalized["pollMinutes"] = max(1, int(poll_minutes))
    normalized["watchSql"] = str(trigger.get("watchSql") or "").strip()
    watch_mode = trigger.get("watchMode")
    normalized["watchMode"] = watch_mode if watch_mode in PLANNER_WATCH_MODES else normalized["watchMode"]
    normalized["directory"] = str(trigger.get("directory") or "").strip()
    normalized["pattern"] = str(trigger.get("pattern") or normalized["pattern"]).strip() or normalized["pattern"]
    normalized["recursive"] = bool(trigger.get("recursive", normalized["recursive"]))
    return normalized


def _normalize_planning_plan(plan: Optional[dict]) -> dict:
    normalized = {
        "id": uuid.uuid4().hex,
        "name": "",
        "prompt": "",
        "agents": [],
        "status": "active",
        "trigger": _default_planning_trigger(),
        "createdAt": "",
        "updatedAt": "",
        "nextRunAt": None,
        "lastRunAt": None,
        "lastStatus": None,
        "lastSummary": "",
        "runtime": _default_planning_runtime(),
    }
    if not isinstance(plan, dict):
        return normalized

    normalized["id"] = str(plan.get("id") or normalized["id"])
    normalized["name"] = str(plan.get("name") or "").strip()
    normalized["prompt"] = str(plan.get("prompt") or plan.get("objective") or "").strip()
    agents = plan.get("agents")
    if isinstance(agents, list):
        normalized["agents"] = [
            agent for agent in agents
            if isinstance(agent, str) and agent in PLANNER_AGENT_ROLES
        ]
    status = plan.get("status")
    normalized["status"] = status if status in {"active", "paused"} else "active"
    normalized["trigger"] = _normalize_planning_trigger(plan.get("trigger"))
    normalized["createdAt"] = str(plan.get("createdAt") or "").strip()
    normalized["updatedAt"] = str(plan.get("updatedAt") or "").strip()
    normalized["nextRunAt"] = plan.get("nextRunAt") if isinstance(plan.get("nextRunAt"), str) or plan.get("nextRunAt") is None else None
    normalized["lastRunAt"] = plan.get("lastRunAt") if isinstance(plan.get("lastRunAt"), str) or plan.get("lastRunAt") is None else None
    normalized["lastStatus"] = plan.get("lastStatus") if plan.get("lastStatus") in {"running", "success", "error", None} else None
    normalized["lastSummary"] = str(plan.get("lastSummary") or "").strip()

    runtime = _default_planning_runtime()
    incoming_runtime = plan.get("runtime")
    if isinstance(incoming_runtime, dict):
        runtime["lastCheckedAt"] = incoming_runtime.get("lastCheckedAt") if isinstance(incoming_runtime.get("lastCheckedAt"), str) or incoming_runtime.get("lastCheckedAt") is None else None
        runtime["lastSeenFingerprint"] = str(incoming_runtime.get("lastSeenFingerprint") or "").strip()
        last_seen_metric = incoming_runtime.get("lastSeenMetric")
        if isinstance(last_seen_metric, (int, float)) or last_seen_metric is None:
            runtime["lastSeenMetric"] = last_seen_metric
        known_files = incoming_runtime.get("knownFiles")
        if isinstance(known_files, list):
            runtime["knownFiles"] = [
                str(path) for path in known_files
                if isinstance(path, str)
            ][:PLANNER_MAX_KNOWN_FILES]
    normalized["runtime"] = runtime
    return normalized


def _normalize_planning_run(run: Optional[dict]) -> dict:
    normalized = {
        "id": uuid.uuid4().hex,
        "planId": "",
        "planName": "Unnamed plan",
        "triggerKind": "manual",
        "triggerLabel": "",
        "startedAt": "",
        "finishedAt": None,
        "status": "running",
        "summary": "",
        "outputs": [],
    }
    if not isinstance(run, dict):
        return normalized

    normalized["id"] = str(run.get("id") or normalized["id"])
    normalized["planId"] = str(run.get("planId") or "").strip()
    normalized["planName"] = str(run.get("planName") or normalized["planName"]).strip()
    trigger_kind = str(run.get("triggerKind") or "manual").strip()
    normalized["triggerKind"] = (
        trigger_kind if trigger_kind in PLANNER_TRIGGER_KINDS or trigger_kind == "manual" else "manual"
    )
    normalized["triggerLabel"] = str(run.get("triggerLabel") or "").strip()
    normalized["startedAt"] = str(run.get("startedAt") or "").strip()
    normalized["finishedAt"] = run.get("finishedAt") if isinstance(run.get("finishedAt"), str) or run.get("finishedAt") is None else None
    status = run.get("status")
    normalized["status"] = status if status in {"running", "success", "error"} else "running"
    normalized["summary"] = str(run.get("summary") or "").strip()
    outputs = run.get("outputs")
    if isinstance(outputs, list):
        normalized["outputs"] = [
            {
                "agent": str(item.get("agent") or "").strip(),
                "status": item.get("status") if item.get("status") in {"success", "error"} else "success",
                "content": str(item.get("content") or "").strip(),
            }
            for item in outputs
            if isinstance(item, dict)
        ]
    return normalized


def _normalize_planning_state(payload: Optional[dict]) -> dict:
    state = _default_planning_state()
    if not isinstance(payload, dict):
        return state

    plans = payload.get("plans")
    if isinstance(plans, list):
        state["plans"] = [_normalize_planning_plan(plan) for plan in plans]

    runs = payload.get("runs")
    if isinstance(runs, list):
        state["runs"] = [_normalize_planning_run(run) for run in runs[:PLANNER_MAX_RUNS]]

    return state


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_db_state() -> dict:
    return {
        "schemaVersion": 1,
        "updatedAt": _utc_now_iso(),
        "config": json.loads(json.dumps(DEFAULT_APP_CONFIG)),
        "conversations": [],
        "preferences": json.loads(json.dumps(DEFAULT_PREFERENCES)),
        "planning": _default_planning_state(),
    }


def _normalize_db_state(payload: Optional[dict]) -> dict:
    state = _default_db_state()
    if not isinstance(payload, dict):
        return state

    incoming_config = payload.get("config")
    if isinstance(incoming_config, dict):
        state["config"].update(incoming_config)
        incoming_file_manager = incoming_config.get("fileManagerConfig")
        if isinstance(incoming_file_manager, dict):
            state["config"]["fileManagerConfig"] = {
                **DEFAULT_APP_CONFIG["fileManagerConfig"],
                **incoming_file_manager,
            }

    incoming_conversations = payload.get("conversations")
    if isinstance(incoming_conversations, list):
        state["conversations"] = incoming_conversations

    incoming_preferences = payload.get("preferences")
    if isinstance(incoming_preferences, dict):
        state["preferences"].update(incoming_preferences)
    if state["preferences"].get("agentRole") not in AGENT_ROLES:
        state["preferences"]["agentRole"] = "manager"

    state["planning"] = _normalize_planning_state(payload.get("planning"))

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
    if not (
        cleaned.startswith("select")
        or cleaned.startswith("with")
        or cleaned.startswith("show")
        or cleaned.startswith("describe")
        or cleaned.startswith("desc")
        or cleaned.startswith("exists")
        or cleaned.startswith("explain")
    ):
        return False
    forbidden = [
        "insert", "update", "delete", "alter", "drop", "truncate", "create",
        "grant", "revoke", "rename", "optimize", "attach", "detach",
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


def normalize_intent_text(text: str) -> str:
    normalized = normalize_choice(text).lower()
    if not normalized:
        return ""
    without_accents = "".join(
        char
        for char in unicodedata.normalize("NFKD", normalized)
        if not unicodedata.combining(char)
    )
    return re.sub(r"\s+", " ", without_accents).strip()


def _normalized_history_messages(
    history: list[dict[str, Any]],
    current_message: Optional[str] = None,
    max_steps: int = CHAT_MEMORY_MAX_STEPS,
) -> list[dict[str, str]]:
    safe_max_steps = max(CHAT_MEMORY_MIN_STEPS, max_steps)
    normalized: list[dict[str, str]] = []
    for item in history:
        role = str(item.get("role") or "user")
        if role not in {"user", "assistant", "system"}:
            continue
        content = str(item.get("content") or "").strip()
        if not content:
            continue
        normalized.append(
            {
                "role": role,
                "content": _truncate_text_preview(content, 1200),
            }
        )

    if current_message:
        current_normalized = normalize_choice(current_message).lower()
        while normalized:
            last = normalized[-1]
            if last["role"] != "user":
                break
            if normalize_choice(last["content"]).lower() != current_normalized:
                break
            normalized.pop()

    return normalized[-safe_max_steps:]


def _conversation_memory_markdown(
    history: list[dict[str, Any]],
    current_message: Optional[str] = None,
    max_steps: int = CHAT_MEMORY_MAX_STEPS,
) -> str:
    trimmed = _normalized_history_messages(history, current_message=current_message, max_steps=max_steps)
    if not trimmed:
        return "No recent memory."
    lines = []
    for item in trimmed:
        label = "User" if item["role"] == "user" else "Assistant"
        lines.append(f"- {label}: {item['content']}")
    return "\n".join(lines)


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


def match_available_options(candidates: list[str], options: list[str]) -> list[str]:
    lookup = {
        option.lower(): option
        for option in options
        if option
    }
    matched: list[str] = []
    for candidate in candidates:
        option = lookup.get((candidate or "").lower())
        if option and option not in matched:
            matched.append(option)
    return matched


def quote_clickhouse_identifier(name: str) -> str:
    escaped = (name or "").replace("`", "``")
    return f"`{escaped}`"


def classify_clickhouse_column_type(type_name: str) -> str:
    lowered = (type_name or "").lower()
    if any(token in lowered for token in ["date", "time"]):
        return "date"
    if any(token in lowered for token in ["int", "float", "decimal", "double"]):
        return "numeric"
    if any(token in lowered for token in ["string", "fixedstring", "uuid", "enum"]):
        return "string"
    return "other"


def _default_data_quality_state() -> dict[str, Any]:
    return {
        "stage": "idle",
        "table": None,
        "columns": [],
        "sample_size": DATA_QUALITY_DEFAULT_SAMPLE_SIZE,
        "row_filter": "",
        "time_column": None,
        "db_type": "clickhouse",
        "schema_info": [],
        "column_stats": {},
        "volumetric_stats": None,
        "llm_analysis": "",
        "final_answer": "",
        "agent_id": "data_quality_tables",
        "session_id": uuid.uuid4().hex,
        "last_error": "",
        "available_tables": [],
        "available_columns": [],
        "date_columns": [],
    }


def _normalize_data_quality_state(payload: Optional[dict]) -> dict[str, Any]:
    state = _default_data_quality_state()
    if not isinstance(payload, dict):
        return state

    schema_info = payload.get("schema_info") if isinstance(payload.get("schema_info"), list) else payload.get("schemaInfo")
    state["stage"] = str(payload.get("stage") or state["stage"]).strip() or state["stage"]
    state["table"] = str(payload.get("table")).strip() if payload.get("table") else None
    state["columns"] = [
        str(item).strip()
        for item in (payload.get("columns") or [])
        if isinstance(item, str) and str(item).strip()
    ]
    sample_size = payload.get("sample_size") if "sample_size" in payload else payload.get("sampleSize")
    if isinstance(sample_size, (int, float)):
        state["sample_size"] = max(0, min(DATA_QUALITY_MAX_SAMPLE_ROWS, int(sample_size)))
    state["row_filter"] = str(payload.get("row_filter") or payload.get("rowFilter") or "").strip()
    time_column = payload.get("time_column") if "time_column" in payload else payload.get("timeColumn")
    state["time_column"] = str(time_column).strip() if isinstance(time_column, str) and time_column.strip() else None
    db_type = str(payload.get("db_type") or payload.get("dbType") or "clickhouse").lower()
    state["db_type"] = "oracle" if db_type == "oracle" else "clickhouse"
    state["schema_info"] = [
        {
            "name": str(column.get("name") or "").strip(),
            "type": str(column.get("type") or "").strip(),
            "category": classify_clickhouse_column_type(str(column.get("type") or "")),
        }
        for column in (schema_info or [])
        if isinstance(column, dict) and str(column.get("name") or "").strip()
    ]
    state["column_stats"] = payload.get("column_stats") if isinstance(payload.get("column_stats"), dict) else payload.get("columnStats") if isinstance(payload.get("columnStats"), dict) else {}
    state["volumetric_stats"] = payload.get("volumetric_stats") if isinstance(payload.get("volumetric_stats"), dict) else payload.get("volumetricStats") if isinstance(payload.get("volumetricStats"), dict) else None
    llm_analysis = payload.get("llm_analysis") if "llm_analysis" in payload else payload.get("llmAnalysis")
    if isinstance(llm_analysis, str):
        state["llm_analysis"] = llm_analysis
    elif isinstance(llm_analysis, dict):
        state["llm_analysis"] = json.dumps(llm_analysis, ensure_ascii=False)
    state["final_answer"] = str(payload.get("final_answer") or payload.get("finalAnswer") or "").strip()
    state["agent_id"] = str(payload.get("agent_id") or payload.get("agentId") or state["agent_id"]).strip() or state["agent_id"]
    state["session_id"] = str(payload.get("session_id") or payload.get("sessionId") or state["session_id"]).strip() or state["session_id"]
    state["last_error"] = str(payload.get("last_error") or payload.get("lastError") or "").strip()
    state["available_tables"] = [
        str(item).strip()
        for item in (payload.get("available_tables") or payload.get("availableTables") or [])
        if isinstance(item, str) and str(item).strip()
    ]
    state["available_columns"] = [
        str(item).strip()
        for item in (payload.get("available_columns") or payload.get("availableColumns") or [])
        if isinstance(item, str) and str(item).strip()
    ]
    state["date_columns"] = [
        str(item).strip()
        for item in (payload.get("date_columns") or payload.get("dateColumns") or [])
        if isinstance(item, str) and str(item).strip()
    ]
    return state


DATA_QUALITY_FOLLOWUP_STAGES = {
    "awaiting_table",
    "awaiting_columns_mode",
    "awaiting_custom_columns",
    "awaiting_sample_size",
    "awaiting_custom_sample_size",
    "awaiting_row_filter_mode",
    "awaiting_row_filter",
    "awaiting_time_column",
    "awaiting_review",
}


def _data_quality_state_needs_followup(state: dict[str, Any]) -> bool:
    return str(state.get("stage") or "").strip() in DATA_QUALITY_FOLLOWUP_STAGES


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _round_metric(value: Any, digits: int = 4) -> Any:
    numeric = _safe_float(value)
    if numeric is None:
        return value
    if math.isfinite(numeric):
        return round(numeric, digits)
    return value


def _first_row(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data") or []
    return data[0] if data else {}


def _data_quality_scan_limit(sample_size: int) -> int:
    if sample_size <= 0:
        return DATA_QUALITY_MAX_SAMPLE_ROWS
    return max(1, min(sample_size, DATA_QUALITY_MAX_SAMPLE_ROWS))


def _validate_data_quality_row_filter(row_filter: str) -> Optional[str]:
    text = (row_filter or "").strip()
    if not text:
        return None
    lowered = text.lower()
    if ";" in text or "--" in text or "/*" in text or "*/" in text:
        return "The row filter must be a single safe boolean expression without comments or semicolons."
    forbidden = ["drop", "delete", "insert", "update", "create", "alter", "exec", "union", "sleep"]
    if any(re.search(rf"\b{keyword}\b", lowered) for keyword in forbidden):
        return "The row filter contains blocked keywords and was rejected for safety reasons."
    return None


def _match_data_quality_columns(column_names: list[str], schema_info: list[dict[str, Any]]) -> list[str]:
    available = {
        str(column.get("name") or "").lower(): str(column.get("name") or "")
        for column in schema_info
        if column.get("name")
    }
    matched: list[str] = []
    for column_name in column_names:
        found = available.get(str(column_name or "").strip().lower())
        if found and found not in matched:
            matched.append(found)
    return matched


def _parse_custom_column_input(text: str) -> list[str]:
    chunks = re.split(r"[\n,]+", text or "")
    return [chunk.strip().strip("`") for chunk in chunks if chunk.strip()]


def _build_data_quality_source_sql(
    table_name: str,
    columns: list[str],
    row_filter: str,
    sample_size: int,
) -> str:
    selected_columns = ", ".join(quote_clickhouse_identifier(column) for column in columns) if columns else "*"
    sql = f"SELECT {selected_columns} FROM {quote_clickhouse_identifier(table_name)}"
    if row_filter:
        sql += f" WHERE ({row_filter})"
    sql += f"\nLIMIT {_data_quality_scan_limit(sample_size)}"
    return sql


def _data_quality_percentile(values: list[float], q: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _data_quality_numeric_severity(stats: dict[str, Any]) -> tuple[str, list[str]]:
    reasons: list[str] = []
    null_pct = _safe_float(stats.get("null_pct")) or 0.0
    iqr_pct = _safe_float(stats.get("iqr_outlier_pct")) or 0.0
    zscore_pct = _safe_float(stats.get("zscore_outlier_pct")) or 0.0
    if null_pct > 20:
        reasons.append(f"Null rate is {round(null_pct, 2)}%, which is critical.")
    elif null_pct > 5:
        reasons.append(f"Null rate is {round(null_pct, 2)}%, which is a warning.")
    if iqr_pct > 10 or zscore_pct > 10:
        reasons.append("Outlier volume is above the critical threshold.")
    elif iqr_pct > 2 or zscore_pct > 2:
        reasons.append("Outlier volume is above the warning threshold.")
    severity = "ok"
    if any("critical" in reason.lower() for reason in reasons):
        severity = "critical"
    elif reasons:
        severity = "warning"
    return severity, reasons


def _data_quality_generic_severity(stats: dict[str, Any]) -> tuple[str, list[str]]:
    reasons: list[str] = []
    null_pct = _safe_float(stats.get("null_pct")) or 0.0
    sentinel_pct = _safe_float(stats.get("sentinel_pct")) or 0.0
    distinct_pct = _safe_float(stats.get("distinct_pct")) or 0.0
    if null_pct > 20:
        reasons.append(f"Null rate is {round(null_pct, 2)}%, which is critical.")
    elif null_pct > 5:
        reasons.append(f"Null rate is {round(null_pct, 2)}%, which is a warning.")
    if sentinel_pct > 5:
        reasons.append(f"Sentinel values represent {round(sentinel_pct, 2)}%, which is critical.")
    elif sentinel_pct > 0:
        reasons.append(f"Sentinel values are present at {round(sentinel_pct, 2)}%.")
    if distinct_pct > 95 and (stats.get("category") == "string"):
        reasons.append("Cardinality is very high for a text field and may indicate identifier-like data.")
    severity = "ok"
    if any("critical" in reason.lower() for reason in reasons):
        severity = "critical"
    elif reasons:
        severity = "warning"
    return severity, reasons


def _finalize_data_quality_stats(stats: dict[str, Any]) -> dict[str, Any]:
    row_count = int(stats.get("row_count") or 0)
    null_count = int(stats.get("null_count") or 0)
    nonnull_count = max(0, row_count - null_count)
    stats["nonnull_count"] = nonnull_count
    stats["null_pct"] = round((null_count / row_count) * 100, 2) if row_count else 0.0
    distinct_count = int(stats.get("distinct_count") or 0)
    stats["distinct_pct"] = round((distinct_count / nonnull_count) * 100, 2) if nonnull_count else 0.0

    if stats.get("category") == "numeric":
        severity, reasons = _data_quality_numeric_severity(stats)
    else:
        severity, reasons = _data_quality_generic_severity(stats)

    stats["severity_hint"] = severity
    stats["severity_icon"] = "🔴" if severity == "critical" else "🟡" if severity == "warning" else "🟢"
    stats["issues"] = reasons
    return stats


async def _data_quality_numeric_stats(
    config: "ClickHouseConfig",
    source_sql: str,
    column_name: str,
) -> dict[str, Any]:
    identifier = quote_clickhouse_identifier(column_name)
    summary = await execute_clickhouse_sql(
        config,
        f"""
        SELECT
          count() AS row_count,
          countIf(isNull(value)) AS null_count,
          uniqExact(value) AS distinct_count,
          min(value) AS min_value,
          max(value) AS max_value,
          avg(value) AS avg_value,
          stddevPop(value) AS stddev_value,
          quantilesExactInclusive(0.25, 0.5, 0.75)(value) AS quartiles,
          countIf(value = 0) AS zero_count,
          countIf(value < 0) AS negative_count
        FROM (
          SELECT toFloat64OrNull({identifier}) AS value
          FROM ({source_sql}) AS src
        ) AS profile
        """,
    )
    row = _first_row(summary)
    quartiles = row.get("quartiles") or [None, None, None]
    q1 = _safe_float(quartiles[0] if len(quartiles) > 0 else None)
    median = _safe_float(quartiles[1] if len(quartiles) > 1 else None)
    q3 = _safe_float(quartiles[2] if len(quartiles) > 2 else None)
    iqr = (q3 - q1) if q1 is not None and q3 is not None else None
    lower_fence = (q1 - 1.5 * iqr) if iqr is not None else None
    upper_fence = (q3 + 1.5 * iqr) if iqr is not None else None
    avg_value = _safe_float(row.get("avg_value"))
    stddev_value = _safe_float(row.get("stddev_value"))

    iqr_outlier_count = 0
    zscore_outlier_count = 0
    if lower_fence is not None and upper_fence is not None:
        outlier_query = await execute_clickhouse_sql(
            config,
            f"""
            SELECT
              countIf(value < {lower_fence} OR value > {upper_fence}) AS iqr_outlier_count,
              countIf(abs((value - {avg_value or 0}) / {stddev_value or 1}) > 3) AS zscore_outlier_count
            FROM (
              SELECT toFloat64OrNull({identifier}) AS value
              FROM ({source_sql}) AS src
            ) AS profile
            WHERE NOT isNull(value)
            """,
        )
        outlier_row = _first_row(outlier_query)
        iqr_outlier_count = int(outlier_row.get("iqr_outlier_count") or 0)
        zscore_outlier_count = int(outlier_row.get("zscore_outlier_count") or 0) if stddev_value and stddev_value > 0 else 0

    stats = {
        "category": "numeric",
        "row_count": int(row.get("row_count") or 0),
        "null_count": int(row.get("null_count") or 0),
        "distinct_count": int(row.get("distinct_count") or 0),
        "min": _round_metric(row.get("min_value")),
        "max": _round_metric(row.get("max_value")),
        "avg": _round_metric(avg_value),
        "stddev": _round_metric(stddev_value),
        "p25": _round_metric(q1),
        "p50": _round_metric(median),
        "p75": _round_metric(q3),
        "iqr": _round_metric(iqr),
        "lower_fence": _round_metric(lower_fence),
        "upper_fence": _round_metric(upper_fence),
        "zero_count": int(row.get("zero_count") or 0),
        "negative_count": int(row.get("negative_count") or 0),
        "iqr_outlier_count": iqr_outlier_count,
        "zscore_outlier_count": zscore_outlier_count,
    }
    nonnull_count = max(1, stats["row_count"] - stats["null_count"])
    stats["iqr_outlier_pct"] = round((iqr_outlier_count / nonnull_count) * 100, 2)
    stats["zscore_outlier_pct"] = round((zscore_outlier_count / nonnull_count) * 100, 2)
    stats["coeff_variation"] = round(abs((stddev_value or 0.0) / avg_value), 4) if avg_value not in (None, 0) else None
    stats["skewness_approx"] = round((3 * ((avg_value or 0.0) - (median or 0.0)) / stddev_value), 4) if stddev_value not in (None, 0) and median is not None else None
    return _finalize_data_quality_stats(stats)


async def _data_quality_string_stats(
    config: "ClickHouseConfig",
    source_sql: str,
    column_name: str,
) -> dict[str, Any]:
    identifier = quote_clickhouse_identifier(column_name)
    sentinel_values = ", ".join(quote_clickhouse_literal(value) for value in DATA_QUALITY_STRING_SENTINELS)
    query = await execute_clickhouse_sql(
        config,
        f"""
        SELECT
          (SELECT count() FROM ({source_sql}) AS src) AS row_count,
          (SELECT countIf(isNull(toNullable({identifier}))) FROM ({source_sql}) AS src) AS null_count,
          count() AS nonnull_count,
          countIf(trimmed = '') AS empty_count,
          uniqExact(trimmed) AS distinct_count,
          min(lengthUTF8(trimmed)) AS min_length,
          max(lengthUTF8(trimmed)) AS max_length,
          avg(lengthUTF8(trimmed)) AS avg_length,
          countIf(lengthUTF8(trimmed) > 1000) AS very_long_count,
          countIf(match(raw_text, '^\\s') OR match(raw_text, '\\s$')) AS edge_space_count,
          countIf(match(trimmed, '[A-Za-z]') AND trimmed = upperUTF8(trimmed)) AS uppercase_count,
          countIf(match(trimmed, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{{2,}}$')) AS email_like_count,
          countIf(match(trimmed, '^[-+]?[0-9]+(\\\\.[0-9]+)?$')) AS numeric_like_count,
          countIf(lowerUTF8(trimmed) IN ({sentinel_values})) AS sentinel_count
        FROM (
          SELECT
            toString({identifier}) AS raw_text,
            replaceRegexpAll(toString({identifier}), '^\\s+|\\s+$', '') AS trimmed
          FROM ({source_sql}) AS src
          WHERE NOT isNull(toNullable({identifier}))
        ) AS profile
        """,
    )
    row = _first_row(query)
    stats = {
        "category": "string",
        "row_count": int(row.get("row_count") or 0),
        "null_count": int(row.get("null_count") or 0),
        "distinct_count": int(row.get("distinct_count") or 0),
        "empty_count": int(row.get("empty_count") or 0),
        "min_length": int(row.get("min_length") or 0) if row.get("min_length") is not None else None,
        "max_length": int(row.get("max_length") or 0) if row.get("max_length") is not None else None,
        "avg_length": _round_metric(row.get("avg_length")),
        "very_long_count": int(row.get("very_long_count") or 0),
        "edge_space_count": int(row.get("edge_space_count") or 0),
        "uppercase_count": int(row.get("uppercase_count") or 0),
        "email_like_count": int(row.get("email_like_count") or 0),
        "numeric_like_count": int(row.get("numeric_like_count") or 0),
        "sentinel_count": int(row.get("sentinel_count") or 0),
    }
    nonnull_count = max(1, stats["row_count"] - stats["null_count"])
    stats["sentinel_pct"] = round((stats["sentinel_count"] / nonnull_count) * 100, 2)
    return _finalize_data_quality_stats(stats)


async def _data_quality_date_stats(
    config: "ClickHouseConfig",
    source_sql: str,
    column_name: str,
) -> dict[str, Any]:
    identifier = quote_clickhouse_identifier(column_name)
    query = await execute_clickhouse_sql(
        config,
        f"""
        SELECT
          (SELECT count() FROM ({source_sql}) AS src) AS row_count,
          (SELECT countIf(isNull(toNullable({identifier}))) FROM ({source_sql}) AS src) AS null_count,
          count() AS nonnull_count,
          uniqExact(parsed_dt) AS distinct_count,
          min(parsed_dt) AS min_value,
          max(parsed_dt) AS max_value,
          countIf(parsed_dt > now()) AS future_count,
          countIf(parsed_dt < toDateTime('1970-01-02 00:00:00')) AS epoch_like_count,
          countIf(parsed_dt < toDateTime('1900-01-01 00:00:00')) AS pre_1900_count,
          countIf(toDayOfWeek(parsed_dt) >= 6) AS weekend_count
        FROM (
          SELECT parseDateTimeBestEffortOrNull(toString({identifier})) AS parsed_dt
          FROM ({source_sql}) AS src
          WHERE NOT isNull(toNullable({identifier}))
        ) AS profile
        WHERE NOT isNull(parsed_dt)
        """,
    )
    row = _first_row(query)
    stats = {
        "category": "date",
        "row_count": int(row.get("row_count") or 0),
        "null_count": int(row.get("null_count") or 0),
        "distinct_count": int(row.get("distinct_count") or 0),
        "min": row.get("min_value"),
        "max": row.get("max_value"),
        "future_count": int(row.get("future_count") or 0),
        "epoch_like_count": int(row.get("epoch_like_count") or 0),
        "pre_1900_count": int(row.get("pre_1900_count") or 0),
        "weekend_count": int(row.get("weekend_count") or 0),
        "sentinel_count": 0,
        "sentinel_pct": 0.0,
    }
    return _finalize_data_quality_stats(stats)


async def data_quality_schema_node(
    config: "ClickHouseConfig",
    state: dict[str, Any],
) -> dict[str, Any]:
    state["available_tables"] = await list_clickhouse_tables(config)
    if state.get("table"):
        raw_schema = await describe_clickhouse_table(config, state["table"])
        state["schema_info"] = [
            {
                "name": column["name"],
                "type": column["type"],
                "category": classify_clickhouse_column_type(column["type"]),
            }
            for column in raw_schema
        ]
        state["available_columns"] = [column["name"] for column in state["schema_info"]]
        state["date_columns"] = [column["name"] for column in state["schema_info"] if column["category"] == "date"]
    return state


async def data_quality_stats_node(
    config: "ClickHouseConfig",
    state: dict[str, Any],
) -> dict[str, Any]:
    if not state.get("table") or not state.get("columns"):
        raise ValueError("Table and columns are required before profiling can start.")
    source_sql = _build_data_quality_source_sql(
        state["table"],
        state["columns"],
        state.get("row_filter") or "",
        int(state.get("sample_size") or DATA_QUALITY_DEFAULT_SAMPLE_SIZE),
    )
    state["column_stats"] = {}
    schema_lookup = {
        column["name"]: column
        for column in state.get("schema_info", [])
        if column.get("name")
    }
    for column_name in state["columns"]:
        schema_entry = schema_lookup.get(column_name, {})
        category = schema_entry.get("category") or classify_clickhouse_column_type(schema_entry.get("type", ""))
        if category == "numeric":
            state["column_stats"][column_name] = await _data_quality_numeric_stats(config, source_sql, column_name)
        elif category == "string":
            state["column_stats"][column_name] = await _data_quality_string_stats(config, source_sql, column_name)
        elif category == "date":
            state["column_stats"][column_name] = await _data_quality_date_stats(config, source_sql, column_name)
        else:
            state["column_stats"][column_name] = {
                "category": "other",
                "severity_hint": "warning",
                "severity_icon": "🟡",
                "issues": ["This column type is not fully profiled yet."],
            }
    return state


async def data_quality_volumetric_node(
    config: "ClickHouseConfig",
    state: dict[str, Any],
) -> dict[str, Any]:
    time_column = state.get("time_column")
    if not time_column:
        state["volumetric_stats"] = None
        return state

    source_sql = _build_data_quality_source_sql(
        state["table"],
        [time_column],
        state.get("row_filter") or "",
        int(state.get("sample_size") or DATA_QUALITY_DEFAULT_SAMPLE_SIZE),
    )
    identifier = quote_clickhouse_identifier(time_column)
    range_query = await execute_clickhouse_sql(
        config,
        f"""
        SELECT
          min(parsed_time) AS min_time,
          max(parsed_time) AS max_time,
          count() AS profiled_rows
        FROM (
          SELECT parseDateTimeBestEffortOrNull(toString({identifier})) AS parsed_time
          FROM ({source_sql}) AS src
        ) AS profile
        WHERE NOT isNull(parsed_time)
        """,
    )
    range_row = _first_row(range_query)
    min_time = range_row.get("min_time")
    max_time = range_row.get("max_time")
    if not min_time or not max_time:
        state["volumetric_stats"] = None
        return state

    min_dt = _parse_iso_datetime(str(min_time)) if "T" in str(min_time) else None
    max_dt = _parse_iso_datetime(str(max_time)) if "T" in str(max_time) else None
    if min_dt and max_dt:
        span_days = max(0, int((max_dt - min_dt).total_seconds() // 86400))
    else:
        span_days = 8
    granularity = "hour" if span_days <= 7 else "day"
    bucket_expression = "toStartOfHour(parsed_time)" if granularity == "hour" else "toStartOfDay(parsed_time)"

    volume_query = await execute_clickhouse_sql(
        config,
        f"""
        SELECT
          {bucket_expression} AS bucket,
          count() AS volume
        FROM (
          SELECT parseDateTimeBestEffortOrNull(toString({identifier})) AS parsed_time
          FROM ({source_sql}) AS src
        ) AS profile
        WHERE NOT isNull(parsed_time)
        GROUP BY bucket
        ORDER BY bucket
        """,
    )
    buckets = [
        {
            "bucket": row.get("bucket"),
            "volume": int(row.get("volume") or 0),
        }
        for row in (volume_query.get("data") or [])
    ]
    volumes = [bucket["volume"] for bucket in buckets]
    if not volumes:
        state["volumetric_stats"] = None
        return state

    q1 = _data_quality_percentile([float(value) for value in volumes], 0.25)
    q3 = _data_quality_percentile([float(value) for value in volumes], 0.75)
    iqr = (q3 - q1) if q1 is not None and q3 is not None else None
    lower_fence = max(0.0, (q1 - 1.5 * iqr)) if iqr is not None else None
    avg_volume = sum(volumes) / len(volumes)
    stddev_volume = math.sqrt(sum((value - avg_volume) ** 2 for value in volumes) / len(volumes))
    anomalously_low = [
        bucket for bucket in buckets
        if lower_fence is not None and bucket["volume"] < lower_fence
    ]
    state["volumetric_stats"] = {
        "granularity": granularity,
        "min_time": min_time,
        "max_time": max_time,
        "bucket_count": len(buckets),
        "avg_volume": round(avg_volume, 2),
        "stddev_volume": round(stddev_volume, 2),
        "q1": _round_metric(q1),
        "q3": _round_metric(q3),
        "iqr": _round_metric(iqr),
        "lower_fence": _round_metric(lower_fence),
        "anomalously_low_periods": anomalously_low[:12],
        "series_preview": buckets[:24],
    }
    return state


def _compact_data_quality_payload(state: dict[str, Any]) -> dict[str, Any]:
    compact_columns = {}
    for column_name, stats in (state.get("column_stats") or {}).items():
        compact_columns[column_name] = {
            "category": stats.get("category"),
            "severity_hint": stats.get("severity_hint"),
            "null_pct": stats.get("null_pct"),
            "distinct_pct": stats.get("distinct_pct"),
            "issues": stats.get("issues") or [],
            "key_metrics": {
                key: value
                for key, value in stats.items()
                if key in {
                    "min", "max", "avg", "stddev", "p25", "p50", "p75", "iqr",
                    "zero_count", "negative_count", "iqr_outlier_pct", "zscore_outlier_pct",
                    "empty_count", "sentinel_pct", "avg_length", "very_long_count",
                    "future_count", "epoch_like_count", "pre_1900_count", "weekend_count",
                }
            },
        }
    return {
        "table": state.get("table"),
        "columns": state.get("columns"),
        "sample_size": state.get("sample_size"),
        "row_filter": state.get("row_filter") or "",
        "time_column": state.get("time_column"),
        "db_type": state.get("db_type"),
        "column_stats": compact_columns,
        "volumetric_stats": state.get("volumetric_stats"),
    }


def _data_quality_python_fallback_analysis(state: dict[str, Any]) -> dict[str, Any]:
    column_entries = []
    critical_count = 0
    warning_count = 0
    ok_count = 0
    score = 100

    for column_name, stats in (state.get("column_stats") or {}).items():
        severity = stats.get("severity_hint") or "ok"
        if severity == "critical":
            critical_count += 1
            score -= 15
        elif severity == "warning":
            warning_count += 1
            score -= 5
        else:
            ok_count += 1
        column_entries.append(
            {
                "name": column_name,
                "severity": severity,
                "headline": (
                    stats.get("issues")[0]
                    if stats.get("issues")
                    else "No major issue detected in the sampled profile."
                ),
                "business_risk": (
                    "Data quality issues may reduce reporting trust and downstream decision accuracy."
                    if severity != "ok"
                    else "No immediate business risk was detected in the sampled data."
                ),
            }
        )

    return {
        "global_score": max(0, score),
        "critical_count": critical_count,
        "warning_count": warning_count,
        "ok_count": ok_count,
        "columns": column_entries,
        "top_recommendations": [
            "Prioritize columns marked as critical before using this table for reporting or automation.",
            "Add validation rules for nulls, sentinel values, and format anomalies at ingestion time.",
            "Review row filters and sample size choices if you need a narrower business scope.",
        ],
        "volumetric_summary": (
            "No volumetric anomaly was computed."
            if not state.get("volumetric_stats")
            else "Volumetric analysis is included below and highlights low-volume periods."
        ),
    }


async def data_quality_llm_analysis_node(
    state: dict[str, Any],
    llm_base_url: str,
    llm_model: str,
    llm_provider: str,
    llm_api_key: Optional[str],
) -> dict[str, Any]:
    payload = _compact_data_quality_payload(state)
    prompt = f"""
You are a senior data-quality analyst.
Analyze the profiling payload below and return JSON only with this exact shape:
{{
  "global_score": 0,
  "critical_count": 0,
  "warning_count": 0,
  "ok_count": 0,
  "columns": [
    {{
      "name": "column_name",
      "severity": "critical" | "warning" | "ok",
      "headline": "short finding summary",
      "business_risk": "short business risk"
    }}
  ],
  "top_recommendations": ["recommendation 1", "recommendation 2"],
  "volumetric_summary": "short summary if volumetric analysis exists"
}}

Scoring rules:
- Critical: null_pct > 20, outliers > 10, sentinel_pct > 5 => -15 points
- Warning: null_pct 5-20, outliers 2-10, suspicious cardinality => -5 points
- OK: no meaningful issue => 0 points

Keep everything in English.
Use the payload only. Do not invent metrics that are not provided.

Payload:
{json.dumps(payload, ensure_ascii=False, indent=2)}
""".strip()

    try:
        raw = await llm_chat(
            [{"role": "user", "content": prompt}],
            llm_base_url,
            llm_model,
            llm_provider,
            llm_api_key,
            response_format="json",
        )
        parsed = extract_json_object(raw)
        if not parsed:
            raise ValueError("Empty data-quality analysis payload from the LLM.")
        state["llm_analysis"] = json.dumps(parsed, ensure_ascii=False)
        return parsed
    except Exception:
        fallback = _data_quality_python_fallback_analysis(state)
        state["llm_analysis"] = json.dumps(fallback, ensure_ascii=False)
        return fallback


def data_quality_synthesizer_node(
    state: dict[str, Any],
    llm_analysis: dict[str, Any],
) -> str:
    lines = [
        "## Executive Summary",
        f"Global score: {int(llm_analysis.get('global_score') or 0)}/100",
        (
            f"Critical issues: {int(llm_analysis.get('critical_count') or 0)} | "
            f"Warnings: {int(llm_analysis.get('warning_count') or 0)} | "
            f"OK: {int(llm_analysis.get('ok_count') or 0)}"
        ),
        "",
        "## Analysis by Column",
    ]

    analysis_by_name = {
        str(item.get("name")): item
        for item in (llm_analysis.get("columns") or [])
        if isinstance(item, dict) and item.get("name")
    }
    for column_name in state.get("columns") or []:
        stats = (state.get("column_stats") or {}).get(column_name, {})
        entry = analysis_by_name.get(column_name, {})
        severity = str(entry.get("severity") or stats.get("severity_hint") or "ok")
        icon = "🔴" if severity == "critical" else "🟡" if severity == "warning" else "🟢"
        label = "Critical" if severity == "critical" else "Warning" if severity == "warning" else "OK"
        lines.append(f"### `{column_name}` — {icon} [{label}]")
        key_bits = [
            f"null_pct={stats.get('null_pct', 0)}%",
            f"distinct_pct={stats.get('distinct_pct', 0)}%",
        ]
        if stats.get("category") == "numeric":
            key_bits.extend([
                f"min={stats.get('min')}",
                f"max={stats.get('max')}",
                f"avg={stats.get('avg')}",
                f"iqr_outliers={stats.get('iqr_outlier_pct', 0)}%",
            ])
        elif stats.get("category") == "string":
            key_bits.extend([
                f"avg_length={stats.get('avg_length')}",
                f"sentinel_pct={stats.get('sentinel_pct', 0)}%",
                f"empty_count={stats.get('empty_count', 0)}",
            ])
        elif stats.get("category") == "date":
            key_bits.extend([
                f"min={stats.get('min')}",
                f"max={stats.get('max')}",
                f"future_count={stats.get('future_count', 0)}",
            ])
        lines.append(f"Key metrics: {', '.join(key_bits)}")
        lines.append(str(entry.get("headline") or (stats.get("issues") or ["No major issue detected."])[0]))
        lines.append(str(entry.get("business_risk") or "No business risk summary was generated."))
        lines.append("")

    lines.extend(["## Top Recommendations"])
    recommendations = llm_analysis.get("top_recommendations") or []
    if recommendations:
        lines.extend(f"- {recommendation}" for recommendation in recommendations[:6])
    else:
        lines.append("- No recommendation was generated.")

    if state.get("volumetric_stats"):
        volumetric = state["volumetric_stats"]
        lines.extend([
            "",
            "## Volumetric Analysis",
            str(llm_analysis.get("volumetric_summary") or "Volumetric analysis was requested for the selected time column."),
            (
                f"Granularity: {volumetric.get('granularity')} | "
                f"Buckets: {volumetric.get('bucket_count')} | "
                f"Average volume: {volumetric.get('avg_volume')} | "
                f"Stddev: {volumetric.get('stddev_volume')}"
            ),
        ])
        low_periods = volumetric.get("anomalously_low_periods") or []
        if low_periods:
            lines.append("Low-volume periods:")
            lines.extend(
                f"- {item.get('bucket')}: {item.get('volume')}"
                for item in low_periods[:8]
            )

    return "\n".join(lines).strip()


DATA_QUALITY_ALL_COLUMNS_OPTION = "All columns"
DATA_QUALITY_NUMERIC_COLUMNS_OPTION = "Numeric columns only"
DATA_QUALITY_TEXT_COLUMNS_OPTION = "Text columns only"
DATA_QUALITY_DATE_COLUMNS_OPTION = "Date columns only"
DATA_QUALITY_CUSTOM_COLUMNS_OPTION = "Custom column list"
DATA_QUALITY_SKIP_TIME_OPTION = "Skip volumetric analysis"
DATA_QUALITY_SKIP_ROW_FILTER_OPTION = "Skip row filter"
DATA_QUALITY_ENTER_ROW_FILTER_OPTION = "Enter row filter manually"
DATA_QUALITY_SAMPLE_OPTIONS = {
    "50,000 rows": 50_000,
    "100,000 rows": 100_000,
    "500,000 rows": 500_000,
    f"Full scan (capped at {DATA_QUALITY_MAX_SAMPLE_ROWS:,} rows)": 0,
}
DATA_QUALITY_CUSTOM_SAMPLE_OPTION = "Custom sample size"
DATA_QUALITY_REVIEW_OPTIONS = [
    "Launch analysis",
    "Edit table",
    "Edit columns",
    "Edit sample size",
    "Edit row filter",
    "Edit time column",
    "Start over",
]


def _data_quality_agent_steps(
    stage_id: str,
    title: str,
    status: str,
    details: str,
    extra_steps: Optional[list[dict[str, Any]]] = None,
) -> list[dict[str, Any]]:
    steps = [
        {
            "id": stage_id,
            "title": title,
            "status": status,
            "details": details,
        }
    ]
    if extra_steps:
        steps.extend(extra_steps)
    return steps


def _data_quality_table_options(state: dict[str, Any]) -> list[str]:
    return (state.get("available_tables") or [])[:DATA_QUALITY_MAX_GUIDED_TABLES]


def _data_quality_column_mode_options(state: dict[str, Any]) -> list[str]:
    schema_info = state.get("schema_info") or []
    options = [DATA_QUALITY_ALL_COLUMNS_OPTION]
    if any(column.get("category") == "numeric" for column in schema_info):
        options.append(DATA_QUALITY_NUMERIC_COLUMNS_OPTION)
    if any(column.get("category") == "string" for column in schema_info):
        options.append(DATA_QUALITY_TEXT_COLUMNS_OPTION)
    if any(column.get("category") == "date" for column in schema_info):
        options.append(DATA_QUALITY_DATE_COLUMNS_OPTION)
    options.append(DATA_QUALITY_CUSTOM_COLUMNS_OPTION)
    return options


def _data_quality_columns_for_mode(mode: str, schema_info: list[dict[str, Any]]) -> list[str]:
    if mode == DATA_QUALITY_ALL_COLUMNS_OPTION:
        return [column["name"] for column in schema_info if column.get("name")]
    if mode == DATA_QUALITY_NUMERIC_COLUMNS_OPTION:
        return [column["name"] for column in schema_info if column.get("category") == "numeric"]
    if mode == DATA_QUALITY_TEXT_COLUMNS_OPTION:
        return [column["name"] for column in schema_info if column.get("category") == "string"]
    if mode == DATA_QUALITY_DATE_COLUMNS_OPTION:
        return [column["name"] for column in schema_info if column.get("category") == "date"]
    return []


def _data_quality_review_payload(state: dict[str, Any]) -> dict[str, Any]:
    raw_sample_size = state.get("sample_size")
    payload = {
        "__dq__": True,
        "table": state.get("table"),
        "columns": state.get("columns") or [],
        "sample_size": 0 if raw_sample_size == 0 else int(raw_sample_size or DATA_QUALITY_DEFAULT_SAMPLE_SIZE),
    }
    if state.get("row_filter"):
        payload["row_filter"] = state["row_filter"]
    if state.get("time_column"):
        payload["time_column"] = state["time_column"]
    return payload


def _data_quality_review_markdown(state: dict[str, Any]) -> str:
    row_filter = state.get("row_filter") or "No row filter"
    time_column = state.get("time_column") or "No volumetric analysis"
    selected_columns = state.get("columns") or []
    lines = [
        "## Data Quality Review",
        f"- Table: `{state.get('table') or 'Not selected yet'}`",
        f"- Columns: {', '.join(f'`{column}`' for column in selected_columns) if selected_columns else 'Not selected yet'}",
        (
            "- Sample size: "
            + (
                f"Full scan capped at {DATA_QUALITY_MAX_SAMPLE_ROWS:,} rows"
                if int(state.get("sample_size") or 0) == 0
                else f"{int(state.get('sample_size') or DATA_QUALITY_DEFAULT_SAMPLE_SIZE):,} rows"
            )
        ),
        f"- Row filter: {row_filter}",
        f"- Time column: {time_column}",
        "",
        "## Structured Payload",
        "```json",
        json.dumps(_data_quality_review_payload(state), ensure_ascii=False, indent=2),
        "```",
        "",
        "## Next Step",
        "Launch the analysis or edit one of the parameters below.",
    ]
    return "\n".join(lines)


def _data_quality_intro_markdown(database_name: str, table_options: list[str], total_tables: int) -> str:
    lines = [
        "## Data quality - Tables",
        (
            "I can guide you through a table quality assessment in English, then run statistical profiling "
            "and local-LLM scoring."
        ),
        "",
        "## What I Need",
        "- A target table",
        "- Which columns to profile",
        "- A sample size (or a capped full scan)",
        "- An optional row filter",
        "- An optional time column for volumetric analysis",
        "",
        f"I found {total_tables} table(s) in `{database_name}`.",
    ]
    if total_tables > len(table_options):
        lines.append(
            f"Choose from the suggestions below or type another exact table name from the remaining {total_tables - len(table_options)} table(s)."
        )
    else:
        lines.append("Choose the table you want to profile.")
    return "\n".join(lines)


def _data_quality_guess_table_from_message(user_message: str, available_tables: list[str]) -> Optional[str]:
    direct = resolve_user_choice(user_message, available_tables)
    if direct:
        return direct
    normalized = normalize_choice(user_message).lower()
    if not normalized:
        return None
    matches = [
        table for table in available_tables
        if re.search(rf"(?<![a-z0-9_]){re.escape(table.lower())}(?![a-z0-9_])", normalized)
    ]
    if len(matches) == 1:
        return matches[0]
    return None


def _data_quality_guess_columns_from_message(
    user_message: str,
    schema_info: list[dict[str, Any]],
) -> list[str]:
    normalized = normalize_choice(user_message).lower()
    if not normalized:
        return []
    exact = []
    for column in schema_info:
        name = str(column.get("name") or "").strip()
        if not name:
            continue
        if re.search(rf"(?<![a-z0-9_]){re.escape(name.lower())}(?![a-z0-9_])", normalized):
            exact.append(name)
    return exact


def _try_extract_data_quality_payload(user_message: str) -> Optional[dict[str, Any]]:
    parsed = extract_json_object(user_message)
    if isinstance(parsed, dict) and parsed.get("__dq__") is True:
        return parsed
    return None


CHART_CREATE_OPTION = "Create a chart"
CHART_SKIP_OPTION = "Keep text answer only"
CHART_TYPE_LABELS = {
    "bar": "Bar chart",
    "line": "Line chart",
    "area": "Area chart",
    "scatter": "Scatter plot",
}
CHART_TYPE_BY_LABEL = {label.lower(): key for key, label in CHART_TYPE_LABELS.items()}


def dump_clickhouse_agent_state(state: ClickHouseAgentState) -> dict[str, Any]:
    return state.model_dump(by_alias=True)


def reset_clickhouse_chart_state(state: ClickHouseAgentState) -> None:
    state.chart_requested = False
    state.chart_suggested = False
    state.chart_offer_options = []
    state.chart_x_options = []
    state.chart_y_options = []
    state.chart_type_options = []
    state.selected_chart_x = None
    state.selected_chart_y = None
    state.selected_chart_type = None


def reset_clickhouse_query_resolution(state: ClickHouseAgentState) -> None:
    state.candidate_fields = []
    state.date_fields = []
    state.selected_field = None
    state.selected_date_field = None
    reset_clickhouse_clarification(state)
    reset_clickhouse_chart_state(state)


def detect_chart_request(text: str) -> bool:
    normalized = (text or "").lower()
    return any(
        keyword in normalized
        for keyword in [
            "chart", "graph", "plot", "visual", "visualize", "visualise",
            "dashboard", "trend chart", "bar chart", "line chart", "scatter",
            "graphique", "graphe", "courbe", "histogram",
        ]
    )


def is_chart_followup_request(text: str) -> bool:
    normalized = (text or "").lower().strip()
    return any(
        phrase in normalized
        for phrase in [
            "create a chart",
            "generate a chart",
            "show a chart",
            "make a chart",
            "plot this",
            "graph this",
            "visualize this",
            "visualise this",
            "show me a graph",
            "create graph",
            "make graph",
        ]
    )


def is_affirmative_response(text: str) -> bool:
    normalized = normalize_choice(text).lower()
    return normalized in {
        "yes",
        "y",
        "ok",
        "okay",
        "sure",
        "please do",
        "do it",
        "go ahead",
        "why not",
        "yes please",
    }


def is_negative_response(text: str) -> bool:
    normalized = normalize_choice(text).lower()
    return normalized in {
        "no",
        "n",
        "nope",
        "not now",
        "skip",
        "cancel",
        "keep text",
        "text only",
        "no thanks",
    }


def detect_requested_chart_type(text: str) -> Optional[str]:
    normalized = (text or "").lower()
    if "scatter" in normalized:
        return "scatter"
    if "area" in normalized:
        return "area"
    if "line" in normalized or "curve" in normalized:
        return "line"
    if "bar" in normalized or "histogram" in normalized:
        return "bar"
    return None


def is_numeric_clickhouse_type(type_name: str) -> bool:
    lowered = (type_name or "").lower()
    return any(
        token in lowered
        for token in [
            "int", "float", "decimal", "numeric", "double", "real",
        ]
    ) and "interval" not in lowered


def is_temporal_clickhouse_type(type_name: str) -> bool:
    lowered = (type_name or "").lower()
    return "date" in lowered or "time" in lowered


def normalize_chart_value(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.replace(",", ""))
        except ValueError:
            return None
    return None


def infer_chart_options(meta: list[dict], rows: list[dict]) -> dict[str, Any]:
    if len(rows) < 2:
        return {
            "can_chart": False,
            "recommended": False,
            "x_options": [],
            "y_options": [],
            "type_options": [],
        }

    numeric_columns = [col["name"] for col in meta if is_numeric_clickhouse_type(col.get("type", ""))]
    temporal_columns = [col["name"] for col in meta if is_temporal_clickhouse_type(col.get("type", ""))]
    text_columns = [
        col["name"] for col in meta
        if col.get("name") not in numeric_columns and col.get("name") not in temporal_columns
    ]

    x_options = temporal_columns + text_columns
    if not x_options and len(numeric_columns) >= 2:
        x_options = numeric_columns[:-1]

    y_options = numeric_columns

    if not x_options or not y_options:
        return {
            "can_chart": False,
            "recommended": False,
            "x_options": [],
            "y_options": [],
            "type_options": [],
        }

    unique_counts = {
        column_name: len({str(row.get(column_name, "")) for row in rows if row.get(column_name) is not None})
        for column_name in x_options
    }

    filtered_x_options = [
        column_name for column_name in x_options
        if unique_counts.get(column_name, 0) <= min(40, len(rows))
    ] or x_options

    uses_temporal_x = any(column_name in temporal_columns for column_name in filtered_x_options)
    uses_numeric_x = all(column_name in numeric_columns for column_name in filtered_x_options)

    type_options = ["Bar chart", "Line chart", "Area chart"]
    if uses_numeric_x and len(numeric_columns) >= 2:
        type_options = ["Scatter plot", "Line chart", "Bar chart"]
    elif uses_temporal_x:
        type_options = ["Line chart", "Area chart", "Bar chart"]

    recommended = len(filtered_x_options) > 0 and len(y_options) > 0 and len(rows) >= 3
    return {
        "can_chart": True,
        "recommended": recommended,
        "x_options": filtered_x_options,
        "y_options": y_options,
        "type_options": type_options,
    }


def build_chart(
    rows: list[dict],
    x_field: str,
    y_field: str,
    chart_type: str,
) -> Optional[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for row in rows:
        x_raw = row.get(x_field)
        y_raw = normalize_chart_value(row.get(y_field))
        if x_raw is None or y_raw is None:
            continue
        points.append({"x": str(x_raw), "y": y_raw})

    if len(points) < 2:
        return None

    points = points[:30]
    return {
        "type": chart_type,
        "title": f"{y_field} by {x_field}",
        "xField": x_field,
        "yField": y_field,
        "points": points,
    }


def initialize_chart_selection(
    state: ClickHouseAgentState,
    x_options: list[str],
    y_options: list[str],
    type_options: list[str],
    requested_chart_type: Optional[str] = None,
) -> None:
    state.chart_requested = True
    state.chart_x_options = x_options
    state.chart_y_options = y_options
    state.chart_type_options = type_options

    if not state.selected_chart_x and len(x_options) == 1:
        state.selected_chart_x = x_options[0]

    filtered_y_options = [
        option for option in y_options
        if option != state.selected_chart_x
    ] or y_options

    if not state.selected_chart_y and len(filtered_y_options) == 1:
        state.selected_chart_y = filtered_y_options[0]

    if (
        not state.selected_chart_type
        and requested_chart_type
        and CHART_TYPE_LABELS.get(requested_chart_type) in type_options
    ):
        state.selected_chart_type = requested_chart_type

    if not state.selected_chart_type and len(type_options) == 1:
        state.selected_chart_type = CHART_TYPE_BY_LABEL.get(type_options[0].lower())


def next_chart_prompt(state: ClickHouseAgentState) -> Optional[dict[str, Any]]:
    x_options = state.chart_x_options
    y_options = [
        option for option in state.chart_y_options
        if option != state.selected_chart_x
    ] or state.chart_y_options

    if not state.selected_chart_x:
        state.stage = "awaiting_chart_x"
        return {
            "title": "Chart X Axis",
            "prompt": "Choose the field to use on the X axis.",
            "options": x_options,
            "step_id": "ch-chart-x",
            "step_title": "Waiting for X axis selection",
            "step_details": "The user must choose which field should drive the horizontal axis.",
        }

    if not state.selected_chart_y:
        state.stage = "awaiting_chart_y"
        return {
            "title": "Chart Y Axis",
            "prompt": "Choose the metric to use on the Y axis.",
            "options": y_options,
            "step_id": "ch-chart-y",
            "step_title": "Waiting for Y axis selection",
            "step_details": "The user must choose the metric to visualize.",
        }

    if not state.selected_chart_type:
        state.stage = "awaiting_chart_type"
        return {
            "title": "Chart Type",
            "prompt": "Choose the chart type.",
            "options": state.chart_type_options,
            "step_id": "ch-chart-type",
            "step_title": "Waiting for chart type",
            "step_details": "The user must choose how to visualize the selected axes.",
        }

    return None

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


# ── File Management agent helpers ────────────────────────────────────────────

FILE_PREVIEW_CHAR_LIMIT = 3000
FILE_TABULAR_PREVIEW_ROWS = 50
FILE_SEARCH_RESULTS_LIMIT = 20
FILE_MANAGER_MAX_ITERATIONS = 15
FILE_MANAGER_WRITE_TOOL_LIMIT = 1000
FILE_MANAGER_TEXT_EXTENSIONS = {
    ".txt", ".md", ".py", ".sql", ".json", ".yaml", ".yml", ".html", ".css",
    ".js", ".ts", ".tsx", ".jsx", ".csv", ".tsv", ".log", ".ini", ".toml",
    ".xml", ".sh", ".env", ".rst",
}
FILE_MANAGER_SPREADSHEET_EXTENSIONS = {".csv", ".tsv", ".xlsx", ".xls"}
FILE_MANAGER_CONFIRMATION_TOOLS = {
    "write_file",
    "delete_file",
    "delete_directory",
    "move_file",
    "write_excel_sheet",
    "edit_excel_cells",
    "delete_excel_sheet",
}


def _truncate_text_preview(text: str, limit: int = FILE_PREVIEW_CHAR_LIMIT) -> str:
    value = str(text or "")
    return value if len(value) <= limit else value[: limit - 1] + "…"


def _file_tool_result(
    summary: str,
    *,
    preview: str = "",
    data: Any = None,
    visited_path: str = "",
    requires_confirmation: bool = False,
    pending_action: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    return {
        "summary": summary.strip(),
        "preview": _truncate_text_preview(preview),
        "data": data,
        "visited_path": visited_path,
        "requires_confirmation": requires_confirmation,
        "pending_action": pending_action,
    }


def _import_openpyxl():
    try:
        from openpyxl import Workbook, load_workbook
        return Workbook, load_workbook
    except ImportError as exc:
        raise ValueError("The optional dependency `openpyxl` is required for Excel tools.") from exc


def _import_xlrd():
    try:
        import xlrd
        return xlrd
    except ImportError as exc:
        raise ValueError("The optional dependency `xlrd` is required to read `.xls` files.") from exc


def _import_docx_document():
    try:
        from docx import Document
        return Document
    except ImportError as exc:
        raise ValueError("The optional dependency `python-docx` is required to read `.docx` files.") from exc


def _import_pyarrow_parquet():
    try:
        import pyarrow.parquet as pq
        return pq
    except ImportError as exc:
        raise ValueError("The optional dependency `pyarrow` is required to read `.parquet` files.") from exc


def _resolve_agent_path(path_value: str, base_path: str = "") -> Path:
    raw_path = str(path_value or ".").strip() or "."
    sandbox_root = Path(base_path).expanduser().resolve() if base_path else None

    if os.path.isabs(raw_path):
        resolved = Path(raw_path).expanduser().resolve()
    else:
        anchor = sandbox_root or Path.cwd().resolve()
        resolved = (anchor / raw_path).resolve()

    if sandbox_root:
        try:
            resolved.relative_to(sandbox_root)
        except ValueError as exc:
            raise ValueError("Path escapes the configured base_path sandbox.") from exc

    return resolved


def _ensure_parent_directory(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _read_text_file(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8", errors="replace")


def _format_table_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No rows found."
    headers = list(rows[0].keys())
    header_line = "| " + " | ".join(headers) + " |"
    divider = "| " + " | ".join(["---"] * len(headers)) + " |"
    body_lines = []
    for row in rows[:FILE_TABULAR_PREVIEW_ROWS]:
        body_lines.append("| " + " | ".join(str(row.get(header, "")) for header in headers) + " |")
    return "\n".join([header_line, divider, *body_lines])


def _normalize_excel_rows(headers: list[Any], rows: list[list[Any]]) -> list[dict[str, Any]]:
    safe_headers = []
    for index, header in enumerate(headers, start=1):
        value = str(header).strip() if header is not None else ""
        safe_headers.append(value or f"column_{index}")
    return [
        {safe_headers[idx]: row[idx] if idx < len(row) else "" for idx in range(len(safe_headers))}
        for row in rows
    ]


def _load_excel_sheet_preview(path: Path, sheet_name: Optional[str] = None) -> tuple[str, list[dict[str, Any]], str]:
    extension = path.suffix.lower()
    if extension == ".xlsx":
        _, load_workbook = _import_openpyxl()
        workbook = load_workbook(path, read_only=True, data_only=True)
        target_sheet = sheet_name or workbook.sheetnames[0]
        if target_sheet not in workbook.sheetnames:
            raise ValueError(f"Sheet `{target_sheet}` was not found.")
        worksheet = workbook[target_sheet]
        rows = list(worksheet.iter_rows(values_only=True))
        headers = list(rows[0]) if rows else []
        data_rows = [list(row) for row in rows[1:1 + FILE_TABULAR_PREVIEW_ROWS]] if len(rows) > 1 else []
        preview_rows = _normalize_excel_rows(headers, data_rows) if headers else []
        return target_sheet, preview_rows, ", ".join(workbook.sheetnames)

    if extension == ".xls":
        xlrd = _import_xlrd()
        workbook = xlrd.open_workbook(path)
        target_sheet = sheet_name or workbook.sheet_names()[0]
        worksheet = workbook.sheet_by_name(target_sheet)
        headers = worksheet.row_values(0) if worksheet.nrows > 0 else []
        data_rows = [worksheet.row_values(index) for index in range(1, min(worksheet.nrows, FILE_TABULAR_PREVIEW_ROWS + 1))]
        preview_rows = _normalize_excel_rows(headers, data_rows) if headers else []
        return target_sheet, preview_rows, ", ".join(workbook.sheet_names())

    raise ValueError("Excel tools support `.xlsx` and `.xls` files only.")


def _read_csv_rows(path: Path, delimiter: Optional[str] = None) -> tuple[list[str], list[dict[str, Any]], int]:
    detected_delimiter = delimiter or ("\t" if path.suffix.lower() == ".tsv" else ",")
    rows_preview: list[dict[str, Any]] = []
    total_rows = 0
    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=detected_delimiter)
        headers = reader.fieldnames or []
        for row in reader:
            total_rows += 1
            if len(rows_preview) < FILE_TABULAR_PREVIEW_ROWS:
                rows_preview.append(dict(row))
    return headers, rows_preview, total_rows


def list_directory_tool(path: str = ".", recursive: bool = False, base_path: str = "") -> dict[str, Any]:
    target = _resolve_agent_path(path, base_path)
    if not target.exists():
        raise ValueError(f"Directory `{target}` does not exist.")
    if not target.is_dir():
        raise ValueError(f"`{target}` is not a directory.")

    iterator = target.rglob("*") if recursive else target.iterdir()
    entries = []
    for item in iterator:
        entry_type = "dir" if item.is_dir() else "file"
        size = item.stat().st_size if item.is_file() else 0
        entries.append(
            {
                "name": item.name,
                "path": str(item),
                "type": entry_type,
                "size": size,
            }
        )
        if len(entries) >= FILE_TABULAR_PREVIEW_ROWS:
            break

    preview_lines = [
        f"- `{Path(entry['path']).name}{'/' if entry['type'] == 'dir' else ''}` · {entry['type']} · {entry['size']} bytes"
        for entry in entries
    ] or ["No entries found."]
    return _file_tool_result(
        f"Listed `{target}`.",
        preview="\n".join(preview_lines),
        data=entries,
        visited_path=str(target),
    )


def get_file_info_tool(path: str, base_path: str = "") -> dict[str, Any]:
    target = _resolve_agent_path(path, base_path)
    if not target.exists():
        raise ValueError(f"Path `{target}` does not exist.")

    stats = target.stat()
    info = {
        "path": str(target),
        "name": target.name,
        "isDirectory": target.is_dir(),
        "size": stats.st_size,
        "modifiedAt": datetime.fromtimestamp(stats.st_mtime, tz=timezone.utc).isoformat(),
        "createdAt": datetime.fromtimestamp(stats.st_ctime, tz=timezone.utc).isoformat(),
        "suffix": target.suffix.lower(),
    }
    preview = "\n".join(f"- **{key}**: {value}" for key, value in info.items())
    return _file_tool_result(
        f"Loaded metadata for `{target.name}`.",
        preview=preview,
        data=info,
        visited_path=str(target if target.is_dir() else target.parent),
    )


def search_files_tool(path: str = ".", query: str = "", recursive: bool = True, base_path: str = "") -> dict[str, Any]:
    if not query.strip():
        raise ValueError("A search query is required.")

    root = _resolve_agent_path(path, base_path)
    if not root.exists() or not root.is_dir():
        raise ValueError(f"Directory `{root}` does not exist.")

    matches = []
    lowered_query = query.lower().strip()
    iterator = root.rglob("*") if recursive else root.iterdir()
    for item in iterator:
        if len(matches) >= FILE_SEARCH_RESULTS_LIMIT:
            break

        relative_path = str(item.relative_to(root))
        match_kind = None
        if lowered_query in relative_path.lower():
            match_kind = "name"
        elif item.is_file() and item.suffix.lower() in FILE_MANAGER_TEXT_EXTENSIONS and item.stat().st_size <= 1_000_000:
            content = _read_text_file(item)
            if lowered_query in content.lower():
                match_kind = "content"

        if match_kind:
            matches.append(
                {
                    "path": str(item),
                    "relativePath": relative_path,
                    "type": "dir" if item.is_dir() else "file",
                    "matchKind": match_kind,
                }
            )

    preview = "\n".join(
        f"- `{item['relativePath']}` · {item['type']} · matched by {item['matchKind']}"
        for item in matches
    ) or "No match found."
    return _file_tool_result(
        f"Found {len(matches)} matching item(s) in `{root}`.",
        preview=preview,
        data=matches,
        visited_path=str(root),
    )


def read_file_tool(path: str, base_path: str = "") -> dict[str, Any]:
    target = _resolve_agent_path(path, base_path)
    if not target.exists() or not target.is_file():
        raise ValueError(f"File `{target}` does not exist.")

    suffix = target.suffix.lower()
    if suffix in FILE_MANAGER_TEXT_EXTENSIONS:
        content = _read_text_file(target)
        return _file_tool_result(
            f"Read text file `{target.name}`.",
            preview=f"```text\n{_truncate_text_preview(content)}\n```",
            data={"path": str(target), "content": _truncate_text_preview(content)},
            visited_path=str(target.parent),
        )

    if suffix in {".csv", ".tsv"}:
        headers, rows, total_rows = _read_csv_rows(target)
        preview = (
            f"Rows shown: {len(rows)} / {total_rows}\n\n"
            f"{_format_table_rows(rows)}"
        )
        return _file_tool_result(
            f"Read tabular file `{target.name}`.",
            preview=preview,
            data={"headers": headers, "rows": rows, "totalRows": total_rows},
            visited_path=str(target.parent),
        )

    if suffix in {".xlsx", ".xls"}:
        sheet_name, rows, sheet_list = _load_excel_sheet_preview(target)
        preview = (
            f"Sheet: `{sheet_name}`\n"
            f"Available sheets: {sheet_list}\n\n"
            f"{_format_table_rows(rows)}"
        )
        return _file_tool_result(
            f"Read Excel file `{target.name}`.",
            preview=preview,
            data={"sheet": sheet_name, "rows": rows, "sheetNames": sheet_list.split(", ") if sheet_list else []},
            visited_path=str(target.parent),
        )

    if suffix == ".docx":
        Document = _import_docx_document()
        document = Document(target)
        content = "\n".join(paragraph.text for paragraph in document.paragraphs if paragraph.text.strip())
        return _file_tool_result(
            f"Read Word document `{target.name}`.",
            preview=f"```text\n{_truncate_text_preview(content)}\n```",
            data={"path": str(target), "content": _truncate_text_preview(content)},
            visited_path=str(target.parent),
        )

    if suffix == ".parquet":
        pq = _import_pyarrow_parquet()
        parquet_file = pq.ParquetFile(target)
        batch = next(parquet_file.iter_batches(batch_size=FILE_TABULAR_PREVIEW_ROWS), None)
        rows = batch.to_pylist() if batch is not None else []
        preview = (
            f"Rows shown: {len(rows)} / {parquet_file.metadata.num_rows}\n\n"
            f"{_format_table_rows(rows)}"
        )
        return _file_tool_result(
            f"Read Parquet file `{target.name}`.",
            preview=preview,
            data={"rows": rows, "totalRows": parquet_file.metadata.num_rows},
            visited_path=str(target.parent),
        )

    raise ValueError(f"Unsupported file format `{suffix or 'unknown'}` for read_file.")


def read_csv_summary_tool(path: str, base_path: str = "") -> dict[str, Any]:
    target = _resolve_agent_path(path, base_path)
    if not target.exists() or not target.is_file():
        raise ValueError(f"File `{target}` does not exist.")
    if target.suffix.lower() not in {".csv", ".tsv"}:
        raise ValueError("read_csv_summary only supports `.csv` and `.tsv` files.")

    headers, rows, total_rows = _read_csv_rows(target)
    preview = (
        f"Columns: {', '.join(headers) if headers else 'No header'}\n"
        f"Rows shown: {len(rows)} / {total_rows}\n\n"
        f"{_format_table_rows(rows)}"
    )
    return _file_tool_result(
        f"Loaded a CSV summary for `{target.name}`.",
        preview=preview,
        data={"headers": headers, "rows": rows, "totalRows": total_rows},
        visited_path=str(target.parent),
    )


def create_directory_tool(path: str, base_path: str = "") -> dict[str, Any]:
    target = _resolve_agent_path(path, base_path)
    target.mkdir(parents=True, exist_ok=True)
    return _file_tool_result(
        f"Created directory `{target}`.",
        preview=f"Directory ready at `{target}`.",
        visited_path=str(target),
    )


def create_file_tool(path: str, content: str = "", base_path: str = "") -> dict[str, Any]:
    target = _resolve_agent_path(path, base_path)
    if target.exists():
        raise ValueError(f"File `{target}` already exists.")
    if target.suffix.lower() not in FILE_MANAGER_TEXT_EXTENSIONS:
        raise ValueError("create_file only supports text-based file formats.")
    _ensure_parent_directory(target)
    target.write_text(str(content), encoding="utf-8")
    return _file_tool_result(
        f"Created file `{target.name}`.",
        preview=f"```text\n{_truncate_text_preview(content)}\n```",
        visited_path=str(target.parent),
    )


def create_excel_file_tool(
    path: str,
    sheet_name: str = "Sheet1",
    headers: Optional[list[Any]] = None,
    rows: Optional[list[list[Any]]] = None,
    base_path: str = "",
) -> dict[str, Any]:
    target = _resolve_agent_path(path, base_path)
    if target.exists():
        raise ValueError(f"File `{target}` already exists.")
    if target.suffix.lower() != ".xlsx":
        raise ValueError("create_excel_file currently supports `.xlsx` files only.")
    Workbook, _ = _import_openpyxl()
    _ensure_parent_directory(target)
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = sheet_name or "Sheet1"
    if headers:
        worksheet.append(list(headers))
    for row in rows or []:
        worksheet.append(list(row))
    workbook.save(target)
    return _file_tool_result(
        f"Created Excel file `{target.name}` with sheet `{worksheet.title}`.",
        preview=f"Workbook saved to `{target}`.",
        visited_path=str(target.parent),
    )


def write_file_tool(path: str, content: str, confirmed: bool = False, base_path: str = "") -> dict[str, Any]:
    target = _resolve_agent_path(path, base_path)
    if target.suffix.lower() not in FILE_MANAGER_TEXT_EXTENSIONS:
        raise ValueError("write_file only supports text-based file formats.")
    existing_content = _read_text_file(target) if target.exists() else ""
    preview = (
        f"Target: `{target}`\n"
        f"Existing size: {len(existing_content)} characters\n"
        f"New size: {len(str(content))} characters\n\n"
        f"## New content preview\n```text\n{_truncate_text_preview(str(content))}\n```"
    )
    if not confirmed:
        return _file_tool_result(
            f"Writing `{target.name}` will overwrite the current content.",
            preview=preview,
            visited_path=str(target.parent),
            requires_confirmation=True,
            pending_action={
                "tool_name": "write_file",
                "tool_input": {"path": path, "content": str(content)},
            },
        )
    _ensure_parent_directory(target)
    target.write_text(str(content), encoding="utf-8")
    return _file_tool_result(
        f"Wrote `{target.name}` successfully.",
        preview=f"```text\n{_truncate_text_preview(str(content))}\n```",
        visited_path=str(target.parent),
    )


def _load_excel_for_write(path: Path):
    _, load_workbook = _import_openpyxl()
    if not path.exists():
        raise ValueError(f"Excel file `{path}` does not exist.")
    if path.suffix.lower() != ".xlsx":
        raise ValueError("Excel write operations currently support `.xlsx` files only.")
    return load_workbook(path)


def list_excel_sheets_tool(path: str, base_path: str = "") -> dict[str, Any]:
    target = _resolve_agent_path(path, base_path)
    if not target.exists() or not target.is_file():
        raise ValueError(f"Excel file `{target}` does not exist.")
    _, _, sheet_list = _load_excel_sheet_preview(target)
    preview = "\n".join(f"- `{sheet}`" for sheet in sheet_list.split(", ") if sheet) or "No sheet found."
    return _file_tool_result(
        f"Listed sheets for `{target.name}`.",
        preview=preview,
        data={"sheets": [sheet for sheet in sheet_list.split(", ") if sheet]},
        visited_path=str(target.parent),
    )


def read_excel_sheet_tool(path: str, sheet_name: Optional[str] = None, base_path: str = "") -> dict[str, Any]:
    target = _resolve_agent_path(path, base_path)
    if not target.exists() or not target.is_file():
        raise ValueError(f"Excel file `{target}` does not exist.")
    resolved_sheet_name, rows, sheet_list = _load_excel_sheet_preview(target, sheet_name)
    preview = (
        f"Sheet: `{resolved_sheet_name}`\n"
        f"Available sheets: {sheet_list}\n\n"
        f"{_format_table_rows(rows)}"
    )
    return _file_tool_result(
        f"Read sheet `{resolved_sheet_name}` from `{target.name}`.",
        preview=preview,
        data={"sheet": resolved_sheet_name, "rows": rows},
        visited_path=str(target.parent),
    )


def add_excel_sheet_tool(path: str, sheet_name: str, base_path: str = "") -> dict[str, Any]:
    target = _resolve_agent_path(path, base_path)
    workbook = _load_excel_for_write(target)
    if sheet_name in workbook.sheetnames:
        raise ValueError(f"Sheet `{sheet_name}` already exists.")
    workbook.create_sheet(title=sheet_name)
    workbook.save(target)
    return _file_tool_result(
        f"Added sheet `{sheet_name}` to `{target.name}`.",
        preview="\n".join(f"- `{sheet}`" for sheet in workbook.sheetnames),
        visited_path=str(target.parent),
    )


def rename_excel_sheet_tool(path: str, old_name: str, new_name: str, base_path: str = "") -> dict[str, Any]:
    target = _resolve_agent_path(path, base_path)
    workbook = _load_excel_for_write(target)
    if old_name not in workbook.sheetnames:
        raise ValueError(f"Sheet `{old_name}` does not exist.")
    if new_name in workbook.sheetnames:
        raise ValueError(f"Sheet `{new_name}` already exists.")
    workbook[old_name].title = new_name
    workbook.save(target)
    return _file_tool_result(
        f"Renamed sheet `{old_name}` to `{new_name}` in `{target.name}`.",
        preview="\n".join(f"- `{sheet}`" for sheet in workbook.sheetnames),
        visited_path=str(target.parent),
    )


def write_excel_sheet_tool(
    path: str,
    sheet_name: str,
    headers: Optional[list[Any]] = None,
    rows: Optional[list[list[Any]]] = None,
    confirmed: bool = False,
    base_path: str = "",
) -> dict[str, Any]:
    target = _resolve_agent_path(path, base_path)
    preview_rows = _normalize_excel_rows(list(headers or []), [list(row) for row in (rows or [])[:10]]) if headers else []
    preview = (
        f"Target workbook: `{target.name}`\n"
        f"Sheet: `{sheet_name}`\n"
        f"Rows to write: {len(rows or [])}\n\n"
        f"{_format_table_rows(preview_rows) if preview_rows else 'The sheet will be cleared and rewritten.'}"
    )
    if not confirmed:
        return _file_tool_result(
            f"Writing sheet `{sheet_name}` will replace its current content.",
            preview=preview,
            visited_path=str(target.parent),
            requires_confirmation=True,
            pending_action={
                "tool_name": "write_excel_sheet",
                "tool_input": {
                    "path": path,
                    "sheet_name": sheet_name,
                    "headers": headers or [],
                    "rows": rows or [],
                },
            },
        )

    workbook = _load_excel_for_write(target)
    worksheet = workbook[sheet_name] if sheet_name in workbook.sheetnames else workbook.create_sheet(sheet_name)
    worksheet.delete_rows(1, worksheet.max_row or 1)
    if headers:
        worksheet.append(list(headers))
    for row in rows or []:
        worksheet.append(list(row))
    workbook.save(target)
    return _file_tool_result(
        f"Wrote sheet `{sheet_name}` in `{target.name}`.",
        preview=preview,
        visited_path=str(target.parent),
    )


def edit_excel_cells_tool(
    path: str,
    sheet_name: str,
    updates: list[dict[str, Any]],
    confirmed: bool = False,
    base_path: str = "",
) -> dict[str, Any]:
    if not updates:
        raise ValueError("At least one cell update is required.")
    target = _resolve_agent_path(path, base_path)
    preview = "\n".join(
        f"- `{item.get('cell', '?')}` → `{item.get('value', '')}`"
        for item in updates[:20]
    )
    if not confirmed:
        return _file_tool_result(
            f"Editing cells in `{sheet_name}` will modify `{target.name}`.",
            preview=preview,
            visited_path=str(target.parent),
            requires_confirmation=True,
            pending_action={
                "tool_name": "edit_excel_cells",
                "tool_input": {
                    "path": path,
                    "sheet_name": sheet_name,
                    "updates": updates,
                },
            },
        )

    workbook = _load_excel_for_write(target)
    if sheet_name not in workbook.sheetnames:
        raise ValueError(f"Sheet `{sheet_name}` does not exist.")
    worksheet = workbook[sheet_name]
    for item in updates:
        cell = str(item.get("cell") or "").strip().upper()
        if not re.fullmatch(r"[A-Z]+[1-9][0-9]*", cell):
            raise ValueError(f"Invalid cell reference `{cell}`.")
        worksheet[cell] = item.get("value")
    workbook.save(target)
    return _file_tool_result(
        f"Updated {len(updates)} cell(s) in `{sheet_name}`.",
        preview=preview,
        visited_path=str(target.parent),
    )


def append_excel_rows_tool(path: str, sheet_name: str, rows: list[list[Any]], base_path: str = "") -> dict[str, Any]:
    if not rows:
        raise ValueError("At least one row is required.")
    target = _resolve_agent_path(path, base_path)
    workbook = _load_excel_for_write(target)
    if sheet_name not in workbook.sheetnames:
        raise ValueError(f"Sheet `{sheet_name}` does not exist.")
    worksheet = workbook[sheet_name]
    for row in rows:
        worksheet.append(list(row))
    workbook.save(target)
    return _file_tool_result(
        f"Appended {len(rows)} row(s) to `{sheet_name}`.",
        preview=f"Sheet `{sheet_name}` now has {worksheet.max_row} row(s).",
        visited_path=str(target.parent),
    )


def delete_excel_sheet_tool(path: str, sheet_name: str, confirmed: bool = False, base_path: str = "") -> dict[str, Any]:
    target = _resolve_agent_path(path, base_path)
    if not confirmed:
        return _file_tool_result(
            f"Deleting sheet `{sheet_name}` will modify `{target.name}`.",
            preview=f"Workbook: `{target}`\nSheet to delete: `{sheet_name}`",
            visited_path=str(target.parent),
            requires_confirmation=True,
            pending_action={
                "tool_name": "delete_excel_sheet",
                "tool_input": {"path": path, "sheet_name": sheet_name},
            },
        )

    workbook = _load_excel_for_write(target)
    if sheet_name not in workbook.sheetnames:
        raise ValueError(f"Sheet `{sheet_name}` does not exist.")
    if len(workbook.sheetnames) == 1:
        raise ValueError("An Excel workbook must keep at least one sheet.")
    worksheet = workbook[sheet_name]
    workbook.remove(worksheet)
    workbook.save(target)
    return _file_tool_result(
        f"Deleted sheet `{sheet_name}` from `{target.name}`.",
        preview="\n".join(f"- `{sheet}`" for sheet in workbook.sheetnames),
        visited_path=str(target.parent),
    )


def delete_file_tool(path: str, confirmed: bool = False, base_path: str = "") -> dict[str, Any]:
    target = _resolve_agent_path(path, base_path)
    if not target.exists() or not target.is_file():
        raise ValueError(f"File `{target}` does not exist.")
    preview = f"File: `{target}`\nSize: {target.stat().st_size} bytes"
    if not confirmed:
        return _file_tool_result(
            f"Deleting `{target.name}` is destructive.",
            preview=preview,
            visited_path=str(target.parent),
            requires_confirmation=True,
            pending_action={"tool_name": "delete_file", "tool_input": {"path": path}},
        )
    target.unlink()
    return _file_tool_result(
        f"Deleted file `{target.name}`.",
        preview=preview,
        visited_path=str(target.parent),
    )


def delete_directory_tool(path: str, recursive: bool = False, confirmed: bool = False, base_path: str = "") -> dict[str, Any]:
    target = _resolve_agent_path(path, base_path)
    sandbox_root = Path(base_path).expanduser().resolve() if base_path else None
    if sandbox_root and target == sandbox_root:
        raise ValueError("Deleting the configured base_path root is blocked for safety.")
    if not target.exists() or not target.is_dir():
        raise ValueError(f"Directory `{target}` does not exist.")
    child_count = sum(1 for _ in target.iterdir())
    preview = f"Directory: `{target}`\nEntries inside: {child_count}\nRecursive: {recursive}"
    if not confirmed:
        return _file_tool_result(
            f"Deleting directory `{target.name}` is destructive.",
            preview=preview,
            visited_path=str(target.parent),
            requires_confirmation=True,
            pending_action={
                "tool_name": "delete_directory",
                "tool_input": {"path": path, "recursive": recursive},
            },
        )
    if recursive:
        shutil.rmtree(target)
    else:
        target.rmdir()
    return _file_tool_result(
        f"Deleted directory `{target.name}`.",
        preview=preview,
        visited_path=str(target.parent),
    )


def move_file_tool(source_path: str, destination_path: str, confirmed: bool = False, base_path: str = "") -> dict[str, Any]:
    source = _resolve_agent_path(source_path, base_path)
    destination = _resolve_agent_path(destination_path, base_path)
    if not source.exists() or not source.is_file():
        raise ValueError(f"Source file `{source}` does not exist.")
    preview = (
        f"Source: `{source}`\n"
        f"Destination: `{destination}`\n"
        f"Destination exists: {destination.exists()}"
    )
    if not confirmed:
        return _file_tool_result(
            f"Moving `{source.name}` will change the filesystem state.",
            preview=preview,
            visited_path=str(source.parent),
            requires_confirmation=True,
            pending_action={
                "tool_name": "move_file",
                "tool_input": {"source_path": source_path, "destination_path": destination_path},
            },
        )
    _ensure_parent_directory(destination)
    if destination.exists():
        if destination.is_dir():
            raise ValueError("Destination already exists and is a directory.")
        destination.unlink()
    shutil.move(str(source), str(destination))
    return _file_tool_result(
        f"Moved `{source.name}` to `{destination}`.",
        preview=preview,
        visited_path=str(destination.parent),
    )


FILE_MANAGER_TOOLS: dict[str, dict[str, Any]] = {
    "list_directory": {
        "description": "List files and folders in a directory.",
        "parameters": {"path": "string, default '.'", "recursive": "boolean, default false"},
        "handler": list_directory_tool,
    },
    "get_file_info": {
        "description": "Get metadata for a file or directory.",
        "parameters": {"path": "string"},
        "handler": get_file_info_tool,
    },
    "search_files": {
        "description": "Search files by name and, for text files, by content.",
        "parameters": {"path": "string, default '.'", "query": "string", "recursive": "boolean, default true"},
        "handler": search_files_tool,
    },
    "read_file": {
        "description": "Read text, Word, Parquet, CSV, TSV, XLSX, or XLS content.",
        "parameters": {"path": "string"},
        "handler": read_file_tool,
    },
    "read_csv_summary": {
        "description": "Read a CSV or TSV file and summarize up to 50 rows.",
        "parameters": {"path": "string"},
        "handler": read_csv_summary_tool,
    },
    "create_directory": {
        "description": "Create a directory.",
        "parameters": {"path": "string"},
        "handler": create_directory_tool,
    },
    "create_file": {
        "description": "Create a new text file.",
        "parameters": {"path": "string", "content": "string, optional"},
        "handler": create_file_tool,
    },
    "create_excel_file": {
        "description": "Create a new XLSX workbook.",
        "parameters": {"path": "string ending with .xlsx", "sheet_name": "string, optional", "headers": "array, optional", "rows": "2D array, optional"},
        "handler": create_excel_file_tool,
    },
    "write_file": {
        "description": "Overwrite a text file. Requires confirmation first.",
        "parameters": {"path": "string", "content": "string", "confirmed": "boolean, default false"},
        "handler": write_file_tool,
    },
    "write_excel_sheet": {
        "description": "Overwrite an XLSX sheet with new rows. Requires confirmation first.",
        "parameters": {"path": "string", "sheet_name": "string", "headers": "array, optional", "rows": "2D array, optional", "confirmed": "boolean, default false"},
        "handler": write_excel_sheet_tool,
    },
    "edit_excel_cells": {
        "description": "Edit specific XLSX cells. Requires confirmation first.",
        "parameters": {"path": "string", "sheet_name": "string", "updates": "array of {cell, value}", "confirmed": "boolean, default false"},
        "handler": edit_excel_cells_tool,
    },
    "append_excel_rows": {
        "description": "Append rows to an existing XLSX sheet.",
        "parameters": {"path": "string", "sheet_name": "string", "rows": "2D array"},
        "handler": append_excel_rows_tool,
    },
    "delete_file": {
        "description": "Delete a file. Requires confirmation first.",
        "parameters": {"path": "string", "confirmed": "boolean, default false"},
        "handler": delete_file_tool,
    },
    "delete_directory": {
        "description": "Delete a directory. Requires confirmation first.",
        "parameters": {"path": "string", "recursive": "boolean, default false", "confirmed": "boolean, default false"},
        "handler": delete_directory_tool,
    },
    "delete_excel_sheet": {
        "description": "Delete an XLSX sheet. Requires confirmation first.",
        "parameters": {"path": "string", "sheet_name": "string", "confirmed": "boolean, default false"},
        "handler": delete_excel_sheet_tool,
    },
    "list_excel_sheets": {
        "description": "List sheets inside an Excel file.",
        "parameters": {"path": "string"},
        "handler": list_excel_sheets_tool,
    },
    "read_excel_sheet": {
        "description": "Read up to 50 rows from an Excel sheet.",
        "parameters": {"path": "string", "sheet_name": "string, optional"},
        "handler": read_excel_sheet_tool,
    },
    "add_excel_sheet": {
        "description": "Add a new sheet to an XLSX workbook.",
        "parameters": {"path": "string", "sheet_name": "string"},
        "handler": add_excel_sheet_tool,
    },
    "rename_excel_sheet": {
        "description": "Rename a sheet inside an XLSX workbook.",
        "parameters": {"path": "string", "old_name": "string", "new_name": "string"},
        "handler": rename_excel_sheet_tool,
    },
    "move_file": {
        "description": "Move a file to another location. Requires confirmation first.",
        "parameters": {"source_path": "string", "destination_path": "string", "confirmed": "boolean, default false"},
        "handler": move_file_tool,
    },
}


def _file_manager_tool_manifest() -> str:
    lines = []
    for tool_name, tool_spec in FILE_MANAGER_TOOLS.items():
        lines.append(f"- {tool_name}: {tool_spec['description']}")
        lines.append("  Parameters:")
        for key, value in tool_spec["parameters"].items():
            lines.append(f"    - {key}: {value}")
    return "\n".join(lines)


def execute_file_manager_tool(tool_name: str, tool_input: dict[str, Any], base_path: str = "") -> dict[str, Any]:
    if tool_name not in FILE_MANAGER_TOOLS:
        raise ValueError(f"Unknown file-management tool `{tool_name}`.")
    handler = FILE_MANAGER_TOOLS[tool_name]["handler"]
    safe_input = dict(tool_input or {})
    safe_input["base_path"] = base_path
    return handler(**safe_input)


def _try_extract_file_export_payload(user_message: str) -> Optional[dict[str, Any]]:
    text = str(user_message or "").strip()
    if not text:
        return None
    try:
        parsed = extract_json_object(text)
    except Exception:
        return None
    if not isinstance(parsed, dict) or not parsed.get("__file_export__"):
        return None

    export_format = str(parsed.get("format") or "").strip().lower()
    if export_format == "xls":
        export_format = "xlsx"
    if export_format not in {"csv", "tsv", "xlsx"}:
        raise ValueError("Unsupported file export format. Use `csv`, `tsv`, or `xlsx`.")

    path = str(parsed.get("path") or "").strip()
    if export_format == "xlsx" and path.lower().endswith(".xls"):
        path = path[:-4] + ".xlsx"
    headers = [str(item) for item in (parsed.get("headers") or []) if str(item).strip()]
    raw_rows = parsed.get("rows") or []
    if not isinstance(raw_rows, list):
        raise ValueError("The file export payload must provide `rows` as a list.")
    rows = []
    for row in raw_rows:
        if isinstance(row, list):
            rows.append(list(row))
        elif isinstance(row, dict) and headers:
            rows.append([row.get(header) for header in headers])
    return {
        "format": export_format,
        "path": path,
        "sheet_name": str(parsed.get("sheet_name") or parsed.get("sheetName") or "Results").strip() or "Results",
        "headers": headers,
        "rows": rows,
        "source_sql": str(parsed.get("source_sql") or parsed.get("sourceSql") or "").strip(),
        "source_request": str(parsed.get("source_request") or parsed.get("sourceRequest") or "").strip(),
    }


def _serialize_delimited_rows(headers: list[str], rows: list[list[Any]], delimiter: str) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=delimiter)
    if headers:
        writer.writerow(headers)
    for row in rows:
        writer.writerow([
            json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value
            for value in row
        ])
    return buffer.getvalue()


def _file_export_answer(result: dict[str, Any], payload: dict[str, Any]) -> str:
    preview = result.get("preview") or ""
    answer = (
        "## File Export\n"
        f"{result['summary']}\n\n"
        f"- **Format:** `{payload.get('format')}`\n"
        f"- **Target:** `{payload.get('path')}`"
    )
    if payload.get("source_sql"):
        answer += "\n- **Source SQL available:** yes"
    if preview:
        answer += f"\n\n## Preview\n{preview}"
    return answer


async def plan_file_manager_step(
    user_message: str,
    history: list[dict[str, Any]],
    scratchpad: list[dict[str, Any]],
    base_path: str,
    system_prompt: str,
    llm_base_url: str,
    llm_model: str,
    llm_provider: str,
    llm_api_key: Optional[str] = None,
) -> dict[str, Any]:
    trimmed_history = _normalized_history_messages(
        history,
        current_message=user_message,
        max_steps=CHAT_MEMORY_MAX_STEPS,
    )
    prompt = f"""
You are a ReAct-style file management agent.
Reply in English.
You must either:
1. return a tool action, or
2. return a final answer.

Return JSON only with this exact shape:
{{
  "reasoning": "short internal reasoning",
  "action": "tool" | "final",
  "tool_name": "list_directory",
  "tool_input": {{"path": "."}},
  "final_answer": "final answer when action is final"
}}

Rules:
- Never invent filesystem state. Use tools whenever the answer depends on the actual files.
- Use only one tool at a time.
- For overwrite, delete, and move actions, call the tool with confirmed=false first.
- Prefer short answers.
- If the task is already complete, return action="final".
- The configured sandbox base_path is: {base_path or "not set"}.
- If you need to inspect files before acting, do that first.

System prompt:
{system_prompt}

Available tools:
{_file_manager_tool_manifest()}

Recent conversation:
{json.dumps(trimmed_history, ensure_ascii=False, indent=2)}

Scratchpad from this request:
{json.dumps(scratchpad, ensure_ascii=False, indent=2)}

Current user request:
{user_message}
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
    action = str(parsed.get("action", "") or "").strip().lower()
    return {
        "reasoning": str(parsed.get("reasoning", "") or "").strip(),
        "action": action,
        "tool_name": str(parsed.get("tool_name", "") or "").strip(),
        "tool_input": parsed.get("tool_input") if isinstance(parsed.get("tool_input"), dict) else {},
        "final_answer": str(parsed.get("final_answer", "") or "").strip(),
    }


def _default_file_manager_state() -> dict[str, Any]:
    return {
        "pending_confirmation": None,
        "last_tool_result": "",
        "last_visited_path": "",
    }


def _normalize_file_manager_state(payload: Optional[dict]) -> dict[str, Any]:
    state = _default_file_manager_state()
    if not isinstance(payload, dict):
        return state

    pending = payload.get("pending_confirmation") or payload.get("pendingConfirmation")
    if isinstance(pending, dict):
        state["pending_confirmation"] = {
            "tool_name": str(pending.get("tool_name") or pending.get("toolName") or "").strip(),
            "tool_input": pending.get("tool_input") if isinstance(pending.get("tool_input"), dict) else pending.get("toolInput") if isinstance(pending.get("toolInput"), dict) else {},
            "preview": str(pending.get("preview") or "").strip(),
            "summary": str(pending.get("summary") or "").strip(),
            "requested_at": str(pending.get("requested_at") or pending.get("requestedAt") or "").strip(),
        }

    state["last_tool_result"] = str(payload.get("last_tool_result") or payload.get("lastToolResult") or "").strip()
    state["last_visited_path"] = str(payload.get("last_visited_path") or payload.get("lastVisitedPath") or "").strip()
    return state


def _normalize_file_manager_config(payload: Optional[dict]) -> dict[str, Any]:
    defaults = DEFAULT_APP_CONFIG["fileManagerConfig"]
    if not isinstance(payload, dict):
        return dict(defaults)
    max_iterations = payload.get("maxIterations") if "maxIterations" in payload else payload.get("max_iterations")
    return {
        "basePath": str(payload.get("basePath") or payload.get("base_path") or defaults["basePath"]).strip(),
        "maxIterations": max(1, min(FILE_MANAGER_MAX_ITERATIONS, int(max_iterations or defaults["maxIterations"]))),
        "systemPrompt": str(payload.get("systemPrompt") or payload.get("system_prompt") or defaults["systemPrompt"]).strip() or defaults["systemPrompt"],
    }


def _file_manager_confirmation_answer(state: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    pending = state.get("pending_confirmation") or {}
    preview = str(pending.get("preview") or "").strip()
    summary = str(pending.get("summary") or "This action requires confirmation.").strip()
    answer = (
        "## Confirmation Needed\n"
        f"{summary}\n\n"
        f"{preview}\n\n"
        "Please confirm if you want me to continue."
    )
    actions = [
        {
            "id": "confirm-file-action",
            "label": "Confirm",
            "actionType": "confirm_file_action",
            "variant": "primary",
        },
        {
            "id": "cancel-file-action",
            "label": "Cancel",
            "actionType": "cancel_file_action",
            "variant": "secondary",
        },
    ]
    return answer, actions


MANAGER_SPECIALIST_LABELS = {
    "clickhouse_query": "ClickHouse Query",
    "file_management": "File management",
    "data_quality_tables": "Data quality - Tables",
}
MANAGER_CLICKHOUSE_FOLLOWUP_STAGES = {
    "awaiting_table",
    "awaiting_field",
    "awaiting_date",
    "awaiting_chart_offer",
    "awaiting_chart_x",
    "awaiting_chart_y",
    "awaiting_chart_type",
}


def _default_manager_agent_state() -> dict[str, Any]:
    return {
        "active_delegate": None,
        "last_routing_reason": "",
        "last_delegate_label": "",
        "pending_pipeline": None,
    }


def _normalize_manager_delegate_role(value: Any, allow_manager: bool = False) -> Optional[str]:
    if not isinstance(value, str):
        return None
    lowered = normalize_choice(value).lower()
    aliases = {
        "manager": "manager",
        "agent manager": "manager",
        "direct": "manager",
        "direct answer": "manager",
        "none": "manager",
        "clickhouse query": "clickhouse_query",
        "clickhouse_query": "clickhouse_query",
        "clickhouse": "clickhouse_query",
        "sql": "clickhouse_query",
        "file management": "file_management",
        "file manager": "file_management",
        "file_management": "file_management",
        "filesystem": "file_management",
        "files": "file_management",
        "data quality": "data_quality_tables",
        "data quality tables": "data_quality_tables",
        "data_quality_tables": "data_quality_tables",
        "data-quality": "data_quality_tables",
        "profiling": "data_quality_tables",
        "quality profiling": "data_quality_tables",
    }
    resolved = aliases.get(lowered)
    if resolved == "manager" and not allow_manager:
        return None
    return resolved


def _normalize_manager_pending_pipeline(payload: Any) -> Optional[dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    kind = str(payload.get("kind") or "").strip()
    stage = str(payload.get("stage") or "").strip()
    next_delegate = _normalize_manager_delegate_role(
        payload.get("next_delegate") or payload.get("nextDelegate"),
        allow_manager=False,
    )
    export_format = str(payload.get("export_format") or payload.get("exportFormat") or "").strip().lower() or None
    if kind != "clickhouse_to_file" or next_delegate != "file_management":
        return None
    if stage not in {"awaiting_clickhouse", "awaiting_export_details"}:
        stage = "awaiting_clickhouse"
    if export_format not in {"csv", "tsv", "xlsx", None}:
        export_format = None
    return {
        "kind": "clickhouse_to_file",
        "stage": stage,
        "next_delegate": "file_management",
        "export_format": export_format,
        "target_path": str(payload.get("target_path") or payload.get("targetPath") or "").strip(),
        "source_request": str(payload.get("source_request") or payload.get("sourceRequest") or "").strip(),
        "reason": str(payload.get("reason") or "").strip(),
    }


def _normalize_manager_agent_state(payload: Optional[dict]) -> dict[str, Any]:
    state = _default_manager_agent_state()
    if not isinstance(payload, dict):
        return state
    state["active_delegate"] = _normalize_manager_delegate_role(
        payload.get("active_delegate") or payload.get("activeDelegate"),
        allow_manager=False,
    )
    state["last_routing_reason"] = str(
        payload.get("last_routing_reason") or payload.get("lastRoutingReason") or ""
    ).strip()
    state["last_delegate_label"] = str(
        payload.get("last_delegate_label") or payload.get("lastDelegateLabel") or ""
    ).strip()
    state["pending_pipeline"] = _normalize_manager_pending_pipeline(
        payload.get("pending_pipeline") or payload.get("pendingPipeline")
    )
    return state


def _clickhouse_state_needs_followup(state: dict[str, Any]) -> bool:
    return str(state.get("stage") or "").strip() in MANAGER_CLICKHOUSE_FOLLOWUP_STAGES


def _file_manager_state_needs_followup(state: dict[str, Any]) -> bool:
    return isinstance(state.get("pending_confirmation"), dict)


def _manager_specialist_label(role: Optional[str]) -> str:
    if not role:
        return "Manager"
    return MANAGER_SPECIALIST_LABELS.get(role, role.replace("_", " ").title())


def _manager_trimmed_history(history: list[dict[str, Any]], limit: int = 10) -> list[dict[str, str]]:
    return _normalized_history_messages(history, max_steps=limit)


def _manager_specialist_state_summary(
    clickhouse_state: dict[str, Any],
    file_manager_state: dict[str, Any],
    data_quality_state: dict[str, Any],
    manager_state: Optional[dict[str, Any]] = None,
) -> str:
    clickhouse_summary = {
        "stage": clickhouse_state.get("stage") or "idle",
        "selected_table": clickhouse_state.get("selected_table"),
        "has_last_sql": bool(clickhouse_state.get("last_sql")),
        "has_last_rows": bool(clickhouse_state.get("last_result_rows")),
    }
    file_summary = {
        "pending_confirmation": bool(file_manager_state.get("pending_confirmation")),
        "last_visited_path": file_manager_state.get("last_visited_path") or "",
        "last_tool_result": _truncate_text_preview(
            str(file_manager_state.get("last_tool_result") or ""), 240
        ),
    }
    data_quality_summary = {
        "stage": data_quality_state.get("stage") or "idle",
        "table": data_quality_state.get("table"),
        "selected_columns": len(data_quality_state.get("columns") or []),
        "has_final_answer": bool(data_quality_state.get("final_answer")),
        "time_column": data_quality_state.get("time_column"),
    }
    return json.dumps(
        {
            "manager": {
                "pending_pipeline": (manager_state or {}).get("pending_pipeline"),
            },
            "clickhouse": clickhouse_summary,
            "file_management": file_summary,
            "data_quality_tables": data_quality_summary,
        },
        ensure_ascii=False,
        indent=2,
    )


MANAGER_EXPORT_FORMAT_OPTIONS = ["CSV (.csv)", "Excel (.xlsx)"]
MANAGER_EXPORT_KEYWORDS = [
    "export",
    "save",
    "write",
    "create file",
    "create csv",
    "create excel",
    "generate csv",
    "generate excel",
    "download csv",
    "download excel",
    "sauvegarder",
    "exporter",
    "creer un csv",
    "creer un excel",
    "genere un csv",
    "genere un excel",
]


def _extract_manager_export_format(user_message: str) -> Optional[str]:
    normalized = normalize_intent_text(user_message)
    if ".csv" in normalized or re.search(r"\bcsv\b", normalized):
        return "csv"
    if ".tsv" in normalized or re.search(r"\btsv\b", normalized):
        return "tsv"
    if any(token in normalized for token in [".xlsx", ".xls"]) or re.search(r"\bexcel\b", normalized):
        return "xlsx"
    return None


def _extract_manager_export_path(user_message: str, export_format: Optional[str] = None) -> str:
    lowered = user_message.strip()
    quoted_match = re.search(r'["\']([^"\']+\.(?:csv|tsv|xlsx|xls))["\']', lowered, flags=re.IGNORECASE)
    if quoted_match:
        candidate = quoted_match.group(1).strip()
        if candidate.lower().endswith(".xls"):
            return candidate[:-4] + ".xlsx"
        return candidate

    patterns = [
        r'(?:named|called|as|to|into|in|vers|dans|nomme|nommé|appele|appelé)\s+([A-Za-z0-9_./\\\\-]+\.(?:csv|tsv|xlsx|xls))',
    ]
    extension = f".{export_format}" if export_format in {"csv", "tsv", "xlsx"} else ""
    for pattern in patterns:
        match = re.search(pattern, lowered, flags=re.IGNORECASE)
        if not match:
            continue
        candidate = match.group(1).strip()
        if not candidate:
            continue
        if extension and "." not in Path(candidate).name:
            candidate = f"{candidate}{extension}"
        if re.search(r"\.(csv|tsv|xlsx|xls)$", candidate, flags=re.IGNORECASE):
            if candidate.lower().endswith(".xls"):
                return candidate[:-4] + ".xlsx"
            return candidate

    matches = re.findall(r'([A-Za-z0-9_./\\\\-]+\.(?:csv|tsv|xlsx|xls))', lowered, flags=re.IGNORECASE)
    if matches:
        candidate = matches[-1].strip().strip('"').strip("'")
        if candidate.lower().endswith(".xls"):
            return candidate[:-4] + ".xlsx"
        return candidate
    return ""


def _extract_clickhouse_file_export_pipeline(user_message: str) -> Optional[dict[str, Any]]:
    normalized = normalize_intent_text(user_message)
    has_export_signal = any(token in normalized for token in MANAGER_EXPORT_KEYWORDS) or any(
        token in normalized for token in ["csv", "tsv", "excel", "xlsx", "xls"]
    )
    if not has_export_signal:
        return None

    export_format = _extract_manager_export_format(user_message)
    target_path = _extract_manager_export_path(user_message, export_format)
    return {
        "kind": "clickhouse_to_file",
        "stage": "awaiting_clickhouse",
        "next_delegate": "file_management",
        "export_format": export_format,
        "target_path": target_path,
        "source_request": user_message.strip(),
        "reason": "The user wants the ClickHouse result to be exported as a file after the query runs.",
    }


def _manager_pending_pipeline_requires_details(pipeline: Optional[dict[str, Any]]) -> bool:
    if not isinstance(pipeline, dict):
        return False
    return not pipeline.get("export_format") or not pipeline.get("target_path")


def _manager_export_details_prompt(pipeline: dict[str, Any]) -> str:
    missing_format = not pipeline.get("export_format")
    missing_path = not pipeline.get("target_path")
    if missing_format and missing_path:
        return append_choice_markdown(
            (
                "## File Export Details Needed\n"
                "I already have the ClickHouse result. To create the export file, I still need the output format and the target file name or path."
            ),
            "Format",
            "Choose the file format first.",
            MANAGER_EXPORT_FORMAT_OPTIONS,
        ) + "\n\nThen tell me the target file name or full path, for example `exports/result.csv`."
    if missing_format:
        return append_choice_markdown(
            (
                "## File Export Details Needed\n"
                f"I already have the ClickHouse result and the target path `{pipeline.get('target_path')}`."
            ),
            "Format",
            "Choose the file format for the export.",
            MANAGER_EXPORT_FORMAT_OPTIONS,
        )
    return (
        "## File Export Details Needed\n"
        f"I already have the ClickHouse result and the requested format **{pipeline.get('export_format')}**.\n\n"
        "Please tell me the target file name or full path, for example `exports/result."
        f"{pipeline.get('export_format')}`."
    )


def _manager_update_export_pipeline_from_reply(
    pipeline: dict[str, Any],
    user_message: str,
) -> dict[str, Any]:
    updated = dict(pipeline)
    format_choice = resolve_user_choice(user_message, MANAGER_EXPORT_FORMAT_OPTIONS)
    if format_choice == "CSV (.csv)":
        updated["export_format"] = "csv"
    elif format_choice == "Excel (.xlsx)":
        updated["export_format"] = "xlsx"
    elif not updated.get("export_format"):
        updated["export_format"] = _extract_manager_export_format(user_message)

    detected_path = _extract_manager_export_path(user_message, updated.get("export_format"))
    if detected_path:
        updated["target_path"] = detected_path

    if updated.get("export_format") and updated.get("target_path"):
        updated["stage"] = "awaiting_clickhouse"
    else:
        updated["stage"] = "awaiting_export_details"
    return updated


def _manager_export_headers_and_rows(clickhouse_state: dict[str, Any]) -> tuple[list[str], list[list[Any]]]:
    headers = [
        str(item.get("name") or "").strip()
        for item in (clickhouse_state.get("last_result_meta") or [])
        if str(item.get("name") or "").strip()
    ]
    rows_data = clickhouse_state.get("last_result_rows") or []
    if not headers and rows_data:
        headers = [str(key) for key in rows_data[0].keys()]

    def _cell(value: Any) -> Any:
        if value is None:
            return ""
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return value

    rows = [
        [_cell(row.get(header)) for header in headers]
        for row in rows_data
    ]
    return headers, rows


def _build_file_export_payload_from_clickhouse(
    pipeline: dict[str, Any],
    clickhouse_state: dict[str, Any],
) -> dict[str, Any]:
    headers, rows = _manager_export_headers_and_rows(clickhouse_state)
    export_format = pipeline.get("export_format") or "csv"
    target_path = str(pipeline.get("target_path") or "").strip()
    if export_format == "xlsx" and target_path.lower().endswith(".xls"):
        target_path = target_path[:-4] + ".xlsx"
    return {
        "__file_export__": True,
        "format": export_format,
        "path": target_path,
        "sheet_name": "Results",
        "headers": headers,
        "rows": rows,
        "source_sql": str(clickhouse_state.get("last_sql") or "").strip(),
        "source_request": str(pipeline.get("source_request") or "").strip(),
    }


def _manager_compose_chained_answer(
    primary_answer: str,
    secondary_answer: str,
    secondary_label: str,
) -> str:
    first = str(primary_answer or "").strip()
    second = str(secondary_answer or "").strip()
    if not first:
        return second
    if not second:
        return first
    return (
        f"{first}\n\n---\n\n"
        f"## {secondary_label}\n"
        "I then continued with the next specialist to complete the rest of the same request.\n\n"
        f"{second}"
    )


def _heuristic_manager_delegate(
    user_message: str,
    manager_state: dict[str, Any],
    clickhouse_state: dict[str, Any],
    file_manager_state: dict[str, Any],
    data_quality_state: dict[str, Any],
) -> Optional[tuple[str, str]]:
    normalized = normalize_intent_text(user_message)
    active_delegate = manager_state.get("active_delegate")
    pending_pipeline = manager_state.get("pending_pipeline")

    if active_delegate == "file_management" and _file_manager_state_needs_followup(file_manager_state):
        return "file_management", "Continuing the active file-management confirmation flow."
    if active_delegate == "clickhouse_query" and _clickhouse_state_needs_followup(clickhouse_state):
        return "clickhouse_query", "Continuing the active ClickHouse clarification flow."
    if active_delegate == "data_quality_tables" and _data_quality_state_needs_followup(data_quality_state):
        return "data_quality_tables", "Continuing the active data-quality setup flow."

    if _file_manager_state_needs_followup(file_manager_state) and (
        is_affirmative_response(user_message)
        or is_negative_response(user_message)
        or normalized in {"confirm", "confirm file action", "cancel", "cancel file action"}
    ):
        return "file_management", "The user is answering a pending file-management confirmation."

    if _clickhouse_state_needs_followup(clickhouse_state):
        return "clickhouse_query", "The user is continuing a ClickHouse clarification step."
    if _data_quality_state_needs_followup(data_quality_state):
        return "data_quality_tables", "The user is continuing a data-quality guided step."

    if (
        isinstance(pending_pipeline, dict)
        and pending_pipeline.get("kind") == "clickhouse_to_file"
        and pending_pipeline.get("stage") == "awaiting_export_details"
    ):
        return "manager", "The manager is waiting for the remaining file-export details before handing off to File management."

    if clickhouse_state.get("last_result_rows") and is_chart_followup_request(user_message):
        return "clickhouse_query", "The user is continuing the latest ClickHouse result with a chart follow-up."

    file_tokens = [
        "file",
        "fichier",
        "folder",
        "dossier",
        "directory",
        "repertoire",
        "path",
        "chemin",
        "csv",
        "tsv",
        "txt",
        "md",
        "markdown",
        "json",
        "yaml",
        "yml",
        "sql",
        "py",
        "html",
        "xlsx",
        "xls",
        "excel",
        "docx",
        "parquet",
        "read",
        "open",
        "inspect",
        "save",
        "write",
        "create",
        "edit",
        "update",
        "append",
        "delete",
        "remove",
        "move",
        "rename",
        "list files",
        "search files",
        "lire",
        "ouvrir",
        "inspecter",
        "sauvegarder",
        "ecrire",
        "creer",
        "modifier",
        "mettre a jour",
        "ajouter",
        "supprimer",
        "effacer",
        "deplacer",
        "renommer",
        "lister les fichiers",
        "chercher des fichiers",
    ]
    clickhouse_tokens = [
        "clickhouse",
        "sql",
        "table",
        "column",
        "database",
        "query",
        "chart",
        "graph",
        "rows",
        "count",
        "metrics",
        "measure",
        "aggregation",
        "trend",
        "schema",
        "requete",
        "base de donnees",
        "colonne",
        "graphique",
        "graphe",
        "courbe",
        "lignes",
        "compte",
        "mesures",
        "agregation",
        "tendance",
        "schema",
    ]
    data_quality_tokens = [
        "data quality",
        "quality score",
        "profiling",
        "profile table",
        "nulls",
        "missing values",
        "outliers",
        "sentinel",
        "duplicate values",
        "cardinality",
        "volumetric",
        "data drift",
        "column quality",
        "qualite des donnees",
        "qualite de donnees",
        "profilage",
        "valeurs nulles",
        "valeurs manquantes",
        "valeurs sentinelles",
        "derive des donnees",
        "qualite des colonnes",
    ]
    file_action_tokens = [
        "create",
        "make",
        "generate",
        "save",
        "write",
        "edit",
        "update",
        "append",
        "move",
        "rename",
        "delete",
        "remove",
        "copy",
        "open",
        "read",
        "creer",
        "fais",
        "faire",
        "generer",
        "sauvegarder",
        "ecrire",
        "modifier",
        "mettre a jour",
        "ajouter",
        "deplacer",
        "renommer",
        "supprimer",
        "effacer",
        "copier",
        "ouvrir",
        "lire",
    ]
    file_target_tokens = [
        "file",
        "fichier",
        "folder",
        "dossier",
        "directory",
        "repertoire",
        "document",
        "spreadsheet",
        "workbook",
        "sheet",
        "excel",
        "csv",
        "parquet",
        "docx",
        "markdown",
        "json",
        "yaml",
        "yml",
        "sql",
        "python file",
        "texte",
    ]
    file_extension_hit = bool(
        re.search(r"\.(txt|md|csv|tsv|xlsx|xls|docx|parquet|json|ya?ml|html|sql|py|log)\b", normalized)
    )
    file_action_hit = any(token in normalized for token in file_action_tokens)
    file_target_hit = file_extension_hit or any(token in normalized for token in file_target_tokens)
    file_creation_or_edit_hit = file_action_hit and file_target_hit

    file_hit = file_creation_or_edit_hit or any(token in normalized for token in file_tokens)
    clickhouse_hit = any(token in normalized for token in clickhouse_tokens)
    data_quality_hit = any(token in normalized for token in data_quality_tokens)
    export_pipeline = _extract_clickhouse_file_export_pipeline(user_message)

    if export_pipeline and clickhouse_hit:
        return "clickhouse_query", "The request needs a ClickHouse query first and then a file export from the query result."

    if data_quality_hit and not file_hit:
        return "data_quality_tables", "The request is explicitly about table profiling or data-quality analysis."
    if file_creation_or_edit_hit and not data_quality_hit:
        return "file_management", "The request explicitly asks to create or modify a file, so File management should handle it."
    if file_hit and not clickhouse_hit and not data_quality_hit:
        return "file_management", "The request is explicitly about filesystem or spreadsheet actions."
    if clickhouse_hit and not file_hit and not data_quality_hit:
        return "clickhouse_query", "The request is explicitly about database querying or charting."
    return None


async def analyze_manager_routing(
    user_message: str,
    history: list[dict[str, Any]],
    manager_state: dict[str, Any],
    clickhouse_state: dict[str, Any],
    file_manager_state: dict[str, Any],
    data_quality_state: dict[str, Any],
    llm_base_url: str,
    llm_model: str,
    llm_provider: str,
    llm_api_key: Optional[str] = None,
) -> dict[str, str]:
    heuristic = _heuristic_manager_delegate(
        user_message,
        manager_state,
        clickhouse_state,
        file_manager_state,
        data_quality_state,
    )
    if heuristic:
        return {
            "delegate": heuristic[0],
            "reasoning": heuristic[1],
            "handoff_message": user_message,
        }

    trimmed_history = _manager_trimmed_history(history)
    prompt = f"""
You are the routing brain of the RAGnarok Agent Manager.
Choose which specialist should handle the next turn.

Available delegates:
- manager: use this when no specialist tool is needed and the manager can answer directly.
- clickhouse_query: use this for SQL, schemas, tables, analytics, metrics, database exploration, and charts from ClickHouse data.
- file_management: use this for filesystem actions, directories, files, CSV/Excel/Word/Parquet handling, create/edit/move/delete operations.
- data_quality_tables: use this for table profiling, null/outlier/sentinel analysis, column health scoring, and volumetric data-quality checks.

If more than one specialist could be relevant, choose the one that should act first.
Return JSON only with this exact shape:
{{
  "delegate": "manager" | "clickhouse_query" | "file_management" | "data_quality_tables",
  "reasoning": "short English explanation",
  "handoff_message": "short English specialist instruction preserving the user's intent"
}}

Rules:
- Keep the answer in English.
- Prefer a specialist when the request depends on real filesystem state or ClickHouse data.
- If the user asks to create, write, save, edit, move, rename, or delete a file or folder, delegate to `file_management`.
- Keep `handoff_message` concise and actionable.
- If the manager can answer directly, set `delegate` to `manager`.

Current manager state:
{json.dumps(manager_state, ensure_ascii=False, indent=2)}

Current specialist state summary:
{_manager_specialist_state_summary(clickhouse_state, file_manager_state, data_quality_state)}

Recent conversation:
{json.dumps(trimmed_history, ensure_ascii=False, indent=2)}

User message:
{user_message}
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
    delegate = _normalize_manager_delegate_role(parsed.get("delegate"), allow_manager=True) or "manager"
    handoff_message = str(parsed.get("handoff_message") or user_message).strip() or user_message
    reasoning = str(parsed.get("reasoning") or "").strip() or "The manager selected the best available execution path."
    return {
        "delegate": delegate,
        "reasoning": reasoning,
        "handoff_message": handoff_message,
    }


async def _run_manager_direct_response(
    history: list[dict[str, Any]],
    llm_base_url: str,
    llm_model: str,
    llm_provider: str,
    llm_api_key: Optional[str],
    system_prompt: str,
) -> str:
    manager_system_prompt = (
        f"{system_prompt}\n\n"
        "You are the RAGnarok Agent Manager. Reply in English. Keep answers concise, "
        "use specialist agents only when needed, and explain clearly when you can answer directly."
    ).strip()
    messages = [{"role": "system", "content": manager_system_prompt}]
    messages.extend(_manager_trimmed_history(history, limit=12))
    return await llm_chat(
        messages,
        llm_base_url,
        llm_model,
        llm_provider,
        llm_api_key,
    )


def _prefix_agent_steps(
    steps: list[dict[str, Any]],
    specialist_role: str,
) -> list[dict[str, Any]]:
    label = _manager_specialist_label(specialist_role)
    prefixed: list[dict[str, Any]] = []
    for index, step in enumerate(steps):
        prefixed.append(
            {
                **step,
                "id": str(step.get("id") or f"{specialist_role}-{index}"),
                "title": f"{label} · {step.get('title') or 'Step'}",
            }
        )
    return prefixed


# ── ClickHouse agent LLM helpers ──────────────────────────────────────────────

async def analyze_clickhouse_schema(
    user_request: str,
    table_name: str,
    schema: list[dict[str, str]],
    conversation_memory: str,
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

Recent conversation memory:
{conversation_memory}

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


async def analyze_clickhouse_tables(
    user_request: str,
    available_tables: list[str],
    conversation_memory: str,
    llm_base_url: str,
    llm_model: str,
    llm_provider: str,
    llm_api_key: Optional[str] = None,
) -> dict[str, Any]:
    tables_text = "\n".join(f"- {table_name}" for table_name in available_tables[:200])
    prompt = f"""
You are routing a user analytics request to the best ClickHouse table.
Return JSON only with this exact shape:
{{
  "selected_table": "orders",
  "table_candidates": ["orders", "order_items"],
  "table_choice_required": false,
  "table_choice_prompt": "Which table should I use for this request?",
  "reasoning": "short explanation"
}}

Rules:
- Use only table names from the provided list.
- If one table is clearly the best match, set selected_table and table_choice_required to false.
- If several tables are plausible, set table_choice_required to true and return the best candidates.
- Keep prompts in English.
- Prefer not asking the user unless there is a real ambiguity.

Available tables:
{tables_text}

Recent conversation memory:
{conversation_memory}

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
        "selected_table": str(parsed.get("selected_table", "") or ""),
        "table_candidates": [
            item for item in parsed.get("table_candidates", [])
            if isinstance(item, str)
        ],
        "table_choice_required": bool(parsed.get("table_choice_required", False)),
        "table_choice_prompt": str(parsed.get("table_choice_prompt", "") or ""),
        "reasoning": str(parsed.get("reasoning", "") or ""),
    }


async def generate_clickhouse_sql(
    user_request: str,
    table_name: str,
    schema: list[dict[str, str]],
    selected_field: Optional[str],
    selected_date_field: Optional[str],
    conversation_memory: str,
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

Recent conversation memory:
{conversation_memory}

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
    conversation_memory: str,
    llm_base_url: str,
    llm_model: str,
    llm_provider: str,
    llm_api_key: Optional[str],
) -> str:
    preview = json.dumps(result_rows[:10], ensure_ascii=False, indent=2)
    prompt = f"""
You are summarizing a ClickHouse query result for an end user.
Write the full answer in English and keep it concise.
The tone must be business-facing and functional, not technical.

Formatting rules:
- Return Markdown only.
- Write only the business result section body, with no title.
- Prefer either one short paragraph or 2 to 4 flat bullet points, depending on what is clearer.
- Highlight the most important values, entities, dates, or thresholds with **bold**.
- Focus on what the result means for the user.
- Do not explain SQL generation, schema inspection, or tool usage.
- Do not include headings, SQL blocks, reasoning sections, or code fences.
- If the result is empty, say so clearly in one short sentence and give the most likely interpretation.

Context:
- User request: {user_request}
- Recent conversation memory:
{conversation_memory}
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


def _clean_clickhouse_summary_markdown(text: str) -> str:
    cleaned = (text or "").strip()
    if not cleaned:
        return "No clear business summary could be generated, but the query completed successfully."

    cleaned = re.sub(r"^\s*#{1,6}\s*(answer|result|summary|executive summary)\s*\n+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"(?is)\n*#{1,6}\s*sql\b[\s\S]*$", "", cleaned)
    cleaned = re.sub(r"(?is)\n*#{1,6}\s*reasoning\b[\s\S]*$", "", cleaned)
    cleaned = re.sub(r"(?is)```sql[\s\S]*?```", "", cleaned)
    cleaned = re.sub(r"(?is)```[\s\S]*?```", "", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()

    return cleaned or "No clear business summary could be generated, but the query completed successfully."


def _build_clickhouse_sql_section(executed_sqls: list[str]) -> str:
    sql_statements = []
    seen = set()
    for statement in executed_sqls:
        normalized = str(statement or "").strip()
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        sql_statements.append(normalized)

    if not sql_statements:
        return ""

    if len(sql_statements) == 1:
        return f"## Executed SQL\n```sql\n{sql_statements[0]}\n```"

    blocks = []
    for index, statement in enumerate(sql_statements, start=1):
        blocks.append(f"### Query {index}\n```sql\n{statement}\n```")
    return "## Executed SQL\n\n" + "\n\n".join(blocks)


def build_clickhouse_response_markdown(
    result_markdown: str,
    executed_sqls: list[str],
    middle_sections: Optional[list[str]] = None,
) -> str:
    sections = [
        "## Result",
        _clean_clickhouse_summary_markdown(result_markdown),
    ]

    for section in middle_sections or []:
        normalized = str(section or "").strip()
        if normalized:
            sections.append(normalized)

    sql_section = _build_clickhouse_sql_section(executed_sqls)
    if sql_section:
        sections.append(sql_section)

    return "\n\n".join(sections)


# ── CrewAI planning helpers ───────────────────────────────────────────────────

PLANNING_ROLE_PROMPTS = {
    "manager": (
        "You are an operations manager agent. Produce a concise operational brief with "
        "clear priorities, risks, and next actions."
    ),
    "file_management": (
        "You are a file management agent. Use filesystem facts only, keep answers concise, "
        "and never imply that a destructive action ran without explicit confirmation."
    ),
}


def _safe_zoneinfo(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name or "UTC")
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    try:
        text = value.strip().replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _to_iso_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _parse_time_of_day(value: str) -> tuple[int, int]:
    match = re.fullmatch(r"(\d{1,2}):(\d{2})", (value or "").strip())
    if not match:
        return 9, 0
    hour = max(0, min(23, int(match.group(1))))
    minute = max(0, min(59, int(match.group(2))))
    return hour, minute


def _weekday_to_index(day: str) -> int:
    return PLANNER_WEEKDAYS.index(day) if day in PLANNER_WEEKDAYS else 0


def _build_localized_datetime(date_value: datetime, time_of_day: str, timezone_name: str) -> datetime:
    tz = _safe_zoneinfo(timezone_name)
    localized = date_value.astimezone(tz)
    hour, minute = _parse_time_of_day(time_of_day)
    return localized.replace(hour=hour, minute=minute, second=0, microsecond=0)


def _compute_plan_next_run_at(plan: dict, reference_dt: Optional[datetime] = None) -> Optional[str]:
    trigger = plan.get("trigger") or {}
    if plan.get("status") != "active":
        return None

    now_dt = (reference_dt or datetime.now(timezone.utc)).astimezone(timezone.utc)
    kind = trigger.get("kind")
    timezone_name = str(trigger.get("timezone") or "UTC")

    if kind == "once":
        one_time = _parse_iso_datetime(trigger.get("oneTimeAt"))
        return _to_iso_datetime(one_time) if one_time and one_time > now_dt else None

    if kind == "daily":
        candidate = _build_localized_datetime(now_dt, trigger.get("timeOfDay") or "09:00", timezone_name)
        if candidate.astimezone(timezone.utc) <= now_dt:
            candidate += timedelta(days=1)
        return _to_iso_datetime(candidate.astimezone(timezone.utc))

    if kind == "weekly":
        weekdays = [
            day for day in trigger.get("weekdays", [])
            if isinstance(day, str) and day in PLANNER_WEEKDAYS
        ] or ["mon"]
        current_local = now_dt.astimezone(_safe_zoneinfo(timezone_name))
        hour, minute = _parse_time_of_day(trigger.get("timeOfDay") or "09:00")
        for offset in range(0, 8):
            candidate_date = current_local + timedelta(days=offset)
            candidate_day = PLANNER_WEEKDAYS[candidate_date.weekday()]
            if candidate_day not in weekdays:
                continue
            candidate = candidate_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if candidate.astimezone(timezone.utc) > now_dt:
                return _to_iso_datetime(candidate.astimezone(timezone.utc))
        fallback = current_local + timedelta(days=7)
        fallback = fallback.replace(hour=hour, minute=minute, second=0, microsecond=0)
        return _to_iso_datetime(fallback.astimezone(timezone.utc))

    if kind == "interval":
        interval_minutes = max(1, int(trigger.get("intervalMinutes") or 60))
        last_run = _parse_iso_datetime(plan.get("lastRunAt"))
        anchor = last_run or now_dt
        if not last_run and plan.get("nextRunAt"):
            return plan.get("nextRunAt")
        return _to_iso_datetime(anchor + timedelta(minutes=interval_minutes))

    return None


def _refresh_planning_plan(plan: dict, reference_dt: Optional[datetime] = None) -> dict:
    trigger = plan.get("trigger") or {}
    if trigger.get("kind") in {"clickhouse_watch", "file_watch"}:
        plan["nextRunAt"] = None
        return plan

    plan["nextRunAt"] = _compute_plan_next_run_at(plan, reference_dt)
    return plan


def _planning_state_from_db(state: dict) -> dict:
    planning = _normalize_planning_state(state.get("planning"))
    for plan in planning["plans"]:
        _refresh_planning_plan(plan)
    planning["runs"] = planning["runs"][:PLANNER_MAX_RUNS]
    return planning


def _default_planning_draft(timezone_name: str = "UTC") -> dict:
    return {
        "name": "",
        "prompt": "",
        "agents": [],
        "status": "active",
        "trigger": {
            "kind": "daily",
            "timezone": timezone_name or "UTC",
            "oneTimeAt": "",
            "timeOfDay": "09:00",
            "weekdays": ["mon"],
            "intervalMinutes": 60,
            "pollMinutes": 5,
            "watchSql": "",
            "watchMode": "result_changes",
            "directory": "",
            "pattern": "*",
            "recursive": False,
        },
    }


def _normalize_planner_agent_role(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    lowered = normalize_choice(value).lower()
    aliases = {
        "manager": "manager",
        "agent manager": "manager",
        "clickhouse query": "clickhouse_query",
        "clickhouse_query": "clickhouse_query",
        "clickhouse": "clickhouse_query",
        "file management": "file_management",
        "file manager": "file_management",
        "file_management": "file_management",
    }
    return aliases.get(lowered)


def _merge_planning_draft(current_draft: Optional[dict], updates: Optional[dict], timezone_name: str = "UTC") -> dict:
    current = _default_planning_draft(timezone_name)
    if isinstance(current_draft, dict):
        current.update({k: v for k, v in current_draft.items() if k != "trigger"})
        current["trigger"].update((current_draft.get("trigger") or {}))

    if not isinstance(updates, dict):
        return current

    if updates.get("name"):
        current["name"] = str(updates.get("name")).strip()
    if updates.get("prompt"):
        current["prompt"] = str(updates.get("prompt")).strip()

    if isinstance(updates.get("agents"), list):
        existing_agents = [agent for agent in current.get("agents", []) if agent in PLANNER_AGENT_ROLES]
        for agent in updates["agents"]:
            normalized_agent = _normalize_planner_agent_role(agent)
            if normalized_agent and normalized_agent not in existing_agents:
                existing_agents.append(normalized_agent)
        current["agents"] = existing_agents

    if updates.get("status") in {"active", "paused"}:
        current["status"] = updates["status"]

    incoming_trigger = updates.get("trigger")
    if isinstance(incoming_trigger, dict):
        for key, value in incoming_trigger.items():
            if value in (None, "", []):
                continue
            current["trigger"][key] = value

    current["trigger"] = _normalize_planning_trigger(current["trigger"])
    if not current["name"] and current["prompt"]:
        prompt_words = current["prompt"].split()
        current["name"] = " ".join(prompt_words[:6])[:64]

    return current


def _validate_planning_draft(draft: dict) -> list[str]:
    missing: list[str] = []
    if not draft.get("prompt"):
        missing.append("prompt")
    if not draft.get("agents"):
        missing.append("agents")

    trigger = draft.get("trigger") or {}
    kind = trigger.get("kind")
    if kind not in PLANNER_TRIGGER_KINDS:
        missing.append("trigger_kind")
        return missing

    if kind == "once" and not trigger.get("oneTimeAt"):
        missing.append("one_time_at")
    elif kind == "daily" and not trigger.get("timeOfDay"):
        missing.append("time_of_day")
    elif kind == "weekly":
        if not trigger.get("weekdays"):
            missing.append("weekdays")
        if not trigger.get("timeOfDay"):
            missing.append("time_of_day")
    elif kind == "interval" and int(trigger.get("intervalMinutes") or 0) <= 0:
        missing.append("interval_minutes")
    elif kind == "clickhouse_watch":
        if not trigger.get("watchSql"):
            missing.append("watch_sql")
        if int(trigger.get("pollMinutes") or 0) <= 0:
            missing.append("poll_minutes")
    elif kind == "file_watch":
        if not trigger.get("directory"):
            missing.append("directory")
        if int(trigger.get("pollMinutes") or 0) <= 0:
            missing.append("poll_minutes")

    return missing


def _planning_missing_prompt(missing_fields: list[str]) -> str:
    labels = {
        "prompt": "what the automation should do",
        "agents": "which existing agent(s) should run",
        "trigger_kind": "what trigger type you want",
        "one_time_at": "the exact date and time",
        "time_of_day": "the time of day",
        "weekdays": "which weekday(s) should run",
        "interval_minutes": "the repeat interval in minutes",
        "watch_sql": "the ClickHouse watch SQL",
        "poll_minutes": "the polling frequency",
        "directory": "the directory to watch",
    }
    readable = [labels.get(field, field.replace("_", " ")) for field in missing_fields]
    if not readable:
        return ""
    if len(readable) == 1:
        return readable[0]
    return ", ".join(readable[:-1]) + f" and {readable[-1]}"


def _planning_summary_markdown(draft: dict, missing_fields: list[str]) -> str:
    trigger = draft.get("trigger") or {}
    agents = draft.get("agents") or []
    lines = [
        "## Draft Summary",
        f"- Name: {draft.get('name') or 'Untitled plan'}",
        f"- Agents: {', '.join(agents) if agents else 'Not selected yet'}",
        f"- Trigger: {trigger.get('kind') or 'Not selected yet'}",
    ]
    if trigger.get("kind") == "once":
        lines.append(f"- Run at: {trigger.get('oneTimeAt') or 'Missing'}")
    elif trigger.get("kind") == "daily":
        lines.append(f"- Time: {trigger.get('timeOfDay') or 'Missing'} ({trigger.get('timezone') or 'UTC'})")
    elif trigger.get("kind") == "weekly":
        weekdays = ", ".join(trigger.get("weekdays") or []) or "Missing"
        lines.append(f"- Weekdays: {weekdays}")
        lines.append(f"- Time: {trigger.get('timeOfDay') or 'Missing'} ({trigger.get('timezone') or 'UTC'})")
    elif trigger.get("kind") == "interval":
        lines.append(f"- Every: {trigger.get('intervalMinutes') or 'Missing'} minute(s)")
    elif trigger.get("kind") == "clickhouse_watch":
        lines.append(f"- Watch SQL: `{(trigger.get('watchSql') or '').strip()[:100] or 'Missing'}`")
        lines.append(f"- Polling: every {trigger.get('pollMinutes') or 'Missing'} minute(s)")
    elif trigger.get("kind") == "file_watch":
        lines.append(f"- Directory: `{trigger.get('directory') or 'Missing'}`")
        lines.append(f"- Pattern: `{trigger.get('pattern') or '*'}`")
        lines.append(f"- Polling: every {trigger.get('pollMinutes') or 'Missing'} minute(s)")

    lines.extend(
        [
            "",
            "## Objective",
            draft.get("prompt") or "Missing",
        ]
    )

    if missing_fields:
        lines.extend(
            [
                "",
                "## Next Step",
                f"I still need { _planning_missing_prompt(missing_fields) } before this automation is ready.",
            ]
        )
    else:
        lines.extend(
            [
                "",
                "## Next Step",
                "The draft is ready. Open the planner form to review and save it.",
            ]
        )
    return "\n".join(lines)


async def analyze_planning_request(
    user_message: str,
    current_draft: dict,
    llm_base_url: str,
    llm_model: str,
    llm_provider: str,
    llm_api_key: Optional[str] = None,
) -> dict[str, Any]:
    prompt = f"""
You are helping a user configure an automation planner for existing agents.
Return JSON only with this exact shape:
{{
  "draft": {{
    "name": "Morning anomaly check",
    "prompt": "Analyze new sales anomalies and write a short operational summary.",
    "agents": ["manager", "clickhouse_query"],
    "status": "active",
    "trigger": {{
      "kind": "daily",
      "timezone": "Europe/Paris",
      "oneTimeAt": "",
      "timeOfDay": "09:00",
      "weekdays": ["mon"],
      "intervalMinutes": 60,
      "pollMinutes": 5,
      "watchSql": "",
      "watchMode": "result_changes",
      "directory": "",
      "pattern": "*",
      "recursive": false
    }}
  }},
  "clarification_question": "short English question if something important is missing",
  "should_open_form": false,
  "reasoning": "short explanation"
}}

Allowed agents: manager, clickhouse_query, file_management.
Allowed trigger kinds: once, daily, weekly, interval, clickhouse_watch, file_watch.
Allowed watch modes: returns_rows, count_increases, result_changes.
Allowed weekdays: mon, tue, wed, thu, fri, sat, sun.
Keep the answer in English.
If the user explicitly asks to open the form, set should_open_form to true.
Do not invent unsupported trigger kinds or agent names.
Only fill fields when the user clearly implies them or they already exist in the current draft.

Current draft:
{json.dumps(current_draft, ensure_ascii=False, indent=2)}

User message:
{user_message}
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
        "draft": parsed.get("draft") if isinstance(parsed.get("draft"), dict) else {},
        "clarification_question": str(parsed.get("clarification_question", "") or "").strip(),
        "should_open_form": bool(parsed.get("should_open_form", False)),
        "reasoning": str(parsed.get("reasoning", "") or "").strip(),
    }


def _planning_state_markdown(planning_state: dict) -> str:
    plans = planning_state.get("plans", [])
    if not plans:
        return (
            "## CrewAI Planning\n"
            "No automation has been saved yet.\n\n"
            "Use natural language to describe a schedule or open the planner form."
        )

    lines = [
        "## Existing Plans",
    ]
    for plan in plans[:8]:
        lines.append(
            f"- **{plan.get('name') or 'Untitled plan'}** · {plan.get('status')} · "
            f"{(plan.get('trigger') or {}).get('kind', 'unknown')}"
        )
    if len(plans) > 8:
        lines.append(f"- ...and {len(plans) - 8} more plan(s)")
    return "\n".join(lines)


def _planning_trigger_label(trigger_context: dict) -> str:
    kind = trigger_context.get("kind") or "manual"
    if kind == "once":
        return "One-time schedule"
    if kind == "daily":
        return "Daily schedule"
    if kind == "weekly":
        return "Weekly schedule"
    if kind == "interval":
        return "Interval schedule"
    if kind == "clickhouse_watch":
        return "ClickHouse watch"
    if kind == "file_watch":
        return "File watcher"
    return "Manual run"


def _truncate_json(value: Any, max_chars: int = 2000) -> str:
    text = json.dumps(value, ensure_ascii=False, indent=2, default=str)
    return text if len(text) <= max_chars else text[: max_chars - 1] + "…"


def _build_trigger_context_markdown(trigger_context: dict) -> str:
    kind = trigger_context.get("kind") or "manual"
    lines = [
        f"Trigger kind: {kind}",
        f"Trigger label: {_planning_trigger_label(trigger_context)}",
    ]
    if kind == "clickhouse_watch":
        lines.append("Watch SQL:")
        lines.append(str(trigger_context.get("sql") or "").strip() or "N/A")
        preview = trigger_context.get("rows") or []
        if preview:
            lines.append("Watch result preview:")
            lines.append(_truncate_json(preview[:5]))
    elif kind == "file_watch":
        files = trigger_context.get("files") or []
        if files:
            lines.append("New files:")
            lines.extend(str(path) for path in files[:10])
    return "\n".join(lines)


def _app_llm_config(app_config: dict) -> dict:
    return {
        "llm_base_url": str(app_config.get("baseUrl") or "http://localhost:11434"),
        "llm_model": str(app_config.get("model") or "llama3"),
        "llm_provider": str(app_config.get("provider") or "ollama"),
        "llm_api_key": app_config.get("apiKey") or None,
    }


def _app_clickhouse_config(app_config: dict) -> "ClickHouseConfig":
    return ClickHouseConfig(
        host=str(app_config.get("clickhouseHost") or "localhost"),
        port=int(app_config.get("clickhousePort") or 8123),
        database=str(app_config.get("clickhouseDatabase") or "default"),
        username=str(app_config.get("clickhouseUsername") or "default"),
        password=str(app_config.get("clickhousePassword") or ""),
        secure=bool(app_config.get("clickhouseSecure", False)),
        verify_ssl=bool(app_config.get("clickhouseVerifySsl", True)),
        http_path=str(app_config.get("clickhouseHttpPath") or ""),
        query_limit=int(app_config.get("clickhouseQueryLimit") or 200),
    )


def _app_file_manager_config(app_config: dict) -> dict[str, Any]:
    return _normalize_file_manager_config(app_config.get("fileManagerConfig"))


async def _run_local_role_agent(
    role: str,
    plan: dict,
    trigger_context: dict,
    app_config: dict,
) -> dict[str, Any]:
    llm_config = _app_llm_config(app_config)
    system_prompt = PLANNING_ROLE_PROMPTS.get(role, PLANNING_ROLE_PROMPTS["manager"])
    user_prompt = f"""
You are executing a scheduled automation for RAGnarok.
Write the answer in English and keep it concise but useful.

Automation name: {plan.get("name") or "Untitled plan"}
Automation objective:
{plan.get("prompt") or ""}

Trigger context:
{_build_trigger_context_markdown(trigger_context)}
""".strip()

    content = await llm_chat(
        [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        llm_config["llm_base_url"],
        llm_config["llm_model"],
        llm_config["llm_provider"],
        llm_config["llm_api_key"],
    )
    return {"agent": role, "status": "success", "content": content}


async def _run_file_management_planning_agent(
    plan: dict,
    trigger_context: dict,
    app_config: dict,
) -> dict[str, Any]:
    llm_config = _app_llm_config(app_config)
    file_manager_config = _app_file_manager_config(app_config)

    request_text = (plan.get("prompt") or "").strip()
    if trigger_context.get("kind") == "file_watch":
        files = trigger_context.get("files") or []
        if files:
            request_text += (
                "\n\nAdditional context from the file watcher:\n"
                + "\n".join(str(path) for path in files[:10])
            )
    elif trigger_context.get("kind") == "clickhouse_watch":
        request_text += (
            "\n\nAdditional context from the ClickHouse watcher:\n"
            f"{_truncate_json(trigger_context.get('rows') or [])}"
        )

    scratchpad: list[dict[str, Any]] = []
    max_iterations = max(
        1,
        min(FILE_MANAGER_MAX_ITERATIONS, int(file_manager_config["maxIterations"])),
    )
    last_error = ""

    for _ in range(max_iterations):
        planned = await plan_file_manager_step(
            request_text,
            [],
            scratchpad,
            file_manager_config["basePath"],
            file_manager_config["systemPrompt"],
            llm_config["llm_base_url"],
            llm_config["llm_model"],
            llm_config["llm_provider"],
            llm_config["llm_api_key"],
        )

        action = planned.get("action")
        if action == "final":
            content = planned.get("final_answer") or "## Answer\nThe scheduled file-management task is complete."
            return {"agent": "file_management", "status": "success", "content": content}

        tool_name = planned.get("tool_name") or ""
        tool_input = dict(planned.get("tool_input") or {})
        if action != "tool" or tool_name not in FILE_MANAGER_TOOLS:
            last_error = "The file-management planner returned an invalid tool action."
            scratchpad.append({"type": "error", "error": last_error})
            continue

        if tool_name in FILE_MANAGER_CONFIRMATION_TOOLS and "confirmed" not in tool_input:
            tool_input["confirmed"] = False

        try:
            result = execute_file_manager_tool(tool_name, tool_input, file_manager_config["basePath"])
        except Exception as exc:
            last_error = str(exc)
            scratchpad.append(
                {
                    "type": "tool_error",
                    "tool": tool_name,
                    "input": tool_input,
                    "error": last_error,
                }
            )
            continue

        scratchpad.append(
            {
                "type": "tool_result",
                "tool": tool_name,
                "input": tool_input,
                "summary": result.get("summary") or "",
                "preview": result.get("preview") or "",
            }
        )

        if result.get("requires_confirmation"):
            preview = result.get("preview") or ""
            content = (
                "## Answer\n"
                "The scheduled file-management task stopped because it requires explicit user confirmation.\n\n"
                "## Reasoning\n"
                f"{result.get('summary') or 'A destructive or overwrite action was requested.'}"
            )
            if preview:
                content += f"\n\n## Preview\n{preview}"
            return {"agent": "file_management", "status": "error", "content": content}

    content = "## Answer\nThe scheduled file-management task reached its iteration limit."
    if last_error:
        content += f"\n\n```text\n{last_error}\n```"
    return {"agent": "file_management", "status": "error", "content": content}


async def _run_clickhouse_planning_agent(
    plan: dict,
    trigger_context: dict,
    app_config: dict,
) -> dict[str, Any]:
    clickhouse = _app_clickhouse_config(app_config)
    llm_config = _app_llm_config(app_config)
    request_text = (plan.get("prompt") or "").strip()
    if trigger_context.get("kind") == "clickhouse_watch":
        request_text += (
            "\n\nAdditional context from the trigger watch query:\n"
            f"{_truncate_json(trigger_context.get('rows') or [])}"
        )
    elif trigger_context.get("kind") == "file_watch":
        request_text += (
            "\n\nAdditional context from new files:\n"
            + "\n".join(str(path) for path in (trigger_context.get("files") or [])[:10])
        )

    tables = await list_clickhouse_tables(clickhouse)
    if not tables:
        raise ValueError("No ClickHouse tables are available for the scheduled ClickHouse agent.")

    table_analysis = await analyze_clickhouse_tables(
        request_text,
        tables,
        "No recent memory. This is a scheduled execution.",
        llm_config["llm_base_url"],
        llm_config["llm_model"],
        llm_config["llm_provider"],
        llm_config["llm_api_key"],
    )
    matched_selected = match_available_options([table_analysis.get("selected_table", "")], tables)
    matched_candidates = match_available_options(table_analysis.get("table_candidates", []), tables)
    selected_table = (
        (matched_selected[0] if matched_selected else None)
        or (matched_candidates[0] if matched_candidates else None)
        or tables[0]
    )

    schema = await describe_clickhouse_table(clickhouse, selected_table)
    if not schema:
        raise ValueError(f"Table '{selected_table}' has no readable schema.")

    analysis = await analyze_clickhouse_schema(
        request_text,
        selected_table,
        schema,
        "No recent memory. This is a scheduled execution.",
        llm_config["llm_base_url"],
        llm_config["llm_model"],
        llm_config["llm_provider"],
        llm_config["llm_api_key"],
    )
    candidate_fields = match_schema_columns(analysis["field_candidates"], schema)
    date_fields = match_schema_columns(analysis["date_candidates"], schema) or find_date_columns(schema)
    selected_field = candidate_fields[0] if candidate_fields else None
    selected_date_field = date_fields[0] if date_fields else None

    generated = await generate_clickhouse_sql(
        request_text,
        selected_table,
        schema,
        selected_field,
        selected_date_field,
        "No recent memory. This is a scheduled execution.",
        llm_config["llm_base_url"],
        llm_config["llm_model"],
        llm_config["llm_provider"],
        llm_config["llm_api_key"],
        clickhouse.query_limit,
    )

    sql = generated["sql"]
    if not is_safe_read_only_sql(sql):
        raise ValueError("Scheduled ClickHouse SQL was rejected because it is not read-only.")

    try:
        await execute_clickhouse_sql(clickhouse, f"EXPLAIN SYNTAX {sql}", readonly=False, json_format=False)
        result = await execute_clickhouse_sql(clickhouse, sql)
    except Exception as first_error:
        repaired = await generate_clickhouse_sql(
            request_text,
            selected_table,
            schema,
            selected_field,
            selected_date_field,
            "No recent memory. This is a scheduled execution.",
            llm_config["llm_base_url"],
            llm_config["llm_model"],
            llm_config["llm_provider"],
            llm_config["llm_api_key"],
            clickhouse.query_limit,
            error_feedback=str(first_error),
        )
        sql = repaired["sql"]
        if not is_safe_read_only_sql(sql):
            raise ValueError("Scheduled ClickHouse SQL repair was rejected because it is not read-only.")
        generated["reasoning"] = repaired["reasoning"] or generated["reasoning"]
        result = await execute_clickhouse_sql(clickhouse, sql)

    result_summary = await summarize_clickhouse_result(
        request_text,
        sql,
        generated["reasoning"],
        result.get("data", []),
        "No recent memory. This is a scheduled execution.",
        llm_config["llm_base_url"],
        llm_config["llm_model"],
        llm_config["llm_provider"],
        llm_config["llm_api_key"],
    )
    content = build_clickhouse_response_markdown(result_summary, [sql])
    return {"agent": "clickhouse_query", "status": "success", "content": content}


async def _summarize_planning_outputs(
    plan: dict,
    outputs: list[dict[str, Any]],
    trigger_context: dict,
    app_config: dict,
) -> str:
    llm_config = _app_llm_config(app_config)
    prompt = f"""
You are summarizing the result of a scheduled automation.
Write the answer in English with exactly these markdown sections:
## Summary
One concise summary.

## Trigger
One concise explanation of why it ran.

## Agent Outputs
One-line summary per agent.

Automation name: {plan.get("name") or "Untitled plan"}
Objective:
{plan.get("prompt") or ""}

Trigger context:
{_build_trigger_context_markdown(trigger_context)}

Outputs:
{_truncate_json(outputs, max_chars=5000)}
""".strip()

    return await llm_chat(
        [{"role": "user", "content": prompt}],
        llm_config["llm_base_url"],
        llm_config["llm_model"],
        llm_config["llm_provider"],
        llm_config["llm_api_key"],
    )


def _clickhouse_watch_metric(result: dict[str, Any]) -> Optional[float]:
    rows = result.get("data") or []
    if not rows:
        return 0.0
    first_row = rows[0]
    if isinstance(first_row, dict):
        for value in first_row.values():
            numeric = normalize_chart_value(value)
            if numeric is not None:
                return numeric
    return float(len(rows))


async def _evaluate_clickhouse_watch(plan: dict, app_config: dict, now_dt: datetime) -> Optional[dict[str, Any]]:
    trigger = plan.get("trigger") or {}
    runtime = plan.get("runtime") or _default_planning_runtime()
    last_checked = _parse_iso_datetime(runtime.get("lastCheckedAt"))
    poll_minutes = max(1, int(trigger.get("pollMinutes") or 5))
    if last_checked and last_checked + timedelta(minutes=poll_minutes) > now_dt:
        return None

    sql = clean_sql_text(trigger.get("watchSql") or "")
    runtime["lastCheckedAt"] = _to_iso_datetime(now_dt)
    if not sql or not is_safe_read_only_sql(sql):
        return None

    clickhouse = _app_clickhouse_config(app_config)
    result = await execute_clickhouse_sql(clickhouse, sql)
    rows = result.get("data", [])[:10]
    fingerprint = hashlib.sha256(
        json.dumps(rows, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    metric = _clickhouse_watch_metric(result)

    previous_fingerprint = str(runtime.get("lastSeenFingerprint") or "")
    previous_metric = runtime.get("lastSeenMetric")
    runtime["lastSeenFingerprint"] = fingerprint
    runtime["lastSeenMetric"] = metric
    plan["runtime"] = runtime

    watch_mode = trigger.get("watchMode") or "result_changes"
    if previous_fingerprint == "" and previous_metric is None:
        return None
    if watch_mode == "returns_rows":
        if rows and previous_fingerprint != fingerprint:
            return {"kind": "clickhouse_watch", "sql": sql, "rows": rows}
        return None
    if watch_mode == "count_increases":
        if (
            metric is not None
            and isinstance(previous_metric, (int, float))
            and metric > previous_metric
        ):
            return {"kind": "clickhouse_watch", "sql": sql, "rows": rows, "metric": metric}
        return None
    if previous_fingerprint != fingerprint:
        return {"kind": "clickhouse_watch", "sql": sql, "rows": rows}
    return None


def _scan_directory_files(directory: str, pattern: str, recursive: bool) -> list[str]:
    root = Path(directory).expanduser()
    if not root.exists() or not root.is_dir():
        return []

    matched: list[str] = []
    iterator = root.rglob("*") if recursive else root.glob("*")
    for path in iterator:
        if not path.is_file():
            continue
        relative_name = str(path.relative_to(root))
        if fnmatch.fnmatch(relative_name, pattern) or fnmatch.fnmatch(path.name, pattern):
            matched.append(str(path.resolve()))
        if len(matched) >= PLANNER_MAX_KNOWN_FILES:
            break
    return sorted(matched)


def _evaluate_file_watch(plan: dict, now_dt: datetime) -> Optional[dict[str, Any]]:
    trigger = plan.get("trigger") or {}
    runtime = plan.get("runtime") or _default_planning_runtime()
    last_checked = _parse_iso_datetime(runtime.get("lastCheckedAt"))
    poll_minutes = max(1, int(trigger.get("pollMinutes") or 5))
    if last_checked and last_checked + timedelta(minutes=poll_minutes) > now_dt:
        return None

    runtime["lastCheckedAt"] = _to_iso_datetime(now_dt)
    current_files = _scan_directory_files(
        str(trigger.get("directory") or ""),
        str(trigger.get("pattern") or "*"),
        bool(trigger.get("recursive", False)),
    )
    previous_files = set(runtime.get("knownFiles") or [])
    runtime["knownFiles"] = current_files[:PLANNER_MAX_KNOWN_FILES]
    plan["runtime"] = runtime

    if not previous_files:
        return None

    new_files = [path for path in current_files if path not in previous_files]
    if new_files:
        return {"kind": "file_watch", "files": new_files[:20]}
    return None


async def _due_trigger_context(plan: dict, app_config: dict, now_dt: datetime) -> Optional[dict[str, Any]]:
    trigger = plan.get("trigger") or {}
    kind = trigger.get("kind")
    if plan.get("status") != "active":
        return None

    if kind in {"once", "daily", "weekly", "interval"}:
        next_run = _parse_iso_datetime(plan.get("nextRunAt"))
        if next_run and next_run <= now_dt:
            return {"kind": kind}
        return None
    if kind == "clickhouse_watch":
        return await _evaluate_clickhouse_watch(plan, app_config, now_dt)
    if kind == "file_watch":
        return _evaluate_file_watch(plan, now_dt)
    return None


async def execute_planning_run(
    plan_id: str,
    trigger_context: Optional[dict[str, Any]] = None,
    manual: bool = False,
) -> dict[str, Any]:
    initial_state = await read_db_state()
    planning = _planning_state_from_db(initial_state)
    plan = next((item for item in planning["plans"] if item.get("id") == plan_id), None)
    if not plan:
        raise HTTPException(status_code=404, detail="Planning job not found.")
    if plan.get("status") != "active" and not manual:
        raise HTTPException(status_code=400, detail="The selected planning job is paused.")

    app_config = initial_state.get("config") or {}
    now_dt = datetime.now(timezone.utc)
    context = trigger_context or {"kind": "manual"}
    run_id = uuid.uuid4().hex
    run_record = {
        "id": run_id,
        "planId": plan.get("id"),
        "planName": plan.get("name") or "Untitled plan",
        "triggerKind": context.get("kind") or "manual",
        "triggerLabel": _planning_trigger_label(context),
        "startedAt": _to_iso_datetime(now_dt),
        "finishedAt": None,
        "status": "running",
        "summary": "",
        "outputs": [],
    }

    planning["runs"].insert(0, run_record)
    planning["runs"] = planning["runs"][:PLANNER_MAX_RUNS]
    plan["lastStatus"] = "running"
    initial_state["planning"] = planning
    await write_db_state(initial_state)

    outputs: list[dict[str, Any]] = []
    overall_status = "success"
    try:
        for agent in plan.get("agents") or []:
            try:
                if agent == "clickhouse_query":
                    output = await _run_clickhouse_planning_agent(plan, context, app_config)
                elif agent == "file_management":
                    output = await _run_file_management_planning_agent(plan, context, app_config)
                else:
                    output = await _run_local_role_agent(agent, plan, context, app_config)
            except Exception as agent_error:
                output = {
                    "agent": agent,
                    "status": "error",
                    "content": f"Agent execution failed: {agent_error}",
                }
                overall_status = "error"
            outputs.append(output)

        summary = await _summarize_planning_outputs(plan, outputs, context, app_config)
    except Exception as execution_error:
        overall_status = "error"
        summary = f"## Summary\nThe automation failed.\n\n## Trigger\n{_planning_trigger_label(context)}\n\n## Agent Outputs\n{execution_error}"

    latest_state = await read_db_state()
    latest_planning = _planning_state_from_db(latest_state)
    latest_plan = next((item for item in latest_planning["plans"] if item.get("id") == plan_id), None)
    latest_run = next((item for item in latest_planning["runs"] if item.get("id") == run_id), None)
    if not latest_plan or not latest_run:
        return {"status": overall_status, "summary": summary, "outputs": outputs}

    finished_at = _to_iso_datetime(datetime.now(timezone.utc))
    latest_run["finishedAt"] = finished_at
    latest_run["status"] = overall_status
    latest_run["summary"] = summary
    latest_run["outputs"] = outputs

    latest_plan["lastRunAt"] = finished_at
    latest_plan["lastStatus"] = overall_status
    latest_plan["lastSummary"] = summary
    if (latest_plan.get("trigger") or {}).get("kind") == "once":
        latest_plan["status"] = "paused"
        latest_plan["nextRunAt"] = None
    else:
        latest_plan["nextRunAt"] = _compute_plan_next_run_at(latest_plan, datetime.now(timezone.utc))

    latest_state["planning"] = latest_planning
    await write_db_state(latest_state)
    return {"status": overall_status, "summary": summary, "outputs": outputs}


async def process_planning_jobs() -> None:
    state = await read_db_state()
    original_snapshot = json.dumps(
        _normalize_planning_state(state.get("planning")),
        sort_keys=True,
        ensure_ascii=False,
        default=str,
    )
    planning = _planning_state_from_db(state)
    now_dt = datetime.now(timezone.utc)
    app_config = state.get("config") or {}
    due_runs: list[tuple[str, dict[str, Any]]] = []

    for plan in planning["plans"]:
        context = await _due_trigger_context(plan, app_config, now_dt)
        if context:
            due_runs.append((plan.get("id"), context))

    updated_snapshot = json.dumps(planning, sort_keys=True, ensure_ascii=False, default=str)
    if updated_snapshot != original_snapshot:
        state["planning"] = planning
        await write_db_state(state)

    for plan_id, trigger_context in due_runs:
        try:
            await execute_planning_run(plan_id, trigger_context=trigger_context, manual=False)
        except Exception:
            continue


async def planning_scheduler_loop(stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            await process_planning_jobs()
        except Exception:
            pass
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=PLANNER_LOOP_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            continue

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
    model_config = ConfigDict(populate_by_name=True)

    stage: str = "idle"
    pending_request: str = ""
    available_tables: list[str] = Field(default_factory=list)
    selected_table: Optional[str] = None
    table_schema: list[dict] = Field(
        default_factory=list,
        validation_alias=AliasChoices("schema", "table_schema"),
        serialization_alias="schema",
    )
    candidate_fields: list[str] = Field(default_factory=list)
    date_fields: list[str] = Field(default_factory=list)
    selected_field: Optional[str] = None
    selected_date_field: Optional[str] = None
    clarification_prompt: str = ""
    clarification_options: list[str] = Field(default_factory=list)
    last_sql: str = ""
    last_result_meta: list[dict] = Field(default_factory=list)
    last_result_rows: list[dict] = Field(default_factory=list)
    chart_requested: bool = False
    chart_suggested: bool = False
    chart_offer_options: list[str] = Field(default_factory=list)
    chart_x_options: list[str] = Field(default_factory=list)
    chart_y_options: list[str] = Field(default_factory=list)
    chart_type_options: list[str] = Field(default_factory=list)
    selected_chart_x: Optional[str] = None
    selected_chart_y: Optional[str] = None
    selected_chart_type: Optional[str] = None


class ClickHouseAgentRequest(BaseModel):
    message: str
    history: list[dict] = []
    clickhouse: ClickHouseConfig
    llm_base_url: str = "http://localhost:11434"
    llm_model: str = "llama3"
    llm_api_key: Optional[str] = None
    llm_provider: str = "ollama"
    agent_state: ClickHouseAgentState = Field(default_factory=ClickHouseAgentState)


class ManagerAgentStateModel(BaseModel):
    active_delegate: Optional[str] = None
    last_routing_reason: str = ""
    last_delegate_label: str = ""
    pending_pipeline: Optional[dict] = None


class FileManagerAgentConfigModel(BaseModel):
    base_path: str = ""
    max_iterations: int = 10
    system_prompt: str = DEFAULT_APP_CONFIG["fileManagerConfig"]["systemPrompt"]


class FileManagerAgentStateModel(BaseModel):
    pending_confirmation: Optional[dict] = None
    last_tool_result: str = ""
    last_visited_path: str = ""


class FileManagerAgentRequest(BaseModel):
    message: str
    history: list[dict] = []
    llm_base_url: str = "http://localhost:11434"
    llm_model: str = "llama3"
    llm_api_key: Optional[str] = None
    llm_provider: str = "ollama"
    agent_state: FileManagerAgentStateModel = Field(default_factory=FileManagerAgentStateModel)
    file_manager_config: FileManagerAgentConfigModel = Field(default_factory=FileManagerAgentConfigModel)


class DataQualityAgentStateModel(BaseModel):
    stage: str = "idle"
    table: Optional[str] = None
    columns: list[str] = Field(default_factory=list)
    sample_size: int = DATA_QUALITY_DEFAULT_SAMPLE_SIZE
    row_filter: str = ""
    time_column: Optional[str] = None
    db_type: str = "clickhouse"
    schema_info: list[dict] = Field(default_factory=list)
    column_stats: dict = Field(default_factory=dict)
    volumetric_stats: Optional[dict] = None
    llm_analysis: str = ""
    final_answer: str = ""
    agent_id: str = "data_quality_tables"
    session_id: str = ""
    last_error: str = ""
    available_tables: list[str] = Field(default_factory=list)
    available_columns: list[str] = Field(default_factory=list)
    date_columns: list[str] = Field(default_factory=list)


class DataQualityAgentRequest(BaseModel):
    message: str
    history: list[dict] = []
    clickhouse: ClickHouseConfig
    llm_base_url: str = "http://localhost:11434"
    llm_model: str = "llama3"
    llm_api_key: Optional[str] = None
    llm_provider: str = "ollama"
    agent_state: DataQualityAgentStateModel = Field(default_factory=DataQualityAgentStateModel)


class DataQualityMetadataRequest(BaseModel):
    clickhouse: ClickHouseConfig
    table: Optional[str] = None


class ManagerAgentRequest(BaseModel):
    message: str
    history: list[dict] = []
    clickhouse: ClickHouseConfig = Field(default_factory=ClickHouseConfig)
    llm_base_url: str = "http://localhost:11434"
    llm_model: str = "llama3"
    llm_api_key: Optional[str] = None
    llm_provider: str = "ollama"
    system_prompt: str = DEFAULT_APP_CONFIG["systemPrompt"]
    manager_state: ManagerAgentStateModel = Field(default_factory=ManagerAgentStateModel)
    clickhouse_state: ClickHouseAgentState = Field(default_factory=ClickHouseAgentState)
    file_manager_state: FileManagerAgentStateModel = Field(default_factory=FileManagerAgentStateModel)
    data_quality_state: DataQualityAgentStateModel = Field(default_factory=DataQualityAgentStateModel)
    file_manager_config: FileManagerAgentConfigModel = Field(default_factory=FileManagerAgentConfigModel)


class PlanningAgentStateModel(BaseModel):
    draft: dict = Field(default_factory=lambda: _default_planning_draft("UTC"))
    missing_fields: list[str] = Field(default_factory=list)
    last_question: str = ""
    ready_to_review: bool = False


class PlanningChatRequest(BaseModel):
    message: str
    history: list[dict] = []
    llm_base_url: str = "http://localhost:11434"
    llm_model: str = "llama3"
    llm_api_key: Optional[str] = None
    llm_provider: str = "ollama"
    agent_state: PlanningAgentStateModel = Field(default_factory=PlanningAgentStateModel)


class PlanningPlanRequest(BaseModel):
    plan: dict


class PlanningPlanStatusRequest(BaseModel):
    status: str


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
    existing_state = await read_db_state()
    payload = {
        "schemaVersion": existing_state.get("schemaVersion", 1),
        "config": req.config,
        "conversations": req.conversations,
        "preferences": req.preferences.model_dump(),
        "planning": existing_state.get("planning", _default_planning_state()),
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


@app.get("/api/planning/state")
async def get_planning_state():
    state = await read_db_state()
    planning = _planning_state_from_db(state)
    if json.dumps(planning, sort_keys=True, ensure_ascii=False, default=str) != json.dumps(
        _normalize_planning_state(state.get("planning")),
        sort_keys=True,
        ensure_ascii=False,
        default=str,
    ):
        state["planning"] = planning
        await write_db_state(state)
    return planning


@app.post("/api/planning/plans")
async def upsert_planning_plan(req: PlanningPlanRequest):
    normalized_plan = _normalize_planning_plan(req.plan)
    if not normalized_plan.get("prompt"):
        raise HTTPException(status_code=400, detail="A planning job objective is required.")
    if not normalized_plan.get("agents"):
        raise HTTPException(status_code=400, detail="Select at least one existing agent.")

    trigger = normalized_plan.get("trigger") or {}
    kind = trigger.get("kind")
    if kind == "clickhouse_watch" and not is_safe_read_only_sql(trigger.get("watchSql") or ""):
        raise HTTPException(status_code=400, detail="The ClickHouse watch SQL must be a safe read-only query.")
    if kind == "file_watch" and not trigger.get("directory"):
        raise HTTPException(status_code=400, detail="A directory is required for file watch jobs.")

    state = await read_db_state()
    planning = _planning_state_from_db(state)
    now_iso = _utc_now_iso()
    existing_plan = next((plan for plan in planning["plans"] if plan.get("id") == normalized_plan["id"]), None)
    if existing_plan:
        normalized_plan["createdAt"] = existing_plan.get("createdAt") or now_iso
    else:
        normalized_plan["createdAt"] = now_iso
    normalized_plan["updatedAt"] = now_iso
    normalized_plan["nextRunAt"] = _compute_plan_next_run_at(normalized_plan, datetime.now(timezone.utc))
    normalized_plan["runtime"] = existing_plan.get("runtime") if existing_plan else _default_planning_runtime()
    if kind in {"clickhouse_watch", "file_watch"}:
        normalized_plan["nextRunAt"] = None

    if existing_plan:
        index = planning["plans"].index(existing_plan)
        planning["plans"][index] = normalized_plan
    else:
        planning["plans"].insert(0, normalized_plan)

    state["planning"] = planning
    await write_db_state(state)
    return planning


@app.post("/api/planning/plans/{plan_id}/status")
async def set_planning_plan_status(plan_id: str, req: PlanningPlanStatusRequest):
    if req.status not in {"active", "paused"}:
        raise HTTPException(status_code=400, detail="Invalid planning job status.")

    state = await read_db_state()
    planning = _planning_state_from_db(state)
    plan = next((item for item in planning["plans"] if item.get("id") == plan_id), None)
    if not plan:
        raise HTTPException(status_code=404, detail="Planning job not found.")

    plan["status"] = req.status
    plan["updatedAt"] = _utc_now_iso()
    plan["nextRunAt"] = _compute_plan_next_run_at(plan, datetime.now(timezone.utc))
    if (plan.get("trigger") or {}).get("kind") in {"clickhouse_watch", "file_watch"}:
        plan["nextRunAt"] = None

    state["planning"] = planning
    await write_db_state(state)
    return planning


@app.delete("/api/planning/plans/{plan_id}")
async def delete_planning_plan(plan_id: str):
    state = await read_db_state()
    planning = _planning_state_from_db(state)
    existing = next((item for item in planning["plans"] if item.get("id") == plan_id), None)
    if not existing:
        raise HTTPException(status_code=404, detail="Planning job not found.")

    planning["plans"] = [plan for plan in planning["plans"] if plan.get("id") != plan_id]
    state["planning"] = planning
    await write_db_state(state)
    return planning


@app.post("/api/planning/plans/{plan_id}/run")
async def run_planning_plan_now(plan_id: str):
    await execute_planning_run(plan_id, trigger_context={"kind": "manual"}, manual=True)
    state = await read_db_state()
    return _planning_state_from_db(state)


@app.post("/api/chat/crewai-planning")
async def chat_crewai_planning(req: PlanningChatRequest):
    user_message = (req.message or "").strip()
    timezone_name = str(
        ((req.agent_state.draft or {}).get("trigger") or {}).get("timezone") or "UTC"
    )
    current_draft = _merge_planning_draft(req.agent_state.draft, None, timezone_name)
    planning_state = _planning_state_from_db(await read_db_state())
    open_form_action = {
        "id": "open-planning-form",
        "label": "Open planning form",
        "actionType": "open_planning_form",
        "variant": "primary",
    }

    if not user_message:
        missing_fields = _validate_planning_draft(current_draft)
        return {
            "answer": (
                "## CrewAI Planning\n"
                "Describe the automation you want in natural language, or open the planner form "
                "to configure triggers, existing agents, and monitoring rules step by step."
            ),
            "agent_state": {
                "draft": current_draft,
                "missing_fields": missing_fields,
                "last_question": "",
                "ready_to_review": len(missing_fields) == 0,
            },
            "actions": [open_form_action],
            "steps": [
                {
                    "id": "planning-ready",
                    "title": "Planner ready",
                    "status": "success",
                    "details": "The planner can work from natural language or from the full-screen form.",
                }
            ],
        }

    lowered = user_message.lower()
    if any(token in lowered for token in ["reset", "start over", "clear draft"]):
        fresh_draft = _default_planning_draft(timezone_name)
        return {
            "answer": "## CrewAI Planning\nThe planning draft has been reset. Tell me what you want to automate, or open the form.",
            "agent_state": {
                "draft": fresh_draft,
                "missing_fields": _validate_planning_draft(fresh_draft),
                "last_question": "",
                "ready_to_review": False,
            },
            "actions": [open_form_action],
            "steps": [
                {
                    "id": "planning-reset",
                    "title": "Reset planning draft",
                    "status": "success",
                    "details": "The previous draft was cleared.",
                }
            ],
        }

    if any(token in lowered for token in ["list plans", "show plans", "existing plans", "what plans"]):
        return {
            "answer": _planning_state_markdown(planning_state),
            "agent_state": {
                "draft": current_draft,
                "missing_fields": _validate_planning_draft(current_draft),
                "last_question": "",
                "ready_to_review": len(_validate_planning_draft(current_draft)) == 0,
            },
            "actions": [open_form_action],
            "steps": [
                {
                    "id": "planning-list",
                    "title": "Loaded existing plans",
                    "status": "success",
                    "details": f"Found {len(planning_state.get('plans', []))} saved planning job(s).",
                }
            ],
        }

    if any(token in lowered for token in ["open form", "open planner", "planner form", "show form"]):
        missing_fields = _validate_planning_draft(current_draft)
        return {
            "answer": _planning_summary_markdown(current_draft, missing_fields),
            "agent_state": {
                "draft": current_draft,
                "missing_fields": missing_fields,
                "last_question": "",
                "ready_to_review": len(missing_fields) == 0,
            },
            "actions": [open_form_action],
            "steps": [
                {
                    "id": "planning-open-form",
                    "title": "Prepared planner form",
                    "status": "success",
                    "details": "The current draft is ready to be reviewed in the planner form.",
                }
            ],
        }

    analysis = await analyze_planning_request(
        user_message,
        current_draft,
        req.llm_base_url,
        req.llm_model,
        req.llm_provider,
        req.llm_api_key,
    )
    merged_draft = _merge_planning_draft(current_draft, analysis.get("draft"), timezone_name)
    missing_fields = _validate_planning_draft(merged_draft)
    ready_to_review = len(missing_fields) == 0
    clarification_question = (
        analysis.get("clarification_question")
        or (
            f"Please tell me { _planning_missing_prompt(missing_fields) }."
            if missing_fields else
            "The draft is ready. Open the planner form to review and save it."
        )
    )
    answer = (
        f"{_planning_summary_markdown(merged_draft, missing_fields)}\n\n"
        "## Guidance\n"
        f"{clarification_question}"
    )

    return {
        "answer": answer,
        "agent_state": {
            "draft": merged_draft,
            "missing_fields": missing_fields,
            "last_question": clarification_question,
            "ready_to_review": ready_to_review,
        },
        "actions": [open_form_action],
        "steps": [
            {
                "id": "planning-parse",
                "title": "Parsed planning request",
                "status": "success",
                "details": analysis.get("reasoning") or "The local LLM extracted the planning intent into a structured draft.",
            },
            {
                "id": "planning-review",
                "title": "Draft readiness",
                "status": "success" if ready_to_review else "running",
                "details": (
                    "The draft is complete and ready for form review."
                    if ready_to_review
                    else f"Still missing { _planning_missing_prompt(missing_fields) }."
                ),
            },
        ],
    }


@app.post("/api/chat/file-manager-agent")
async def chat_file_manager_agent(req: FileManagerAgentRequest):
    user_message = (req.message or "").strip()
    state = _normalize_file_manager_state(req.agent_state.model_dump())
    config = _normalize_file_manager_config(req.file_manager_config.model_dump())
    normalized_choice = normalize_choice(user_message).lower()
    export_payload = _try_extract_file_export_payload(user_message)

    if not user_message:
        return {
            "answer": (
                "## File Management Agent\n"
                "Ask me to inspect, search, create, edit, move, or delete files.\n\n"
                f"Current sandbox base path: `{config['basePath'] or 'not restricted'}`"
            ),
            "agent_state": state,
            "steps": [
                {
                    "id": "fm-ready",
                    "title": "Ready for file operations",
                    "status": "success",
                    "details": "The agent is ready to reason over filesystem tasks with tool calls.",
                }
                ],
            }

    if export_payload:
        if not export_payload.get("path"):
            return {
                "answer": (
                    "## File Export\n"
                    "I have the dataset ready, but I still need the target file name or path.\n\n"
                    f"Requested format: **{export_payload.get('format')}**"
                ),
                "agent_state": state,
                "steps": [
                    {
                        "id": "fm-export-path",
                        "title": "Waiting for export path",
                        "status": "running",
                        "details": "The export payload is ready, but the target file path is missing.",
                    }
                ],
            }

        try:
            if export_payload["format"] in {"csv", "tsv"}:
                content = _serialize_delimited_rows(
                    export_payload.get("headers") or [],
                    export_payload.get("rows") or [],
                    "\t" if export_payload["format"] == "tsv" else ",",
                )
                target = _resolve_agent_path(export_payload["path"], config["basePath"])
                result = (
                    write_file_tool(export_payload["path"], content, confirmed=False, base_path=config["basePath"])
                    if target.exists()
                    else create_file_tool(export_payload["path"], content, base_path=config["basePath"])
                )
            else:
                target = _resolve_agent_path(export_payload["path"], config["basePath"])
                result = (
                    write_excel_sheet_tool(
                        export_payload["path"],
                        export_payload.get("sheet_name") or "Results",
                        headers=export_payload.get("headers") or [],
                        rows=export_payload.get("rows") or [],
                        confirmed=False,
                        base_path=config["basePath"],
                    )
                    if target.exists()
                    else create_excel_file_tool(
                        export_payload["path"],
                        sheet_name=export_payload.get("sheet_name") or "Results",
                        headers=export_payload.get("headers") or [],
                        rows=export_payload.get("rows") or [],
                        base_path=config["basePath"],
                    )
                )
        except Exception as exc:
            return {
                "answer": f"## File Export\nI could not prepare the export.\n\n```text\n{exc}\n```",
                "agent_state": state,
                "steps": [
                    {
                        "id": "fm-export-error",
                        "title": "Export failed",
                        "status": "error",
                        "details": str(exc),
                    }
                ],
            }

        state["last_tool_result"] = result["summary"]
        state["last_visited_path"] = result.get("visited_path") or state.get("last_visited_path", "")
        if result.get("requires_confirmation"):
            pending_action = dict(result.get("pending_action") or {})
            state["pending_confirmation"] = {
                "tool_name": pending_action.get("tool_name"),
                "tool_input": pending_action.get("tool_input") or {},
                "preview": result.get("preview") or "",
                "summary": result.get("summary") or "",
                "requested_at": _utc_now_iso(),
            }
            answer, actions = _file_manager_confirmation_answer(state)
            return {
                "answer": answer,
                "actions": actions,
                "agent_state": state,
                "steps": [
                    {
                        "id": "fm-export-confirm",
                        "title": "Confirmation required",
                        "status": "running",
                        "details": result["summary"],
                    }
                ],
            }

        return {
            "answer": _file_export_answer(result, export_payload),
            "agent_state": state,
            "steps": [
                {
                    "id": "fm-export-dataset",
                    "title": "Exported ClickHouse result",
                    "status": "success",
                    "details": result["summary"],
                }
            ],
        }

    if state.get("pending_confirmation"):
        if is_negative_response(user_message) or normalized_choice in {"cancel", "cancel file action"}:
            state["pending_confirmation"] = None
            return {
                "answer": "## Answer\nThe pending file operation was cancelled.",
                "agent_state": state,
                "steps": [
                    {
                        "id": "fm-cancel",
                        "title": "Cancelled pending action",
                        "status": "success",
                        "details": "The destructive or overwrite operation was not executed.",
                    }
                ],
            }

        if is_affirmative_response(user_message) or normalized_choice in {"confirm", "confirm file action"}:
            pending = state["pending_confirmation"] or {}
            tool_name = str(pending.get("tool_name") or "").strip()
            tool_input = dict(pending.get("tool_input") or {})
            tool_input["confirmed"] = True
            try:
                result = execute_file_manager_tool(tool_name, tool_input, config["basePath"])
            except Exception as exc:
                state["pending_confirmation"] = None
                return {
                    "answer": f"## Answer\nI could not complete the confirmed action.\n\n```text\n{exc}\n```",
                    "agent_state": state,
                    "steps": [
                        {
                            "id": "fm-confirm-error",
                            "title": "Confirmed action failed",
                            "status": "error",
                            "details": str(exc),
                        }
                    ],
                }

            state["pending_confirmation"] = None
            state["last_tool_result"] = result["summary"]
            state["last_visited_path"] = result.get("visited_path") or state.get("last_visited_path", "")
            preview = result.get("preview") or ""
            answer = f"## Answer\n{result['summary']}"
            if preview:
                answer += f"\n\n## Preview\n{preview}"
            return {
                "answer": answer,
                "agent_state": state,
                "steps": [
                    {
                        "id": "fm-confirmed-action",
                        "title": f"Executed `{tool_name}`",
                        "status": "success",
                        "details": result["summary"],
                    }
                ],
            }

        answer, actions = _file_manager_confirmation_answer(state)
        return {
            "answer": answer,
            "actions": actions,
            "agent_state": state,
            "steps": [
                {
                    "id": "fm-await-confirmation",
                    "title": "Waiting for confirmation",
                    "status": "running",
                    "details": "The requested file operation needs explicit user confirmation.",
                }
            ],
        }

    scratchpad: list[dict[str, Any]] = []
    steps: list[dict[str, Any]] = []
    last_error = ""
    max_iterations = max(1, min(FILE_MANAGER_MAX_ITERATIONS, int(config["maxIterations"])))

    for iteration in range(max_iterations):
        planned = await plan_file_manager_step(
            user_message,
            req.history,
            scratchpad,
            config["basePath"],
            config["systemPrompt"],
            req.llm_base_url,
            req.llm_model,
            req.llm_provider,
            req.llm_api_key,
        )
        reasoning = planned.get("reasoning") or "The local LLM selected the next best action."
        action = planned.get("action")

        if action == "final":
            final_answer = planned.get("final_answer") or "## Answer\nThe file-management task is complete."
            steps.append(
                {
                    "id": f"fm-final-{iteration}",
                    "title": "Prepared final answer",
                    "status": "success",
                    "details": reasoning,
                }
            )
            state["last_tool_result"] = final_answer
            return {
                "answer": final_answer,
                "agent_state": state,
                "steps": steps,
            }

        tool_name = planned.get("tool_name") or ""
        tool_input = dict(planned.get("tool_input") or {})
        if action != "tool" or tool_name not in FILE_MANAGER_TOOLS:
            last_error = "The planner returned an invalid tool action."
            scratchpad.append({"type": "error", "error": last_error})
            steps.append(
                {
                    "id": f"fm-invalid-{iteration}",
                    "title": "Planner action invalid",
                    "status": "error",
                    "details": last_error,
                }
            )
            continue

        if tool_name in FILE_MANAGER_CONFIRMATION_TOOLS and "confirmed" not in tool_input:
            tool_input["confirmed"] = False

        try:
            result = execute_file_manager_tool(tool_name, tool_input, config["basePath"])
        except Exception as exc:
            last_error = str(exc)
            scratchpad.append(
                {
                    "type": "tool_error",
                    "tool": tool_name,
                    "input": tool_input,
                    "error": last_error,
                }
            )
            steps.append(
                {
                    "id": f"fm-tool-error-{iteration}",
                    "title": f"Tool `{tool_name}` failed",
                    "status": "error",
                    "details": last_error,
                }
            )
            continue

        state["last_tool_result"] = result["summary"]
        state["last_visited_path"] = result.get("visited_path") or state.get("last_visited_path", "")
        scratchpad.append(
            {
                "type": "tool_result",
                "tool": tool_name,
                "input": tool_input,
                "summary": result["summary"],
                "preview": result.get("preview") or "",
            }
        )
        steps.append(
            {
                "id": f"fm-tool-{iteration}",
                "title": f"Used `{tool_name}`",
                "status": "success",
                "details": result["summary"],
            }
        )

        if result.get("requires_confirmation"):
            pending_action = dict(result.get("pending_action") or {})
            state["pending_confirmation"] = {
                "tool_name": pending_action.get("tool_name"),
                "tool_input": pending_action.get("tool_input") or {},
                "preview": result.get("preview") or "",
                "summary": result.get("summary") or "",
                "requested_at": _utc_now_iso(),
            }
            answer, actions = _file_manager_confirmation_answer(state)
            steps.append(
                {
                    "id": f"fm-confirm-{iteration}",
                    "title": "Confirmation required",
                    "status": "running",
                    "details": result["summary"],
                }
            )
            return {
                "answer": answer,
                "actions": actions,
                "agent_state": state,
                "steps": steps,
            }

    answer = (
        "## Answer\n"
        "I reached the file-management iteration limit before I could finish the task."
    )
    if last_error:
        answer += f"\n\n```text\n{last_error}\n```"
    return {
        "answer": answer,
        "agent_state": state,
        "steps": steps or [
            {
                "id": "fm-timeout",
                "title": "Reached iteration limit",
                "status": "error",
                "details": "The agent stopped to avoid an infinite loop.",
            }
        ],
    }


@app.post("/api/data-quality/options")
async def get_data_quality_options(req: DataQualityMetadataRequest):
    state = _default_data_quality_state()
    table_name = str(req.table or "").strip()
    if table_name:
        state["table"] = table_name

    try:
        state = await data_quality_schema_node(req.clickhouse, state)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Data-quality metadata loading failed: {exc}") from exc

    return {
        "available_tables": state.get("available_tables") or [],
        "schema_info": state.get("schema_info") or [],
        "available_columns": state.get("available_columns") or [],
        "date_columns": state.get("date_columns") or [],
    }


@app.post("/api/chat/data-quality-agent")
async def chat_data_quality_agent(req: DataQualityAgentRequest):
    user_message = (req.message or "").strip()
    normalized_choice = normalize_choice(user_message)
    normalized_lower = normalized_choice.lower()
    state = _normalize_data_quality_state(req.agent_state.model_dump())
    state["agent_id"] = "data_quality_tables"
    state["db_type"] = "clickhouse"
    state["session_id"] = state.get("session_id") or uuid.uuid4().hex
    state["last_error"] = ""

    async def _reload_tables(current_state: dict[str, Any]) -> dict[str, Any]:
        try:
            return await data_quality_schema_node(req.clickhouse, current_state)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"ClickHouse schema discovery failed: {exc}") from exc

    async def _run_data_quality_analysis(current_state: dict[str, Any]) -> dict[str, Any]:
        execution_state = dict(current_state)
        steps = [
            {
                "id": "dq-schema-node",
                "title": "schema_node",
                "status": "success",
                "details": f"Loaded schema metadata for `{execution_state.get('table')}`.",
            }
        ]

        execution_state = await data_quality_stats_node(req.clickhouse, execution_state)
        steps.append(
            {
                "id": "dq-stats-node",
                "title": "stats_node",
                "status": "success",
                "details": f"Profiled {len(execution_state.get('columns') or [])} column(s) with statistical SQL.",
            }
        )

        if execution_state.get("time_column"):
            execution_state = await data_quality_volumetric_node(req.clickhouse, execution_state)
            steps.append(
                {
                    "id": "dq-volumetric-node",
                    "title": "volumetric_node",
                    "status": "success",
                    "details": (
                        "Computed volumetric patterns for the selected time column."
                        if execution_state.get("volumetric_stats")
                        else "Volumetric analysis was requested, but no usable time buckets were found."
                    ),
                }
            )

        llm_analysis = await data_quality_llm_analysis_node(
            execution_state,
            req.llm_base_url,
            req.llm_model,
            req.llm_provider,
            req.llm_api_key,
        )
        steps.append(
            {
                "id": "dq-llm-analysis-node",
                "title": "llm_analysis_node",
                "status": "success",
                "details": "The local LLM scored the dataset and generated recommendations.",
            }
        )

        execution_state["final_answer"] = data_quality_synthesizer_node(execution_state, llm_analysis)
        execution_state["stage"] = "ready"
        execution_state["last_error"] = ""
        steps.append(
            {
                "id": "dq-synthesizer-node",
                "title": "synthesizer_node",
                "status": "success",
                "details": "Built the final Markdown report in English.",
            }
        )
        table_slug = re.sub(r"[^a-z0-9]+", "-", str(execution_state.get("table") or "data-quality").strip().lower()).strip("-") or "data-quality"
        return {
            "answer": execution_state["final_answer"],
            "agent_state": execution_state,
            "steps": steps,
            "actions": [
                {
                    "id": "dq-export-pdf",
                    "label": "Export Summary PDF",
                    "actionType": "export_data_quality_pdf",
                    "variant": "secondary",
                    "payload": {
                        "fileName": f"data-quality-{table_slug}.pdf",
                        "title": f"Data Quality Summary - {execution_state.get('table') or 'Table'}",
                    },
                }
            ],
        }

    start_over_requested = any(
        token in normalized_lower for token in ["start over", "reset", "new analysis", "clear analysis"]
    )
    if start_over_requested:
        state = _default_data_quality_state()
        state["agent_id"] = "data_quality_tables"
        state["db_type"] = "clickhouse"
        state["session_id"] = uuid.uuid4().hex

    state = await _reload_tables(state)
    if not state.get("available_tables"):
        raise HTTPException(status_code=400, detail="No tables were found in the configured ClickHouse database.")

    direct_payload = _try_extract_data_quality_payload(user_message)
    if direct_payload:
        table_name = str(direct_payload.get("table") or "").strip()
        if not table_name:
            raise HTTPException(status_code=400, detail="The structured data-quality payload must include a table.")
        matched_table = _data_quality_guess_table_from_message(table_name, state["available_tables"])
        if not matched_table:
            raise HTTPException(status_code=400, detail=f"Unknown table in the data-quality payload: {table_name}")

        state["table"] = matched_table
        payload_sample_size = direct_payload.get("sample_size")
        if isinstance(payload_sample_size, (int, float)):
            state["sample_size"] = max(0, min(DATA_QUALITY_MAX_SAMPLE_ROWS, int(payload_sample_size)))
        else:
            state["sample_size"] = DATA_QUALITY_DEFAULT_SAMPLE_SIZE
        state["row_filter"] = str(direct_payload.get("row_filter") or "").strip()
        validation_error = _validate_data_quality_row_filter(state["row_filter"])
        if validation_error:
            raise HTTPException(status_code=400, detail=validation_error)

        state = await _reload_tables(state)
        requested_columns = direct_payload.get("columns")
        if isinstance(requested_columns, list) and requested_columns:
            state["columns"] = _match_data_quality_columns(
                [str(item).strip() for item in requested_columns if isinstance(item, str)],
                state["schema_info"],
            )
            if not state["columns"]:
                raise HTTPException(status_code=400, detail="None of the requested columns were found in the selected table.")
        else:
            state["columns"] = [column["name"] for column in state.get("schema_info") or [] if column.get("name")]
            if not state["columns"]:
                raise HTTPException(status_code=400, detail="The selected table has no readable columns.")

        requested_time_column = str(direct_payload.get("time_column") or "").strip()
        if requested_time_column:
            matched_time = resolve_user_choice(requested_time_column, state.get("date_columns") or [])
            if not matched_time:
                raise HTTPException(status_code=400, detail="The structured time_column must match a date-like column in the selected table.")
            state["time_column"] = matched_time
        else:
            state["time_column"] = None

        return await _run_data_quality_analysis(state)

    if state.get("stage") == "ready" and user_message:
        state = _default_data_quality_state()
        state["agent_id"] = "data_quality_tables"
        state["db_type"] = "clickhouse"
        state["session_id"] = uuid.uuid4().hex
        state = await _reload_tables(state)

    table_options = _data_quality_table_options(state)

    if not user_message or normalized_lower in {"start guided setup", "guide me", "guided setup", "start setup", "start"}:
        state["stage"] = "awaiting_table"
        answer = append_choice_markdown(
            _data_quality_intro_markdown(req.clickhouse.database, table_options, len(state["available_tables"])),
            "Table Selection",
            "Choose the table you want to profile first.",
            table_options,
        )
        return {
            "answer": answer,
            "agent_state": state,
            "steps": _data_quality_agent_steps(
                "dq-guided-start",
                "Started guided setup",
                "running",
                f"Loaded {len(state['available_tables'])} table(s) and waiting for the table selection.",
            ),
        }

    if not state.get("table"):
        guessed_table = _data_quality_guess_table_from_message(user_message, state["available_tables"])
        if guessed_table:
            state["table"] = guessed_table
            state["stage"] = "awaiting_columns_mode"
            state["columns"] = []
            state["column_stats"] = {}
            state["volumetric_stats"] = None
            state["llm_analysis"] = ""
            state["final_answer"] = ""
            state["time_column"] = None
            state = await _reload_tables(state)
        else:
            state["stage"] = "awaiting_table"
            answer = append_choice_markdown(
                _data_quality_intro_markdown(req.clickhouse.database, table_options, len(state["available_tables"])),
                "Table Selection",
                "Choose the table you want to profile first.",
                table_options,
            )
            return {
                "answer": answer,
                "agent_state": state,
                "steps": _data_quality_agent_steps(
                    "dq-await-table",
                    "Waiting for table",
                    "running",
                    "The agent needs the target table before it can configure profiling parameters.",
                ),
            }

    if state.get("table") and not state.get("schema_info"):
        state = await _reload_tables(state)
    if state.get("table") and not state.get("schema_info"):
        raise HTTPException(status_code=400, detail=f"Table '{state['table']}' has no readable schema.")

    if state.get("stage") == "awaiting_table":
        selected_table = _data_quality_guess_table_from_message(user_message, state["available_tables"])
        if not selected_table:
            return {
                "answer": append_choice_markdown(
                    _data_quality_intro_markdown(req.clickhouse.database, table_options, len(state["available_tables"])),
                    "Table Selection",
                    "Choose the table you want to profile first.",
                    table_options,
                ),
                "agent_state": state,
                "steps": _data_quality_agent_steps(
                    "dq-await-table",
                    "Waiting for table",
                    "running",
                    "The selected agent is waiting for the table choice.",
                ),
            }
        state["table"] = selected_table
        state["stage"] = "awaiting_columns_mode"
        state["columns"] = []
        state["column_stats"] = {}
        state["volumetric_stats"] = None
        state["llm_analysis"] = ""
        state["final_answer"] = ""
        state["time_column"] = None
        state = await _reload_tables(state)

    guessed_columns = []
    if state.get("stage") in {"idle", "awaiting_columns_mode"}:
        guessed_columns = _data_quality_guess_columns_from_message(user_message, state.get("schema_info") or [])
        if guessed_columns and not state.get("columns"):
            state["columns"] = guessed_columns
            state["stage"] = "awaiting_sample_size"

    if state.get("stage") == "awaiting_columns_mode" and not state.get("columns"):
        column_mode_options = _data_quality_column_mode_options(state)
        selected_mode = resolve_user_choice(user_message, column_mode_options)
        if selected_mode == DATA_QUALITY_CUSTOM_COLUMNS_OPTION:
            state["stage"] = "awaiting_custom_columns"
            preview_columns = (state.get("available_columns") or [])[:20]
            answer = (
                "## Custom Column Selection\n"
                "Type the exact column names separated by commas or new lines.\n\n"
                "Available columns preview:\n"
                + "\n".join(f"- `{column}`" for column in preview_columns)
            )
            return {
                "answer": answer,
                "agent_state": state,
                "steps": _data_quality_agent_steps(
                    "dq-custom-columns",
                    "Waiting for custom columns",
                    "running",
                    "The user chose to provide a custom column list.",
                ),
            }

        if not selected_mode and not guessed_columns:
            return {
                "answer": build_choice_markdown(
                    "Column Scope",
                    f"I loaded `{len(state.get('available_columns') or [])}` column(s) from `{state['table']}`. Choose the profiling scope.",
                    column_mode_options,
                ),
                "agent_state": state,
                "steps": _data_quality_agent_steps(
                    "dq-columns-mode",
                    "Waiting for column scope",
                    "running",
                    "The agent is waiting for the user to define which columns should be profiled.",
                ),
            }

        if selected_mode:
            selected_columns = _data_quality_columns_for_mode(selected_mode, state.get("schema_info") or [])
            if not selected_columns:
                return {
                    "answer": (
                        "## Column Scope\n"
                        "The selected scope did not resolve to any column in this table. Please choose another option."
                    ),
                    "agent_state": state,
                    "steps": _data_quality_agent_steps(
                        "dq-columns-empty",
                        "Column scope empty",
                        "error",
                        "The selected column category returned no usable column.",
                    ),
                }
            state["columns"] = selected_columns
            state["stage"] = "awaiting_sample_size"

    if state.get("stage") == "awaiting_custom_columns":
        matched_columns = _match_data_quality_columns(
            _parse_custom_column_input(user_message),
            state.get("schema_info") or [],
        )
        if not matched_columns:
            preview_columns = (state.get("available_columns") or [])[:20]
            answer = (
                "## Custom Column Selection\n"
                "I could not match those names to the selected table. Please type exact column names separated by commas or new lines.\n\n"
                "Available columns preview:\n"
                + "\n".join(f"- `{column}`" for column in preview_columns)
            )
            return {
                "answer": answer,
                "agent_state": state,
                "steps": _data_quality_agent_steps(
                    "dq-custom-columns",
                    "Waiting for custom columns",
                    "running",
                    "The provided custom columns did not match the table schema.",
                ),
            }
        state["columns"] = matched_columns
        state["stage"] = "awaiting_sample_size"

    if state.get("stage") == "awaiting_sample_size":
        sample_options = list(DATA_QUALITY_SAMPLE_OPTIONS.keys()) + [DATA_QUALITY_CUSTOM_SAMPLE_OPTION]
        selected_sample = resolve_user_choice(user_message, sample_options)
        if not selected_sample:
            number_match = re.search(r"\b(\d[\d\s_,]*)\b", user_message)
            if number_match:
                parsed_number = int(re.sub(r"[^\d]", "", number_match.group(1)))
                state["sample_size"] = max(0, min(DATA_QUALITY_MAX_SAMPLE_ROWS, parsed_number))
                state["stage"] = "awaiting_row_filter_mode"
                return {
                    "answer": build_choice_markdown(
                        "Row Filter",
                        "Choose whether to profile all rows or enter a manual row filter.",
                        [DATA_QUALITY_SKIP_ROW_FILTER_OPTION, DATA_QUALITY_ENTER_ROW_FILTER_OPTION],
                    ),
                    "agent_state": state,
                    "steps": _data_quality_agent_steps(
                        "dq-row-filter-mode",
                        "Waiting for row filter mode",
                        "running",
                        "The sampling strategy is set, and the agent is waiting for the optional row-filter choice.",
                    ),
                }
            else:
                return {
                    "answer": build_choice_markdown(
                        "Sample Size",
                        "Choose how many rows should be profiled. Full scans are safety-capped.",
                        sample_options,
                    ),
                    "agent_state": state,
                    "steps": _data_quality_agent_steps(
                        "dq-sample",
                        "Waiting for sample size",
                        "running",
                        "The agent is waiting for the sampling strategy.",
                    ),
                }
        elif selected_sample == DATA_QUALITY_CUSTOM_SAMPLE_OPTION:
            state["stage"] = "awaiting_custom_sample_size"
            return {
                "answer": (
                    "## Custom Sample Size\n"
                    f"Type the number of rows to profile. Use `0` for a capped full scan up to {DATA_QUALITY_MAX_SAMPLE_ROWS:,} rows."
                ),
                "agent_state": state,
                "steps": _data_quality_agent_steps(
                    "dq-custom-sample",
                    "Waiting for custom sample size",
                    "running",
                    "The user chose to enter a custom sample size.",
                ),
            }
        else:
            state["sample_size"] = DATA_QUALITY_SAMPLE_OPTIONS[selected_sample]
            state["stage"] = "awaiting_row_filter_mode"
            return {
                "answer": build_choice_markdown(
                    "Row Filter",
                    "Choose whether to profile all rows or enter a manual row filter.",
                    [DATA_QUALITY_SKIP_ROW_FILTER_OPTION, DATA_QUALITY_ENTER_ROW_FILTER_OPTION],
                ),
                "agent_state": state,
                "steps": _data_quality_agent_steps(
                    "dq-row-filter-mode",
                    "Waiting for row filter mode",
                    "running",
                    "The sampling strategy is set, and the agent is waiting for the optional row-filter choice.",
                ),
            }

    if state.get("stage") == "awaiting_custom_sample_size":
        number_match = re.search(r"\b(\d[\d\s_,]*)\b", user_message)
        if not number_match and normalized_lower not in {"0", "full scan"}:
            return {
                "answer": (
                    "## Custom Sample Size\n"
                    f"Please enter a numeric row count. Use `0` for a capped full scan up to {DATA_QUALITY_MAX_SAMPLE_ROWS:,} rows."
                ),
                "agent_state": state,
                "steps": _data_quality_agent_steps(
                    "dq-custom-sample",
                    "Waiting for custom sample size",
                    "running",
                    "The custom sample size must be numeric.",
                ),
            }
        parsed_number = 0 if normalized_lower in {"0", "full scan"} else int(re.sub(r"[^\d]", "", number_match.group(1)))
        state["sample_size"] = max(0, min(DATA_QUALITY_MAX_SAMPLE_ROWS, parsed_number))
        state["stage"] = "awaiting_row_filter_mode"
        return {
            "answer": build_choice_markdown(
                "Row Filter",
                "Choose whether to profile all rows or enter a manual row filter.",
                [DATA_QUALITY_SKIP_ROW_FILTER_OPTION, DATA_QUALITY_ENTER_ROW_FILTER_OPTION],
            ),
            "agent_state": state,
            "steps": _data_quality_agent_steps(
                "dq-row-filter-mode",
                "Waiting for row filter mode",
                "running",
                "The custom sampling strategy is set, and the agent is waiting for the optional row-filter choice.",
            ),
        }

    if state.get("stage") == "awaiting_row_filter_mode":
        row_filter_options = [DATA_QUALITY_SKIP_ROW_FILTER_OPTION, DATA_QUALITY_ENTER_ROW_FILTER_OPTION]
        selected_mode = resolve_user_choice(user_message, row_filter_options)

        if not selected_mode and normalized_lower in {"skip", "no filter", "without filter", "none", "all rows"}:
            selected_mode = DATA_QUALITY_SKIP_ROW_FILTER_OPTION

        if selected_mode == DATA_QUALITY_SKIP_ROW_FILTER_OPTION:
            state["row_filter"] = ""
            state["stage"] = "awaiting_time_column" if state.get("date_columns") else "awaiting_review"

            if state.get("stage") == "awaiting_time_column":
                time_options = [DATA_QUALITY_SKIP_TIME_OPTION] + list(state.get("date_columns") or [])
                return {
                    "answer": build_choice_markdown(
                        "Volumetric Analysis",
                        "Choose a time column if you also want a volume-over-time analysis.",
                        time_options,
                    ),
                    "agent_state": state,
                    "steps": _data_quality_agent_steps(
                        "dq-time-column",
                        "Waiting for time column",
                        "running",
                        "The row filter was skipped, and the agent is waiting for the optional volumetric-analysis choice.",
                    ),
                }

            return {
                "answer": append_choice_markdown(
                    _data_quality_review_markdown(state),
                    "Review Actions",
                    "Choose the next action for this data-quality run.",
                    DATA_QUALITY_REVIEW_OPTIONS,
                ),
                "agent_state": state,
                "steps": _data_quality_agent_steps(
                    "dq-review",
                    "Ready to launch",
                    "running",
                    "The row filter choice is complete, and the run is ready for review.",
                ),
            }

        if selected_mode == DATA_QUALITY_ENTER_ROW_FILTER_OPTION:
            state["stage"] = "awaiting_row_filter"
            return {
                "answer": append_choice_markdown(
                    (
                        "## Row Filter\n"
                        "Type a safe boolean expression such as `region = 'FR'`."
                    ),
                    "Quick Choice",
                    "Or skip the row filter for this run.",
                    [DATA_QUALITY_SKIP_ROW_FILTER_OPTION],
                ),
                "agent_state": state,
                "steps": _data_quality_agent_steps(
                    "dq-row-filter",
                    "Waiting for row filter",
                    "running",
                    "The agent is waiting for a manual row filter expression.",
                ),
            }

        if user_message:
            validation_error = _validate_data_quality_row_filter(user_message)
            if not validation_error:
                state["row_filter"] = user_message.strip()
                state["stage"] = "awaiting_time_column" if state.get("date_columns") else "awaiting_review"

                if state.get("stage") == "awaiting_time_column":
                    time_options = [DATA_QUALITY_SKIP_TIME_OPTION] + list(state.get("date_columns") or [])
                    return {
                        "answer": build_choice_markdown(
                            "Volumetric Analysis",
                            "Choose a time column if you also want a volume-over-time analysis.",
                            time_options,
                        ),
                        "agent_state": state,
                        "steps": _data_quality_agent_steps(
                            "dq-time-column",
                            "Waiting for time column",
                            "running",
                            "The row filter is set, and the agent is waiting for the optional volumetric-analysis choice.",
                        ),
                    }

                return {
                    "answer": append_choice_markdown(
                        _data_quality_review_markdown(state),
                        "Review Actions",
                        "Choose the next action for this data-quality run.",
                        DATA_QUALITY_REVIEW_OPTIONS,
                    ),
                    "agent_state": state,
                    "steps": _data_quality_agent_steps(
                        "dq-review",
                        "Ready to launch",
                        "running",
                        "The row filter is set, and the run is ready for review.",
                    ),
                }

        return {
            "answer": build_choice_markdown(
                "Row Filter",
                "Choose whether to profile all rows or enter a manual row filter.",
                row_filter_options,
            ),
            "agent_state": state,
            "steps": _data_quality_agent_steps(
                "dq-row-filter-mode",
                "Waiting for row filter mode",
                "running",
                "The agent is waiting for the optional row-filter choice.",
            ),
        }

    if state.get("stage") == "awaiting_row_filter":
        if (
            not user_message
            or normalized_lower in {"skip", "no filter", "without filter", "none", "all rows"}
            or resolve_user_choice(user_message, [DATA_QUALITY_SKIP_ROW_FILTER_OPTION]) == DATA_QUALITY_SKIP_ROW_FILTER_OPTION
        ):
            state["row_filter"] = ""
            state["stage"] = "awaiting_time_column" if state.get("date_columns") else "awaiting_review"
        else:
            validation_error = _validate_data_quality_row_filter(user_message)
            if validation_error:
                return {
                    "answer": append_choice_markdown(
                        (
                            "## Row Filter\n"
                            f"{validation_error}\n\n"
                            "Please type a safe boolean expression."
                        ),
                        "Quick Choice",
                        "Or skip the row filter for this run.",
                        [DATA_QUALITY_SKIP_ROW_FILTER_OPTION],
                    ),
                    "agent_state": state,
                    "steps": _data_quality_agent_steps(
                        "dq-row-filter",
                        "Waiting for row filter",
                        "running",
                        "The proposed row filter did not pass the safety validation.",
                    ),
                }
            state["row_filter"] = user_message.strip()
            state["stage"] = "awaiting_time_column" if state.get("date_columns") else "awaiting_review"

        if state.get("stage") == "awaiting_time_column":
            time_options = [DATA_QUALITY_SKIP_TIME_OPTION] + list(state.get("date_columns") or [])
            return {
                "answer": build_choice_markdown(
                    "Volumetric Analysis",
                    "Choose a time column if you also want a volume-over-time analysis.",
                    time_options,
                ),
                "agent_state": state,
                "steps": _data_quality_agent_steps(
                    "dq-time-column",
                    "Waiting for time column",
                    "running",
                    "The agent is waiting for the optional volumetric-analysis choice.",
                ),
            }

    if state.get("stage") == "awaiting_time_column":
        time_options = [DATA_QUALITY_SKIP_TIME_OPTION] + list(state.get("date_columns") or [])
        selected_time = resolve_user_choice(user_message, time_options)
        if not selected_time:
            return {
                "answer": build_choice_markdown(
                    "Volumetric Analysis",
                    "Choose a time column if you also want a volume-over-time analysis.",
                    time_options,
                ),
                "agent_state": state,
                "steps": _data_quality_agent_steps(
                    "dq-time-column",
                    "Waiting for time column",
                    "running",
                    "The agent is waiting for the optional volumetric-analysis choice.",
                ),
            }
        state["time_column"] = None if selected_time == DATA_QUALITY_SKIP_TIME_OPTION else selected_time
        state["stage"] = "awaiting_review"
        return {
            "answer": append_choice_markdown(
                _data_quality_review_markdown(state),
                "Review Actions",
                "Choose the next action for this data-quality run.",
                DATA_QUALITY_REVIEW_OPTIONS,
            ),
            "agent_state": state,
            "steps": _data_quality_agent_steps(
                "dq-review",
                "Ready to launch",
                "running",
                "The optional volumetric-analysis choice is complete, and the run is ready for review.",
            ),
        }

    if state.get("stage") == "awaiting_review":
        launch_tokens = {"launch analysis", "run analysis", "launch", "run", "go", "analyze"}
        selected_review_action = resolve_user_choice(user_message, DATA_QUALITY_REVIEW_OPTIONS)
        if not selected_review_action and normalized_lower in launch_tokens:
            selected_review_action = "Launch analysis"

        if selected_review_action == "Launch analysis":
            return await _run_data_quality_analysis(state)
        if selected_review_action == "Edit table":
            state["stage"] = "awaiting_table"
            return {
                "answer": append_choice_markdown(
                    _data_quality_intro_markdown(req.clickhouse.database, table_options, len(state["available_tables"])),
                    "Table Selection",
                    "Choose the table you want to profile first.",
                    table_options,
                ),
                "agent_state": state,
                "steps": _data_quality_agent_steps(
                    "dq-edit-table",
                    "Editing table",
                    "running",
                    "The user chose to change the target table.",
                ),
            }
        if selected_review_action == "Edit columns":
            state["columns"] = []
            state["stage"] = "awaiting_columns_mode"
        elif selected_review_action == "Edit sample size":
            state["stage"] = "awaiting_sample_size"
        elif selected_review_action == "Edit row filter":
            state["stage"] = "awaiting_row_filter_mode"
        elif selected_review_action == "Edit time column":
            if state.get("date_columns"):
                state["stage"] = "awaiting_time_column"
            else:
                state["time_column"] = None
        elif selected_review_action == "Start over":
            state = _default_data_quality_state()
            state["agent_id"] = "data_quality_tables"
            state["db_type"] = "clickhouse"
            state["session_id"] = uuid.uuid4().hex
            state = await _reload_tables(state)
            state["stage"] = "awaiting_table"
            table_options = _data_quality_table_options(state)
            return {
                "answer": append_choice_markdown(
                    _data_quality_intro_markdown(req.clickhouse.database, table_options, len(state["available_tables"])),
                    "Table Selection",
                    "Choose the table you want to profile first.",
                    table_options,
                ),
                "agent_state": state,
                "steps": _data_quality_agent_steps(
                    "dq-restart",
                    "Restarted setup",
                    "running",
                    "The guided data-quality setup was reset.",
                ),
            }

        if state.get("stage") == "awaiting_columns_mode":
            return {
                "answer": build_choice_markdown(
                    "Column Scope",
                    f"I loaded `{len(state.get('available_columns') or [])}` column(s) from `{state['table']}`. Choose the profiling scope.",
                    _data_quality_column_mode_options(state),
                ),
                "agent_state": state,
                "steps": _data_quality_agent_steps(
                    "dq-columns-mode",
                    "Waiting for column scope",
                    "running",
                    "The agent is waiting for the user to redefine the profiling scope.",
                ),
            }
        if state.get("stage") == "awaiting_sample_size":
            return {
                "answer": build_choice_markdown(
                    "Sample Size",
                    "Choose how many rows should be profiled. Full scans are safety-capped.",
                    list(DATA_QUALITY_SAMPLE_OPTIONS.keys()) + [DATA_QUALITY_CUSTOM_SAMPLE_OPTION],
                ),
                "agent_state": state,
                "steps": _data_quality_agent_steps(
                    "dq-sample",
                    "Waiting for sample size",
                    "running",
                    "The agent is waiting for a new sample size.",
                ),
            }
        if state.get("stage") == "awaiting_row_filter_mode":
            return {
                "answer": build_choice_markdown(
                    "Row Filter",
                    "Choose whether to profile all rows or enter a manual row filter.",
                    [DATA_QUALITY_SKIP_ROW_FILTER_OPTION, DATA_QUALITY_ENTER_ROW_FILTER_OPTION],
                ),
                "agent_state": state,
                "steps": _data_quality_agent_steps(
                    "dq-row-filter-mode",
                    "Waiting for row filter mode",
                    "running",
                    "The agent is waiting for the optional row-filter choice.",
                ),
            }
        if state.get("stage") == "awaiting_row_filter":
            return {
                "answer": append_choice_markdown(
                    (
                        "## Row Filter\n"
                        "Type a safe boolean expression such as `region = 'FR'`."
                    ),
                    "Quick Choice",
                    "Or skip the row filter for this run.",
                    [DATA_QUALITY_SKIP_ROW_FILTER_OPTION],
                ),
                "agent_state": state,
                "steps": _data_quality_agent_steps(
                    "dq-row-filter",
                    "Waiting for row filter",
                    "running",
                    "The agent is waiting for the optional row filter.",
                ),
            }
        if state.get("stage") == "awaiting_time_column":
            return {
                "answer": build_choice_markdown(
                    "Volumetric Analysis",
                    "Choose a time column if you also want a volume-over-time analysis.",
                    [DATA_QUALITY_SKIP_TIME_OPTION] + list(state.get("date_columns") or []),
                ),
                "agent_state": state,
                "steps": _data_quality_agent_steps(
                    "dq-time-column",
                    "Waiting for time column",
                    "running",
                    "The agent is waiting for the optional volumetric-analysis choice.",
                ),
            }

        return {
            "answer": append_choice_markdown(
                _data_quality_review_markdown(state),
                "Review Actions",
                "Choose the next action for this data-quality run.",
                DATA_QUALITY_REVIEW_OPTIONS,
            ),
            "agent_state": state,
            "steps": _data_quality_agent_steps(
                "dq-review",
                "Ready to launch",
                "running",
                "The analysis parameters are ready and waiting for the final launch decision.",
            ),
        }

    if state.get("columns") and state.get("stage") == "awaiting_sample_size":
        return {
            "answer": build_choice_markdown(
                "Sample Size",
                "Choose how many rows should be profiled. Full scans are safety-capped.",
                list(DATA_QUALITY_SAMPLE_OPTIONS.keys()) + [DATA_QUALITY_CUSTOM_SAMPLE_OPTION],
            ),
            "agent_state": state,
            "steps": _data_quality_agent_steps(
                "dq-sample",
                "Waiting for sample size",
                "running",
                "The agent is waiting for the sampling strategy.",
            ),
        }

    state["stage"] = "awaiting_review"
    return {
        "answer": append_choice_markdown(
            _data_quality_review_markdown(state),
            "Review Actions",
            "Choose the next action for this data-quality run.",
            DATA_QUALITY_REVIEW_OPTIONS,
        ),
        "agent_state": state,
        "steps": _data_quality_agent_steps(
            "dq-review",
            "Ready to launch",
            "running",
            "The analysis parameters are ready and waiting for the final launch decision.",
        ),
    }


@app.post("/api/chat/manager-agent")
async def chat_manager_agent(req: ManagerAgentRequest):
    user_message = (req.message or "").strip()
    manager_state = _normalize_manager_agent_state(req.manager_state.model_dump())
    clickhouse_state = dump_clickhouse_agent_state(req.clickhouse_state)
    file_manager_state = _normalize_file_manager_state(req.file_manager_state.model_dump())
    data_quality_state = _normalize_data_quality_state(req.data_quality_state.model_dump())
    file_manager_config = _normalize_file_manager_config(req.file_manager_config.model_dump())
    pending_pipeline = manager_state.get("pending_pipeline")

    if not user_message:
        manager_state["active_delegate"] = None
        return {
            "answer": (
                "## Agent Manager\n"
                "Describe the outcome you want, and I will either answer directly or route the task "
                "to ClickHouse Query, File management, or Data quality - Tables when a specialist is needed."
            ),
            "agent_state": {
                "manager": manager_state,
                "clickhouse": clickhouse_state,
                "fileManager": file_manager_state,
                "dataQuality": data_quality_state,
            },
            "steps": [
                {
                    "id": "manager-ready",
                    "title": "Manager ready",
                    "status": "success",
                    "details": "The manager can orchestrate all specialist agents currently available in RAGnarok.",
                }
            ],
        }

    async def _delegate_file_manager(message: str) -> dict[str, Any]:
        return await chat_file_manager_agent(
            FileManagerAgentRequest(
                message=message,
                history=req.history,
                llm_base_url=req.llm_base_url,
                llm_model=req.llm_model,
                llm_api_key=req.llm_api_key,
                llm_provider=req.llm_provider,
                agent_state=FileManagerAgentStateModel(**file_manager_state),
                file_manager_config=FileManagerAgentConfigModel(
                    base_path=file_manager_config["basePath"],
                    max_iterations=file_manager_config["maxIterations"],
                    system_prompt=file_manager_config["systemPrompt"],
                ),
            )
        )

    if (
        isinstance(pending_pipeline, dict)
        and pending_pipeline.get("kind") == "clickhouse_to_file"
        and pending_pipeline.get("stage") == "awaiting_export_details"
    ):
        if is_negative_response(user_message) or normalize_choice(user_message).lower() in {"cancel", "stop", "never mind"}:
            manager_state["pending_pipeline"] = None
            manager_state["active_delegate"] = None
            return {
                "answer": "## Agent Manager\nThe pending file export was cancelled.",
                "agent_state": {
                    "manager": manager_state,
                    "clickhouse": clickhouse_state,
                    "fileManager": file_manager_state,
                    "dataQuality": data_quality_state,
                },
                "steps": [
                    {
                        "id": "manager-export-cancelled",
                        "title": "Cancelled pending export",
                        "status": "success",
                        "details": "The user cancelled the second step of the ClickHouse-to-file workflow.",
                    }
                ],
            }

        pending_pipeline = _manager_update_export_pipeline_from_reply(pending_pipeline, user_message)
        manager_state["pending_pipeline"] = pending_pipeline
        if _manager_pending_pipeline_requires_details(pending_pipeline):
            manager_state["active_delegate"] = None
            return {
                "answer": _manager_export_details_prompt(pending_pipeline),
                "agent_state": {
                    "manager": manager_state,
                    "clickhouse": clickhouse_state,
                    "fileManager": file_manager_state,
                    "dataQuality": data_quality_state,
                },
                "steps": [
                    {
                        "id": "manager-export-details",
                        "title": "Waiting for export details",
                        "status": "running",
                        "details": "The manager still needs the target format or path before delegating to File management.",
                    }
                ],
            }

        delegate = "file_management"
        routing = {
            "delegate": "file_management",
            "reasoning": "The manager now has the remaining export details and can continue with File management.",
            "handoff_message": json.dumps(
                _build_file_export_payload_from_clickhouse(pending_pipeline, clickhouse_state),
                ensure_ascii=False,
            ),
        }
    else:
        routing = await analyze_manager_routing(
            user_message,
            req.history,
            manager_state,
            clickhouse_state,
            file_manager_state,
            data_quality_state,
            req.llm_base_url,
            req.llm_model,
            req.llm_provider,
            req.llm_api_key,
        )
        if routing["delegate"] == "clickhouse_query":
            export_pipeline = _extract_clickhouse_file_export_pipeline(user_message)
            if export_pipeline and not manager_state.get("pending_pipeline"):
                manager_state["pending_pipeline"] = export_pipeline
        elif routing["delegate"] != "file_management":
            manager_state["pending_pipeline"] = None

    delegate = routing["delegate"]
    manager_state["last_routing_reason"] = routing["reasoning"]
    manager_state["last_delegate_label"] = _manager_specialist_label(delegate)

    base_steps = [
        {
            "id": "manager-analyze",
            "title": "Analyzed request",
            "status": "success",
            "details": routing["reasoning"],
        }
    ]

    if delegate == "manager":
        manager_state["active_delegate"] = None
        manager_state["pending_pipeline"] = None
        answer = await _run_manager_direct_response(
            req.history,
            req.llm_base_url,
            req.llm_model,
            req.llm_provider,
            req.llm_api_key,
            req.system_prompt,
        )
        return {
            "answer": answer,
            "agent_state": {
                "manager": manager_state,
                "clickhouse": clickhouse_state,
                "fileManager": file_manager_state,
                "dataQuality": data_quality_state,
            },
            "steps": base_steps + [
                {
                    "id": "manager-direct",
                    "title": "Answered directly",
                    "status": "success",
                    "details": "No specialist tool was needed for this turn.",
                }
            ],
        }

    try:
        if delegate == "clickhouse_query":
            delegated = await chat_clickhouse_agent(
                ClickHouseAgentRequest(
                    message=(
                        routing["handoff_message"] + "\n\nDo not start a chart flow for this turn. Return the text result only."
                        if manager_state.get("pending_pipeline")
                        else routing["handoff_message"]
                    ),
                    history=req.history,
                    clickhouse=req.clickhouse,
                    llm_base_url=req.llm_base_url,
                    llm_model=req.llm_model,
                    llm_api_key=req.llm_api_key,
                    llm_provider=req.llm_provider,
                    agent_state=ClickHouseAgentState(**clickhouse_state),
                )
            )
            clickhouse_state = (
                delegated.get("agent_state")
                if isinstance(delegated.get("agent_state"), dict)
                else clickhouse_state
            )
            manager_state["active_delegate"] = (
                "clickhouse_query" if _clickhouse_state_needs_followup(clickhouse_state) else None
            )
        elif delegate == "file_management":
            delegated = await _delegate_file_manager(routing["handoff_message"])
            file_manager_state = _normalize_file_manager_state(delegated.get("agent_state"))
            manager_state["active_delegate"] = (
                "file_management" if _file_manager_state_needs_followup(file_manager_state) else None
            )
            manager_state["pending_pipeline"] = None
        elif delegate == "data_quality_tables":
            delegated = await chat_data_quality_agent(
                DataQualityAgentRequest(
                    message=routing["handoff_message"],
                    history=req.history,
                    clickhouse=req.clickhouse,
                    llm_base_url=req.llm_base_url,
                    llm_model=req.llm_model,
                    llm_api_key=req.llm_api_key,
                    llm_provider=req.llm_provider,
                    agent_state=DataQualityAgentStateModel(**data_quality_state),
                )
            )
            data_quality_state = _normalize_data_quality_state(delegated.get("agent_state"))
            manager_state["active_delegate"] = (
                "data_quality_tables" if _data_quality_state_needs_followup(data_quality_state) else None
            )
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported manager delegate: {delegate}")
    except HTTPException as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail=f"Manager delegation to {_manager_specialist_label(delegate)} failed: {exc.detail}",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Manager delegation to {_manager_specialist_label(delegate)} failed: {exc}",
        ) from exc

    specialist_label = _manager_specialist_label(delegate)
    specialist_steps = _prefix_agent_steps(delegated.get("steps") or [], delegate)
    manager_steps = base_steps + [
        {
            "id": "manager-route",
            "title": f"Delegated to {specialist_label}",
            "status": "running" if manager_state.get("active_delegate") else "success",
            "details": (
                "The specialist needs more user input to continue."
                if manager_state.get("active_delegate")
                else "The specialist completed its turn and returned the result."
            ),
        }
    ]

    if (
        delegate == "clickhouse_query"
        and isinstance(manager_state.get("pending_pipeline"), dict)
        and manager_state["pending_pipeline"].get("kind") == "clickhouse_to_file"
        and not manager_state.get("active_delegate")
    ):
        pending_pipeline = dict(manager_state["pending_pipeline"])
        if _manager_pending_pipeline_requires_details(pending_pipeline):
            pending_pipeline["stage"] = "awaiting_export_details"
            manager_state["pending_pipeline"] = pending_pipeline
            manager_steps.append(
                {
                    "id": "manager-await-export-details",
                    "title": "Waiting for export details",
                    "status": "running",
                    "details": "The ClickHouse query is complete, but the manager still needs the export format or target path.",
                }
            )
            return {
                "answer": _manager_compose_chained_answer(
                    delegated.get("answer") or "",
                    _manager_export_details_prompt(pending_pipeline),
                    "File Export",
                ),
                "actions": delegated.get("actions"),
                "chart": delegated.get("chart"),
                "agent_state": {
                    "manager": manager_state,
                    "clickhouse": clickhouse_state,
                    "fileManager": file_manager_state,
                    "dataQuality": data_quality_state,
                },
                "steps": manager_steps + specialist_steps,
            }

        try:
            chained = await _delegate_file_manager(
                json.dumps(
                    _build_file_export_payload_from_clickhouse(pending_pipeline, clickhouse_state),
                    ensure_ascii=False,
                )
            )
            file_manager_state = _normalize_file_manager_state(chained.get("agent_state"))
            manager_state["active_delegate"] = (
                "file_management" if _file_manager_state_needs_followup(file_manager_state) else None
            )
            manager_state["pending_pipeline"] = None
            chained_steps = _prefix_agent_steps(chained.get("steps") or [], "file_management")
            manager_steps.append(
                {
                    "id": "manager-chain-file-export",
                    "title": "Continued to File management",
                    "status": "running" if manager_state.get("active_delegate") else "success",
                    "details": "The manager continued the same request by exporting the ClickHouse result through File management.",
                }
            )
            return {
                "answer": _manager_compose_chained_answer(
                    delegated.get("answer") or "",
                    chained.get("answer") or "",
                    "File Export",
                ),
                "actions": chained.get("actions") or delegated.get("actions"),
                "chart": delegated.get("chart") or chained.get("chart"),
                "agent_state": {
                    "manager": manager_state,
                    "clickhouse": clickhouse_state,
                    "fileManager": file_manager_state,
                    "dataQuality": data_quality_state,
                },
                "steps": manager_steps + specialist_steps + chained_steps,
            }
        except HTTPException as exc:
            manager_state["pending_pipeline"] = None
            manager_state["active_delegate"] = None
            manager_steps.append(
                {
                    "id": "manager-chain-file-export-failed",
                    "title": "File export failed",
                    "status": "error",
                    "details": str(exc.detail),
                }
            )
            return {
                "answer": _manager_compose_chained_answer(
                    delegated.get("answer") or "",
                    f"## File Export\nI could not complete the export step.\n\n```text\n{exc.detail}\n```",
                    "File Export",
                ),
                "actions": delegated.get("actions"),
                "chart": delegated.get("chart"),
                "agent_state": {
                    "manager": manager_state,
                    "clickhouse": clickhouse_state,
                    "fileManager": file_manager_state,
                    "dataQuality": data_quality_state,
                },
                "steps": manager_steps + specialist_steps,
            }

    return {
        "answer": delegated.get("answer") or "## Answer\nThe delegated agent completed its turn.",
        "actions": delegated.get("actions"),
        "chart": delegated.get("chart"),
        "agent_state": {
            "manager": manager_state,
            "clickhouse": clickhouse_state,
            "fileManager": file_manager_state,
            "dataQuality": data_quality_state,
        },
        "steps": manager_steps + specialist_steps,
    }


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


def append_choice_markdown(base_answer: str, title: str, prompt: str, options: list[str]) -> str:
    choice = build_choice_markdown(title, prompt, options)
    return f"{base_answer}\n\n---\n\n{choice}" if base_answer else choice


def reset_clickhouse_clarification(state: ClickHouseAgentState) -> None:
    state.clarification_prompt = ""
    state.clarification_options = []


@app.post("/api/chat/clickhouse-agent")
async def chat_clickhouse_agent(req: ClickHouseAgentRequest):
    state = req.agent_state
    user_message = (req.message or "").strip()
    memory_anchor = state.pending_request or user_message
    conversation_memory = _conversation_memory_markdown(
        req.history,
        current_message=memory_anchor,
        max_steps=CHAT_MEMORY_MAX_STEPS,
    )

    try:
        state.available_tables = state.available_tables or await list_clickhouse_tables(req.clickhouse)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"ClickHouse connection error: {e}")

    if not state.available_tables:
        raise HTTPException(status_code=400, detail="No tables were found in the configured ClickHouse database.")

    explicit_table_switch = resolve_user_choice(user_message, state.available_tables)
    if state.selected_table and explicit_table_switch and explicit_table_switch != state.selected_table:
        state.selected_table = explicit_table_switch
        state.table_schema = []
        state.pending_request = ""
        reset_clickhouse_query_resolution(state)
        state.stage = "ready"

    if state.stage == "ready" and not state.pending_request and state.last_result_rows and is_chart_followup_request(user_message):
        state.chart_requested = True
        state.stage = "awaiting_chart_x"

    chart_flow_cancelled = False

    if state.last_result_rows and state.last_result_meta and (
        state.chart_requested
        or state.stage in {"awaiting_chart_offer", "awaiting_chart_x", "awaiting_chart_y", "awaiting_chart_type"}
    ):
        chart_context = infer_chart_options(state.last_result_meta, state.last_result_rows)
        if not chart_context["can_chart"]:
            reset_clickhouse_chart_state(state)
            state.stage = "ready"
        else:
            state.chart_x_options = state.chart_x_options or chart_context["x_options"]
            state.chart_y_options = state.chart_y_options or chart_context["y_options"]
            state.chart_type_options = state.chart_type_options or chart_context["type_options"]

            if state.stage == "awaiting_chart_offer":
                chart_offer_choice = resolve_user_choice(
                    user_message,
                    state.chart_offer_options or [CHART_CREATE_OPTION, CHART_SKIP_OPTION],
                )
                if not chart_offer_choice and is_affirmative_response(user_message):
                    chart_offer_choice = CHART_CREATE_OPTION
                if not chart_offer_choice and is_negative_response(user_message):
                    chart_offer_choice = CHART_SKIP_OPTION
                if chart_offer_choice == CHART_SKIP_OPTION:
                    reset_clickhouse_chart_state(state)
                    state.stage = "ready"
                    return {
                        "answer": build_clickhouse_response_markdown(
                            "I kept the latest result in **text format only**. No chart was created.",
                            [state.last_sql],
                        ),
                        "agent_state": dump_clickhouse_agent_state(state),
                        "steps": [
                            {
                                "id": "ch-chart-skip",
                                "title": "Skipped chart generation",
                                "status": "success",
                                "details": "The user chose to keep the tabular/text answer only.",
                            }
                        ],
                    }
                if chart_offer_choice == CHART_CREATE_OPTION:
                    state.chart_requested = True
                    state.stage = "awaiting_chart_x"
                elif user_message:
                    reset_clickhouse_chart_state(state)
                    state.stage = "ready"

            if state.chart_requested or state.stage in {"awaiting_chart_x", "awaiting_chart_y", "awaiting_chart_type"}:
                requested_chart_type = detect_requested_chart_type(user_message)
                x_options = state.chart_x_options
                y_options = [
                    option for option in state.chart_y_options
                    if option != state.selected_chart_x
                ] or state.chart_y_options

                if not state.selected_chart_x:
                    x_choice = resolve_user_choice(user_message, x_options) if state.stage == "awaiting_chart_x" else None
                    if len(x_options) == 1:
                        state.selected_chart_x = x_options[0]
                    elif x_choice:
                        state.selected_chart_x = x_choice
                    elif state.stage == "awaiting_chart_x" and user_message:
                        reset_clickhouse_chart_state(state)
                        state.stage = "ready"
                        chart_flow_cancelled = True
                    else:
                        state.stage = "awaiting_chart_x"
                        return {
                            "answer": build_choice_markdown(
                                "Chart X Axis",
                                "Choose the field to use on the X axis.",
                                x_options,
                            ),
                            "agent_state": dump_clickhouse_agent_state(state),
                            "steps": [
                                {
                                    "id": "ch-chart-x",
                                    "title": "Waiting for X axis selection",
                                    "status": "running",
                                    "details": "The user must choose which field should drive the horizontal axis.",
                                }
                            ],
                        }

                if chart_flow_cancelled:
                    pass
                elif not state.selected_chart_y:
                    y_choice = resolve_user_choice(user_message, y_options) if state.stage == "awaiting_chart_y" else None
                    if len(y_options) == 1:
                        state.selected_chart_y = y_options[0]
                    elif y_choice:
                        state.selected_chart_y = y_choice
                    elif state.stage == "awaiting_chart_y" and user_message:
                        reset_clickhouse_chart_state(state)
                        state.stage = "ready"
                        chart_flow_cancelled = True
                    else:
                        state.stage = "awaiting_chart_y"
                        return {
                            "answer": build_choice_markdown(
                                "Chart Y Axis",
                                "Choose the metric to use on the Y axis.",
                                y_options,
                            ),
                            "agent_state": dump_clickhouse_agent_state(state),
                            "steps": [
                                {
                                    "id": "ch-chart-y",
                                    "title": "Waiting for Y axis selection",
                                    "status": "running",
                                    "details": "The user must choose the metric to visualize.",
                                }
                            ],
                        }

                if chart_flow_cancelled:
                    pass
                elif not state.selected_chart_type:
                    chart_type_choice = None
                    if requested_chart_type and CHART_TYPE_LABELS.get(requested_chart_type) in state.chart_type_options:
                        chart_type_choice = requested_chart_type
                    elif state.stage == "awaiting_chart_type":
                        resolved_type_label = resolve_user_choice(user_message, state.chart_type_options)
                        if resolved_type_label:
                            chart_type_choice = CHART_TYPE_BY_LABEL.get(resolved_type_label.lower())

                    if len(state.chart_type_options) == 1:
                        chart_type_choice = CHART_TYPE_BY_LABEL.get(state.chart_type_options[0].lower())

                    if chart_type_choice:
                        state.selected_chart_type = chart_type_choice
                    elif state.stage == "awaiting_chart_type" and user_message:
                        reset_clickhouse_chart_state(state)
                        state.stage = "ready"
                        chart_flow_cancelled = True
                    else:
                        state.stage = "awaiting_chart_type"
                        return {
                            "answer": build_choice_markdown(
                                "Chart Type",
                                "Choose the chart type.",
                                state.chart_type_options,
                            ),
                            "agent_state": dump_clickhouse_agent_state(state),
                            "steps": [
                                {
                                    "id": "ch-chart-type",
                                    "title": "Waiting for chart type",
                                    "status": "running",
                                    "details": "The user must choose how to visualize the selected axes.",
                                }
                            ],
                        }

                if not chart_flow_cancelled:
                    chart = build_chart(
                        state.last_result_rows,
                        state.selected_chart_x,
                        state.selected_chart_y,
                        state.selected_chart_type,
                    )
                    if not chart:
                        reset_clickhouse_chart_state(state)
                        state.stage = "ready"
                        return {
                            "answer": build_clickhouse_response_markdown(
                                (
                                    "I could not build a usable chart from the latest result because the selected "
                                    "axes do not produce enough valid numeric data points."
                                ),
                                [state.last_sql],
                            ),
                            "agent_state": dump_clickhouse_agent_state(state),
                            "steps": [
                                {
                                    "id": "ch-chart-failed",
                                    "title": "Chart generation failed",
                                    "status": "error",
                                    "details": "Not enough valid data points remained after filtering null or non-numeric values.",
                                }
                            ],
                        }

                    chart_type_label = CHART_TYPE_LABELS.get(state.selected_chart_type, "Chart")
                    answer = build_clickhouse_response_markdown(
                        (
                            f"I created a **{chart_type_label.lower()}** based on the latest query result.\n\n"
                            f"- **X axis:** `{state.selected_chart_x}`\n"
                            f"- **Y axis:** `{state.selected_chart_y}`\n"
                            f"- **Purpose:** make the result easier to read and compare visually"
                        ),
                        [state.last_sql],
                    )
                    reset_clickhouse_chart_state(state)
                    state.stage = "ready"
                    return {
                        "answer": answer,
                        "chart": chart,
                        "agent_state": dump_clickhouse_agent_state(state),
                        "steps": [
                            {
                                "id": "ch-chart-built",
                                "title": "Generated chart",
                                "status": "success",
                                "details": f"Built a {chart_type_label.lower()} from the latest ClickHouse query result.",
                            }
                        ],
                    }

    if state.stage == "awaiting_table":
        table_options = state.clarification_options or state.available_tables
        selected_table = resolve_user_choice(user_message, table_options) or resolve_user_choice(user_message, state.available_tables)
        if not selected_table:
            return {
                "answer": build_choice_markdown(
                    "Table Clarification",
                    state.clarification_prompt or "Which table should I use for this request?",
                    table_options,
                ),
                "agent_state": dump_clickhouse_agent_state(state),
                "steps": [
                    {
                        "id": "ch-await-table",
                        "title": "Waiting for table selection",
                        "status": "running",
                        "details": "Multiple tables remain plausible for the current request.",
                    }
                ],
            }
        state.selected_table = selected_table
        state.stage = "ready"
        reset_clickhouse_clarification(state)

    if state.stage == "ready" and user_message and not explicit_table_switch:
        state.pending_request = user_message
        reset_clickhouse_query_resolution(state)

    if not state.selected_table:
        selected_table = resolve_user_choice(user_message, state.available_tables)
        if selected_table:
            state.selected_table = selected_table
            state.stage = "ready"
            reset_clickhouse_clarification(state)
            if not state.pending_request:
                state.pending_request = ""
        elif len(state.available_tables) == 1:
            state.selected_table = state.available_tables[0]
            state.stage = "ready"
            reset_clickhouse_clarification(state)
        else:
            if user_message and not state.pending_request:
                state.pending_request = user_message
            if not state.pending_request:
                return {
                    "answer": (
                        "## ClickHouse Query Agent\n"
                        "Ask your question directly. I will infer the best table when the intent is clear, "
                        "and I will only ask you to choose if the request stays ambiguous."
                    ),
                    "agent_state": dump_clickhouse_agent_state(state),
                    "steps": [
                        {
                            "id": "ch-ready",
                            "title": "Ready for direct query",
                            "status": "success",
                            "details": f"Loaded {len(state.available_tables)} table(s) from `{req.clickhouse.database}`.",
                        }
                    ],
                }

            table_analysis = await analyze_clickhouse_tables(
                state.pending_request,
                state.available_tables,
                conversation_memory,
                req.llm_base_url,
                req.llm_model,
                req.llm_provider,
                req.llm_api_key,
            )
            matched_candidates = match_available_options(
                table_analysis["table_candidates"],
                state.available_tables,
            )
            matched_selected_table = match_available_options(
                [table_analysis["selected_table"]],
                state.available_tables,
            )

            if matched_selected_table and not table_analysis["table_choice_required"]:
                state.selected_table = matched_selected_table[0]
                state.stage = "ready"
                reset_clickhouse_clarification(state)
            elif len(matched_candidates) == 1 and not table_analysis["table_choice_required"]:
                state.selected_table = matched_candidates[0]
                state.stage = "ready"
                reset_clickhouse_clarification(state)
            else:
                state.stage = "awaiting_table"
                state.clarification_options = matched_candidates or state.available_tables
                state.clarification_prompt = (
                    table_analysis["table_choice_prompt"]
                    or "Which table should I use for this request?"
                )
                return {
                    "answer": build_choice_markdown(
                        "Table Clarification",
                        state.clarification_prompt,
                        state.clarification_options,
                    ),
                    "agent_state": dump_clickhouse_agent_state(state),
                    "steps": [
                        {
                            "id": "ch-tables",
                            "title": "Loaded ClickHouse tables",
                            "status": "success",
                            "details": f"Found {len(state.available_tables)} table(s) in database `{req.clickhouse.database}`.",
                        },
                        {
                            "id": "ch-table-routing",
                            "title": "Need table confirmation",
                            "status": "running",
                            "details": table_analysis["reasoning"] or "Several tables look plausible for the current request.",
                        },
                    ],
                }

    if state.selected_table and not state.pending_request and not explicit_table_switch:
        state.pending_request = user_message

    if not state.pending_request:
        return {
            "answer": (
                f"## ClickHouse Query Agent\nI am focused on table `{state.selected_table}`.\n\n"
                "Please tell me what you want to know, and I will inspect the schema before writing SQL."
            ),
            "agent_state": dump_clickhouse_agent_state(state),
            "steps": [
                {
                    "id": "ch-await-request",
                    "title": "Waiting for analytical request",
                    "status": "running",
                    "details": "The table is selected, but the user has not asked a data question yet.",
                }
            ],
        }

    if not state.table_schema:
        try:
            state.table_schema = await describe_clickhouse_table(req.clickhouse, state.selected_table)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to inspect schema for {state.selected_table}: {e}")

    if not state.table_schema:
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
                "agent_state": dump_clickhouse_agent_state(state),
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
                "agent_state": dump_clickhouse_agent_state(state),
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
        state.table_schema,
        conversation_memory,
        req.llm_base_url,
        req.llm_model,
        req.llm_provider,
        req.llm_api_key,
    )

    state.candidate_fields = match_schema_columns(analysis["field_candidates"], state.table_schema)
    detected_date_fields = match_schema_columns(analysis["date_candidates"], state.table_schema)
    heuristic_dates = find_date_columns(state.table_schema)
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
            "agent_state": dump_clickhouse_agent_state(state),
            "steps": [
                {
                    "id": "ch-schema",
                    "title": "Inspected table schema",
                    "status": "success",
                    "details": f"Loaded {len(state.table_schema)} columns from `{state.selected_table}`.",
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
            "agent_state": dump_clickhouse_agent_state(state),
            "steps": [
                {
                    "id": "ch-schema",
                    "title": "Inspected table schema",
                    "status": "success",
                    "details": f"Loaded {len(state.table_schema)} columns from `{state.selected_table}`.",
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
        state.table_schema,
        state.selected_field,
        state.selected_date_field,
        conversation_memory,
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
            state.table_schema,
            state.selected_field,
            state.selected_date_field,
            conversation_memory,
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

    original_request = state.pending_request
    result_summary = await summarize_clickhouse_result(
        original_request,
        sql,
        generated["reasoning"],
        result.get("data", []),
        conversation_memory,
        req.llm_base_url,
        req.llm_model,
        req.llm_provider,
        req.llm_api_key,
    )
    answer = build_clickhouse_response_markdown(result_summary, [sql])

    state.last_sql = sql
    state.last_result_meta = result.get("meta", [])
    export_row_cap = max(1, min(int(req.clickhouse.query_limit or 200), 2000))
    state.last_result_rows = result.get("data", [])[:export_row_cap]

    chart_context = infer_chart_options(state.last_result_meta, state.last_result_rows)
    requested_chart = detect_chart_request(original_request)
    if chart_context["can_chart"] and (requested_chart or chart_context["recommended"]):
        reset_clickhouse_chart_state(state)
        if requested_chart:
            initialize_chart_selection(
                state,
                chart_context["x_options"],
                chart_context["y_options"],
                chart_context["type_options"],
                requested_chart_type=detect_requested_chart_type(original_request),
            )
            prompt = next_chart_prompt(state)
            if prompt:
                answer = build_clickhouse_response_markdown(
                    result_summary,
                    [sql],
                    [
                        build_choice_markdown(
                            prompt["title"],
                            prompt["prompt"],
                            prompt["options"],
                        )
                    ],
                )
            else:
                chart = build_chart(
                    state.last_result_rows,
                    state.selected_chart_x,
                    state.selected_chart_y,
                    state.selected_chart_type,
                )
                if chart:
                    state.stage = "ready"
                    answer = build_clickhouse_response_markdown(
                        result_summary,
                        [sql],
                        [
                            (
                                "## Visualization\n"
                                f"I also generated a **{CHART_TYPE_LABELS.get(state.selected_chart_type, 'chart').lower()}** "
                                f"for `{state.selected_chart_y}` by `{state.selected_chart_x}` to make the result easier to interpret."
                            )
                        ],
                    )
                    reset_clickhouse_chart_state(state)
                    return {
                        "answer": answer,
                        "chart": chart,
                        "agent_state": dump_clickhouse_agent_state(state),
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
                                "details": f"Loaded {len(state.table_schema)} columns to map the request safely.",
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
                            {
                                "id": "ch-chart-auto",
                                "title": "Generated chart",
                                "status": "success",
                                "details": "The chart request had only one clear X/Y/type combination, so it was generated directly.",
                            },
                        ],
                    }
        else:
            state.chart_suggested = True
            state.chart_offer_options = [CHART_CREATE_OPTION, CHART_SKIP_OPTION]
            state.chart_x_options = chart_context["x_options"]
            state.chart_y_options = chart_context["y_options"]
            state.chart_type_options = chart_context["type_options"]
            state.stage = "awaiting_chart_offer"
            answer = build_clickhouse_response_markdown(
                result_summary,
                [sql],
                [
                    build_choice_markdown(
                        "Visualization",
                        "This result would work well as a chart. If you want, I can let you choose X, Y, and the chart type.",
                        state.chart_offer_options,
                    )
                ],
            )

    if state.stage not in {"awaiting_chart_offer", "awaiting_chart_x", "awaiting_chart_y", "awaiting_chart_type"}:
        state.stage = "ready"
    state.pending_request = ""
    reset_clickhouse_clarification(state)

    return {
        "answer": answer,
        "agent_state": dump_clickhouse_agent_state(state),
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
                "details": f"Loaded {len(state.table_schema)} columns to map the request safely.",
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
                memory_history = _normalized_history_messages(
                    req.history,
                    current_message=req.message,
                    max_steps=CHAT_MEMORY_MAX_STEPS,
                )

                # Build initial messages
                messages: list[dict] = []
                if req.system_prompt:
                    messages.append({"role": "system", "content": req.system_prompt})
                for m in memory_history:
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
    history = _normalized_history_messages(
        req.history,
        current_message=message,
        max_steps=CHAT_MEMORY_MAX_STEPS,
    )

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
