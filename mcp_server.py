#!/usr/bin/env python3
"""
Logistics Platform MCP Server v1.0
====================================
Реальные MCP tools для AI агента.
Каждый tool читает конфиг партнёра из registry/ и вызывает HTTP adapter.

Протокол: MCP SSE (Model Context Protocol)
Деплой: добавить как отдельный сервис или запустить рядом с api_server.py
"""

import json
import os
import time
import asyncio
from pathlib import Path
from typing import Any

try:
    import httpx
except ImportError:
    httpx = None

try:
    from fastapi import FastAPI, Request
    from fastapi.responses import StreamingResponse, JSONResponse
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn
except ImportError:
    raise SystemExit("pip install fastapi uvicorn httpx")

# ── Config ────────────────────────────────────────────────────────────────────
PLATFORM_API = os.getenv("PLATFORM_API_URL", "https://web-production-93110.up.railway.app")
REGISTRY_DIR = Path(__file__).parent / "registry"
MCP_PORT = int(os.getenv("MCP_PORT", "8002"))

app = FastAPI(title="Logistics Platform MCP Server", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Load partner configs from registry/ ──────────────────────────────────────
def load_all_configs() -> list[dict]:
    """Загружает все MCP конфиги из registry/."""
    configs = []
    if not REGISTRY_DIR.exists():
        print(f"[MCP] Registry dir not found: {REGISTRY_DIR}")
        return configs
    for f in REGISTRY_DIR.glob("*.json"):
        try:
            cfg = json.loads(f.read_text())
            if cfg.get("tools"):  # только MCP конфиги
                configs.append(cfg)
                print(f"[MCP] Loaded: {cfg['partner_id']} ({len(cfg['tools'])} tools)")
        except Exception as e:
            print(f"[MCP] Error loading {f}: {e}")
    return configs

def build_tools_list(configs: list[dict]) -> list[dict]:
    """Строит список MCP tools из всех конфигов."""
    tools = []
    for cfg in configs:
        if cfg.get("status") not in ("active", "pending"):
            continue  # только активные и ожидающие верификации
        for tool in cfg.get("tools", []):
            tools.append({
                "name": tool["name"],
                "description": tool["description"],
                "inputSchema": tool["inputSchema"]
            })
    return tools

# ── Call tool via Platform HTTP Adapter ──────────────────────────────────────
async def execute_tool(tool_name: str, params: dict, configs: list[dict]) -> dict:
    """Выполняет tool через Platform HTTP Adapter."""
    # tool_name format: {partner_id}__{action}
    if "__" not in tool_name:
        return {"error": f"Invalid tool name format: {tool_name}"}

    partner_id, action = tool_name.split("__", 1)

    # Verify tool exists
    tool_exists = any(
        t["name"] == tool_name
        for cfg in configs
        for t in cfg.get("tools", [])
    )
    if not tool_exists:
        return {"error": f"Tool {tool_name} not found in registry"}

    if httpx is None:
        return {"error": "httpx not installed", "tool": tool_name, "params": params}

    # Call Platform HTTP Adapter
    payload = {
        "partner_id": partner_id,
        "action": action,
        "params": params,
        "dry_run": False
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(f"{PLATFORM_API}/adapter/call", json=payload)
        result = resp.json()
        print(f"[MCP] {tool_name} → {result.get('http_status', '?')} ({result.get('latency_ms', '?')}ms)")
        return result
    except httpx.TimeoutException:
        return {"error": "timeout", "tool": tool_name, "detail": "Platform API timeout"}
    except Exception as e:
        return {"error": str(e), "tool": tool_name}

# ── MCP Protocol endpoints ────────────────────────────────────────────────────

@app.get("/health")
def health():
    configs = load_all_configs()
    tools = build_tools_list(configs)
    return {
        "status": "ok",
        "partners": len(configs),
        "tools": len(tools),
        "tool_names": [t["name"] for t in tools]
    }

@app.get("/mcp")
async def mcp_sse(request: Request):
    """MCP SSE endpoint для AI агентов (Claude, etc)."""
    configs = load_all_configs()
    tools = build_tools_list(configs)

    async def event_stream():
        # Send tools/list
        msg = json.dumps({
            "jsonrpc": "2.0",
            "method": "tools/list",
            "result": {"tools": tools}
        })
        yield f"data: {msg}\n\n"

        # Keep connection alive
        while not await request.is_disconnected():
            yield f": ping\n\n"
            await asyncio.sleep(15)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )

@app.post("/mcp")
async def mcp_post(request: Request):
    """MCP JSON-RPC endpoint."""
    body = await request.json()
    method = body.get("method", "")
    req_id = body.get("id")
    params = body.get("params", {})

    configs = load_all_configs()

    if method == "tools/list":
        tools = build_tools_list(configs)
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"tools": tools}
        })

    elif method == "tools/call":
        tool_name = params.get("name", "")
        tool_params = params.get("arguments", {})

        result = await execute_tool(tool_name, tool_params, configs)

        content_text = json.dumps(result, ensure_ascii=False, indent=2)
        is_error = "error" in result

        return JSONResponse({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "content": [{"type": "text", "text": content_text}],
                "isError": is_error
            }
        })

    elif method == "initialize":
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "logistics-platform-mcp", "version": "1.0.0"}
            }
        })

    else:
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": f"Method not found: {method}"}
        }, status_code=404)


@app.get("/mcp/tools")
def list_tools():
    """Список всех доступных tools (для отладки)."""
    configs = load_all_configs()
    tools = build_tools_list(configs)
    return {
        "total": len(tools),
        "partners": [{"id": c["partner_id"], "name": c["partner_name"], "tools": len(c["tools"])} for c in configs],
        "tools": tools
    }


if __name__ == "__main__":
    print(f"[MCP] Starting Logistics Platform MCP Server on port {MCP_PORT}")
    print(f"[MCP] Platform API: {PLATFORM_API}")
    print(f"[MCP] Registry: {REGISTRY_DIR}")
    uvicorn.run(app, host="0.0.0.0", port=MCP_PORT)
