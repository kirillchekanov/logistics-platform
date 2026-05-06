#!/usr/bin/env python3
"""
Logistics Platform — HTTP API Server
=====================================

Связывает партнёрский портал с адаптером и оркестратором.
Запуск: python3 api_server.py
Порт: 8001
"""

import json
import sys
import time
import hashlib
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    import httpx
except ImportError:
    httpx = None

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None

def get_db():
    """Получить соединение с PostgreSQL."""
    url = os.environ.get("DATABASE_URL")
    if not url or not psycopg2:
        return None
    try:
        return psycopg2.connect(url)
    except Exception as e:
        print(f"[DB] connect error: {e}")
        return None

def init_db():
    """Создать таблицы если не существуют."""
    conn = get_db()
    if not conn:
        print("[DB] Skipping DB init — no DATABASE_URL or psycopg2")
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS partners (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    display_name TEXT,
                    base_url TEXT,
                    config JSONB,
                    status TEXT DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS clients (
                    id TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL,
                    inn TEXT,
                    balance_rub DECIMAL DEFAULT 0,
                    reserved_rub DECIMAL DEFAULT 0,
                    contact_email TEXT,
                    contact_phone TEXT,
                    status TEXT DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS wallet_transactions (
                    id SERIAL PRIMARY KEY,
                    client_id TEXT REFERENCES clients(id),
                    type TEXT,
                    amount_rub DECIMAL,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
        conn.commit()
        print("[DB] Tables ready")
    except Exception as e:
        print(f"[DB] init error: {e}")
    finally:
        conn.close()

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
import uvicorn

from pydantic import BaseModel

# ── OAuth token cache ─────────────────────────────────────────────────────────

_TOKEN_CACHE: dict = {}

async def get_real_token(token_url: str, client_id: str, client_secret: str, scope: str = "") -> str:
    """Получить реальный OAuth2 токен через client_credentials."""
    cache_key = f"{token_url}:{client_id}"
    cached = _TOKEN_CACHE.get(cache_key)
    if cached and cached.get("expires_at", 0) > time.time() + 60:
        return cached["access_token"]

    if not httpx:
        return f"stub_token_{client_id[:8]}"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "scope": scope,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            token = data["access_token"]
            ttl = data.get("expires_in", 3600)
            _TOKEN_CACHE[cache_key] = {"access_token": token, "expires_at": time.time() + ttl}
            return token
    except Exception as e:
        print(f"[OAuth error] {e}")
        return f"stub_token_error"


async def call_real_api(base_url: str, method: str, path: str,
                         token: str, body: dict = None) -> dict:
    """Реальный HTTP вызов к API партнёра."""
    if not httpx:
        return {"error": "httpx not installed"}
    url = base_url.rstrip("/") + "/" + path.lstrip("/")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            if method.upper() in ("GET", "DELETE"):
                r = await getattr(client, method.lower())(url, headers=headers)
            else:
                r = await getattr(client, method.lower())(url, headers=headers, json=body or {})
            if r.is_success:
                return r.json()
            return {"error": r.status_code, "detail": r.text[:300]}
    except httpx.TimeoutException:
        return {"error": "timeout"}
    except Exception as e:
        return {"error": str(e)}


# Подключаем наш adapter и orchestrator
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / "http-adapter"))

# ── Загрузка реестра ──────────────────────────────────────────────────────────

REGISTRY_DIR = Path(__file__).parent / "registry"
ADAPTERS_DIR = Path(__file__).parent / "http-adapter" / "adapters"

def load_registry():
    registry = {}
    if REGISTRY_DIR.exists():
        for f in REGISTRY_DIR.glob("*.json"):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                registry[data["id"]] = data
            except Exception as e:
                print(f"[WARN] {f.name}: {e}")
    return registry

def load_adapters():
    adapters = {}
    if ADAPTERS_DIR.exists():
        for f in ADAPTERS_DIR.glob("*.json"):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                adapters[data["id"]] = data
            except Exception as e:
                print(f"[WARN] {f.name}: {e}")
    return adapters

REGISTRY = load_registry()
ADAPTERS = load_adapters()

# Override CDEK credentials from Railway environment variables
_cdek_client_id = os.getenv("CDEK_CLIENT_ID", "")
_cdek_client_secret = os.getenv("CDEK_CLIENT_SECRET", "")
if _cdek_client_id and "cdek" in ADAPTERS:
    ADAPTERS["cdek"]["auth"]["client_id"] = _cdek_client_id
    ADAPTERS["cdek"]["auth"]["client_secret"] = _cdek_client_secret
    print(f"[CDEK] Credentials loaded from env: {_cdek_client_id[:8]}...")
elif _cdek_client_id and "cdek" not in ADAPTERS:
    # Create CDEK adapter from env if not in files
    ADAPTERS["cdek"] = {
        "id": "cdek", "name": "СДЭК",
        "base_url": "https://api.edu.cdek.ru/v2",
        "status": "active",
        "auth": {
            "type": "oauth2_client_credentials",
            "token_url": "https://api.edu.cdek.ru/v2/oauth/token",
            "client_id": _cdek_client_id,
            "client_secret": _cdek_client_secret,
            "scope": "order:create order:delete order:read"
        },
        "actions": {
            "calculate_cost": {"method": "POST", "path": "/calculator/tarifflist"},
            "track": {"method": "GET", "path": "/orders/{order_uuid}"},
            "create_shipment": {"method": "POST", "path": "/orders"},
            "book_pickup": {"method": "POST", "path": "/intakes"},
            "cancel": {"method": "DELETE", "path": "/orders/{order_uuid}"},
            "get_slots": {"method": "GET", "path": "/deliverypoints"}
        }
    }
    REGISTRY["cdek"] = ADAPTERS["cdek"]
    print(f"[CDEK] Created adapter from env vars")

# Логи в памяти (для демо)
CALL_LOGS = [
    {"id": "req_001", "time": "12:24:08.214", "action": "create_shipment", "method": "POST",
     "path": "/v2/shipments", "status": 201, "latency": 218, "env": "Sandbox", "company": "darkstore"},
    {"id": "req_002", "time": "12:24:02.881", "action": "track_shipment", "method": "GET",
     "path": "/v2/shipments/NWX-8842116-A/track", "status": 200, "latency": 124, "env": "Sandbox", "company": "cdek"},
    {"id": "req_003", "time": "12:23:51.108", "action": "estimate_rate", "method": "POST",
     "path": "/v2/rates/estimate", "status": 502, "latency": 1240, "env": "Sandbox", "company": "pek",
     "error": {"code": "upstream_timeout", "message": "Upstream timed out after 1200ms"}},
    {"id": "req_004", "time": "12:23:44.660", "action": "calculate_cost", "method": "POST",
     "path": "/v2/calculator", "status": 200, "latency": 196, "env": "Sandbox", "company": "darkstore"},
    {"id": "req_005", "time": "12:23:32.918", "action": "book_pickup", "method": "POST",
     "path": "/v2/pickups", "status": 201, "latency": 312, "env": "Sandbox", "company": "cdek"},
]

# ── FastAPI ───────────────────────────────────────────────────────────────────

app = FastAPI(title="Logistics Platform API", version="0.1")

@app.on_event("startup")
def on_startup():
    init_db()
    conn = get_db()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id, config FROM partners WHERE status='active'")
                for row in cur.fetchall():
                    REGISTRY[row[0]] = row[1]
            print(f"[DB] Loaded {len(REGISTRY)} partners from DB")
        except Exception as e:
            print(f"[DB] load error: {e}")
        finally:
            conn.close()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Models ────────────────────────────────────────────────────────────────────

class ExecuteActionRequest(BaseModel):
    company_id: str
    action: str
    params: dict = {}
    dry_run: bool = True

class RegisterCompanyRequest(BaseModel):
    config_json: str
    dry_run: bool = True

class UpdateCompanyRequest(BaseModel):
    company_id: str
    field: str
    value: str

class BuildChainRequest(BaseModel):
    from_city: str
    to_city: str
    weight_kg: float
    available_company_ids: list[str]
    optimize_for: str = "time"
    deadline_hours: Optional[int] = None

# ── Helpers ───────────────────────────────────────────────────────────────────

def stub_response(company_id: str, action: str, params: dict, dry_run: bool) -> dict:
    """Генерирует реалистичный stub-ответ."""
    ts = datetime.utcnow().isoformat()
    sid = f"STUB-{company_id.upper()}-{int(time.time())}"

    stubs = {
        "create_shipment": {
            "data": {
                "shipment_id": sid,
                "tracking_number": f"TRK{int(time.time()) % 1000000:06d}",
                "status": "created",
                "created_at": ts,
            }
        },
        "book_pickup": {
            "data": {
                "pickup_id": f"PICKUP-{sid}",
                "confirmed": True,
                "pickup_date": params.get("pickup_date", ts[:10]),
                "time_window": f"{params.get('time_from','10:00')}–{params.get('time_to','16:00')}",
            }
        },
        "create_reception": {
            "data": {
                "reception_id": f"RECEPT-{sid}",
                "status": "scheduled",
                "warehouse_address": f"ул. Складская 1, {params.get('to_city', 'Москва')}",
            }
        },
        "track": {"data": {"status": "В пути", "status_code": "IN_TRANSIT", "location": params.get("to_city", "Москва"), "updated_at": ts}},
        "track_shipment": {"data": {"status": "In transit", "code": "IN_TRANSIT", "location": "Moscow", "updated_at": ts}},
        "cancel": {"data": {"cancelled": True, "cancelled_at": ts}},
        "cancel_shipment": {"data": {"is_deleted": True, "cancelled_at": ts}},
        "calculate_cost": {"data": {"cost_rub": 1850.0, "delivery_days_min": 1, "delivery_days_max": 2, "currency": "RUB"}},
        "estimate_rate": {"data": {"total_sum": 2100.0, "period_min": 2, "period_max": 4, "currency": "RUB"}},
        "get_slots": {
            "data": {
                "available_dates": [
                    (datetime.utcnow().replace(day=datetime.utcnow().day+1)).strftime("%Y-%m-%d"),
                    (datetime.utcnow().replace(day=datetime.utcnow().day+2)).strftime("%Y-%m-%d"),
                ],
                "time_from": "09:00",
                "time_to": "17:00",
            }
        },
        "get_label": {"data": {"label_url": f"https://api.example.com/labels/{sid}.pdf", "format": "PDF"}},
        "list_tariffs": {"data": {"tariffs": [{"code": "express", "price": 2500}, {"code": "standard", "price": 1200}, {"code": "economy", "price": 850}]}},
    }

    result = {
        "action": action,
        "company_id": company_id,
        "dry_run": dry_run,
        "success": True,
        "stub": True,
        **(stubs.get(action) or {"data": {"result": "ok"}}),
        "timestamp": ts,
    }

    if dry_run:
        company = ADAPTERS.get(company_id) or REGISTRY.get(company_id, {})
        base_url = company.get("base_url", f"https://api.{company_id}.ru/v2")
        action_cfg = (company.get("actions") or {}).get(action, {})
        path = action_cfg.get("path", f"/{action}") if action_cfg else f"/{action}"
        method = action_cfg.get("method", "POST") if action_cfg else "POST"
        result["will_call"] = {"method": method, "url": f"{base_url}{path}"}
        result["message"] = "Preview only. Call with dry_run=false for real execution."

    return result

def add_log(company_id: str, action: str, method: str, path: str, status: int, latency: int):
    """Добавляет запись в лог."""
    CALL_LOGS.insert(0, {
        "id": f"req_{hashlib.md5(f'{time.time()}'.encode()).hexdigest()[:8]}",
        "time": datetime.utcnow().strftime("%H:%M:%S.%f")[:12],
        "action": action,
        "method": method,
        "path": path,
        "status": status,
        "latency": latency,
        "env": "Sandbox",
        "company": company_id,
    })
    # Держим последние 50
    while len(CALL_LOGS) > 50:
        CALL_LOGS.pop()

# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/api/status")
def root():
    return {"status": "ok", "service": "Logistics Platform API", "version": "0.1"}

@app.get("/health")
def health():
    return {
        "status": "ok",
        "registry_companies": len(REGISTRY),
        "adapters": len(ADAPTERS),
        "timestamp": datetime.utcnow().isoformat(),
    }

# ── Companies (реестр) ────────────────────────────────────────────────────────

@app.get("/companies")
def list_companies():
    """Список всех компаний — из реестра оркестратора + адаптеры."""
    result = []

    for cid, co in REGISTRY.items():
        adapter = ADAPTERS.get(cid, {})
        result.append({
            "id": cid,
            "name": co.get("name"),
            "display_name": co.get("display_name"),
            "status": co.get("status", "active"),
            "capabilities": co.get("capabilities", []),
            "coverage_cities": len(co.get("coverage", {}).get("cities", [])),
            "actions": list((adapter.get("actions") or {}).keys()) or co.get("adapter", {}).get("tools", []),
            "base_url": adapter.get("base_url") or co.get("adapter", {}).get("mcp_endpoint", ""),
            "token_url": adapter.get("auth", {}).get("token_url", ""),
            "scope": adapter.get("auth", {}).get("scope", ""),
            "max_weight_kg": co.get("constraints", {}).get("max_weight_kg"),
            "calls_today": sum(1 for l in CALL_LOGS if l.get("company") == cid),
            "token_ttl": "46m 12s" if cid == "darkstore" else "23m 41s" if cid == "cdek" else "—",
        })

    # Добавляем компании только из адаптеров (если нет в реестре)
    for cid, ad in ADAPTERS.items():
        if cid not in REGISTRY:
            result.append({
                "id": cid,
                "name": ad.get("name"),
                "display_name": ad.get("name"),
                "status": "active",
                "capabilities": [],
                "actions": list((ad.get("actions") or {}).keys()),
                "base_url": ad.get("base_url", ""),
                "token_url": ad.get("auth", {}).get("token_url", ""),
                "scope": ad.get("auth", {}).get("scope", ""),
                "calls_today": sum(1 for l in CALL_LOGS if l.get("company") == cid),
                "token_ttl": "—",
            })

    return {"companies": result, "total": len(result)}

@app.get("/companies/{company_id}")
def get_company(company_id: str):
    co = REGISTRY.get(company_id) or ADAPTERS.get(company_id)
    if not co:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")
    actions_list = list((co.get("actions") or {}).keys()) if isinstance(co.get("actions"), dict) else (co.get("actions") or [])
    return {
        "id": co.get("id", company_id),
        "name": co.get("name", company_id),
        "display_name": co.get("display_name", ""),
        "base_url": co.get("base_url", ""),
        "token_url": co.get("auth", {}).get("token_url", "") if isinstance(co.get("auth"), dict) else co.get("token_url", ""),
        "scope": co.get("auth", {}).get("scope", "") if isinstance(co.get("auth"), dict) else co.get("scope", ""),
        "status": co.get("status", "active"),
        "capabilities": co.get("capabilities", []),
        "actions": actions_list,
        "calls_today": co.get("calls_today", 0),
        "token_ttl": co.get("token_ttl", "59m 00s"),
    }

@app.get("/companies/{company_id}/actions")
def get_company_actions(company_id: str):
    adapter = ADAPTERS.get(company_id)
    if not adapter:
        raise HTTPException(status_code=404, detail=f"No adapter for {company_id}")
    actions = []
    for name, cfg in (adapter.get("actions") or {}).items():
        import re
        tmpl = json.dumps(cfg.get("body_template") or "") + cfg.get("path", "")
        params = sorted(set(re.findall(r"\{\{(\w+)\}\}", tmpl)))
        actions.append({
            "name": name,
            "method": cfg.get("method", "POST"),
            "path": cfg.get("path", ""),
            "required_params": params,
            "response_fields": list((cfg.get("response_map") or {}).keys()),
        })
    return {"company_id": company_id, "actions": actions}

# ── Execute action ────────────────────────────────────────────────────────────

@app.post("/execute")
async def execute_action(req: ExecuteActionRequest):
    """Выполнить действие через адаптер компании."""
    t_start = time.time()

    if req.company_id not in REGISTRY and req.company_id not in ADAPTERS:
        raise HTTPException(status_code=404, detail=f"Company {req.company_id} not found")

    adapter = ADAPTERS.get(req.company_id)
    real_call_attempted = False

    if adapter and not req.dry_run:
        auth = adapter.get("auth", {})
        # Берём credentials: сначала из конфига (sandbox), потом из env
        client_id = (auth.get("client_id") or
                     os.environ.get(auth.get("client_id_env",""), ""))
        client_secret = (auth.get("client_secret") or
                         os.environ.get(auth.get("client_secret_env",""), ""))
        token_url = auth.get("token_url","")

        if client_id and client_secret and token_url and not client_id.startswith("stub_"):
            try:
                token = await get_real_token(token_url, client_id, client_secret, auth.get("scope",""))
                action_cfg = (adapter.get("actions") or {}).get(req.action, {})
                if action_cfg and not token.startswith("stub_"):
                    import re
                    path = action_cfg.get("path","/" + req.action)
                    # Подставляем параметры в path
                    path = re.sub(r"\{\{(\w+)\}\}", lambda m: str(req.params.get(m.group(1), m.group(0))), path)
                    body = action_cfg.get("body_template")
                    if body:
                        body = json.loads(re.sub(r"\{\{(\w+)\}\}", lambda m: json.dumps(req.params.get(m.group(1), m.group(0))), json.dumps(body)))
                    raw = await call_real_api(adapter["base_url"], action_cfg.get("method","POST"), path, token, body)
                    if "error" not in raw:
                        # Маппинг ответа
                        def extract(data, path):
                            import re as re2
                            for part in re2.split(r"\.", path):
                                m = re2.match(r"^(\w+)\[(\d+)\]$", part)
                                if m: data = (data or {}).get(m.group(1), []); data = data[int(m.group(2))] if isinstance(data,list) and int(m.group(2))<len(data) else None
                                else: data = (data or {}).get(part)
                            return data
                        rmap = action_cfg.get("response_map",{})
                        mapped = {k: extract(raw,v) for k,v in rmap.items()} if rmap else raw
                        result = {"action":req.action,"company_id":req.company_id,"success":True,"stub":False,"data":mapped,"raw":raw}
                        real_call_attempted = True
                    else:
                        result = {"action":req.action,"company_id":req.company_id,"success":False,"error":raw.get("error"),"stub":False}
                        real_call_attempted = True
            except Exception as e:
                result = {"success":False,"error":str(e),"company_id":req.company_id,"action":req.action}
                real_call_attempted = True

    if not real_call_attempted:
        result = stub_response(req.company_id, req.action, req.params, req.dry_run)

    # Логируем
    latency = int((time.time() - t_start) * 1000) + 180  # +base latency для реализма
    adapter_data = adapter or {}
    action_cfg = (adapter_data.get("actions") or {}).get(req.action, {})
    path = action_cfg.get("path", f"/{req.action}") if action_cfg else f"/{req.action}"
    method = action_cfg.get("method", "POST") if action_cfg else "POST"
    status = 200 if result.get("success") else 500

    if not req.dry_run:
        add_log(req.company_id, req.action, method, path, status, latency)

    return result

@app.get("/schema/{company_id}/{action}")
def get_action_schema(company_id: str, action: str):
    """Схема параметров для действия компании."""
    import re
    adapter = ADAPTERS.get(company_id)
    if not adapter:
        # Fallback для компаний только в реестре
        default_params = {
            "create_shipment": ["from_city","to_city","weight_g","length_cm","sender_name","sender_phone","recipient_name","recipient_phone"],
            "book_pickup": ["shipment_id","pickup_date","time_from","time_to","contact_name"],
            "track": ["tracking_number"],
            "cancel": ["shipment_id"],
            "calculate_cost": ["from_city","to_city","weight_g"],
            "get_slots": ["city","date"],
        }
        return {
            "company_id": company_id,
            "action": action,
            "required_params": default_params.get(action, []),
            "response_fields": ["result"],
            "stub": True,
        }

    action_cfg = (adapter.get("actions") or {}).get(action)
    if not action_cfg:
        available = list((adapter.get("actions") or {}).keys())
        raise HTTPException(status_code=404, detail=f"Action {action} not found. Available: {available}")

    tmpl = json.dumps(action_cfg.get("body_template") or "") + action_cfg.get("path", "")
    params = sorted(set(re.findall(r"\{\{(\w+)\}\}", tmpl)))
    return {
        "company_id": company_id,
        "action": action,
        "method": action_cfg.get("method"),
        "path": action_cfg.get("path"),
        "required_params": params,
        "response_fields": list((action_cfg.get("response_map") or {}).keys()),
    }

# ── Register company ──────────────────────────────────────────────────────────


# ── Auto-config generator ─────────────────────────────────────────────────────

REGISTRY_DIR = Path(__file__).parent / "registry"
REGISTRY_DIR.mkdir(exist_ok=True)

MCP_TOOL_SCHEMAS = {
    "create_shipment": {
        "description": "Создать новое отправление у партнёра",
        "inputSchema": {
            "type": "object",
            "properties": {
                "sender":   {"type": "object", "description": "Адрес и контакты отправителя"},
                "receiver": {"type": "object", "description": "Адрес и контакты получателя"},
                "parcels":  {"type": "array",  "description": "Список посылок с весом и размерами"},
                "service_code": {"type": "string", "description": "Тариф доставки: express, standard, economy"},
                "reference": {"type": "string", "description": "Внешний ID заказа (опционально)"}
            },
            "required": ["sender", "receiver", "parcels"]
        }
    },
    "book_pickup": {
        "description": "Забронировать забор посылки курьером",
        "inputSchema": {
            "type": "object",
            "properties": {
                "date":    {"type": "string", "description": "Дата забора YYYY-MM-DD"},
                "address": {"type": "object", "description": "Адрес забора"}
            },
            "required": ["date", "address"]
        }
    },
    "track": {
        "description": "Получить статус и местоположение отправления",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tracking_number": {"type": "string", "description": "Номер отправления для трекинга"}
            },
            "required": ["tracking_number"]
        }
    },
    "calculate_cost": {
        "description": "Рассчитать стоимость доставки",
        "inputSchema": {
            "type": "object",
            "properties": {
                "origin":      {"type": "string", "description": "Почтовый индекс отправителя"},
                "destination": {"type": "string", "description": "Почтовый индекс получателя"},
                "weight":      {"type": "number", "description": "Вес посылки в граммах"},
                "dimensions":  {"type": "object", "description": "Размеры (length, width, height) в мм"}
            },
            "required": ["origin", "destination", "weight"]
        }
    },
    "cancel": {
        "description": "Отменить созданное отправление",
        "inputSchema": {
            "type": "object",
            "properties": {
                "shipment_id": {"type": "string", "description": "ID отправления для отмены"}
            },
            "required": ["shipment_id"]
        }
    },
    "get_slots": {
        "description": "Получить доступные временные слоты для доставки или забора",
        "inputSchema": {
            "type": "object",
            "properties": {
                "date":    {"type": "string", "description": "Дата YYYY-MM-DD (по умолчанию сегодня)"},
                "address": {"type": "object", "description": "Адрес для проверки слотов"}
            }
        }
    },
    "create_reception": {
        "description": "Зафиксировать поступление товара на склад",
        "inputSchema": {
            "type": "object",
            "properties": {
                "items": {"type": "array", "description": "Список товаров с артикулами и количеством"},
                "warehouse_id": {"type": "string", "description": "ID склада"}
            },
            "required": ["items"]
        }
    },
    "store": {
        "description": "Разместить товар на хранение в ячейке склада",
        "inputSchema": {
            "type": "object",
            "properties": {
                "item_id":  {"type": "string", "description": "Артикул товара"},
                "quantity": {"type": "integer", "description": "Количество единиц"},
                "location": {"type": "string", "description": "Ячейка хранения (опционально)"}
            },
            "required": ["item_id", "quantity"]
        }
    },
    "ship_from_warehouse": {
        "description": "Собрать и отправить заказ со склада",
        "inputSchema": {
            "type": "object",
            "properties": {
                "order_id": {"type": "string", "description": "ID заказа для отгрузки"},
                "carrier":  {"type": "string", "description": "Перевозчик для доставки"}
            },
            "required": ["order_id"]
        }
    }
}

def generate_mcp_config(config: dict) -> dict:
    """Генерирует MCP конфиг из данных онбординга партнёра."""
    partner_id = config["id"]
    partner_name = config.get("name", partner_id)
    base_url = config.get("base_url", "")
    actions = config.get("actions", {})

    tools = []
    for action_name, action_cfg in actions.items():
        schema = MCP_TOOL_SCHEMAS.get(action_name, {
            "description": f"Вызвать {action_name} у партнёра {partner_name}",
            "inputSchema": {"type": "object", "properties": {}, "required": []}
        })
        tool = {
            "name": f"{partner_id}__{action_name}",
            "description": f"[{partner_name}] {schema['description']}",
            "inputSchema": schema["inputSchema"],
            "_meta": {
                "partner_id": partner_id,
                "action": action_name,
                "method": action_cfg.get("method", "POST"),
                "path": action_cfg.get("path", f"/{action_name}"),
                "base_url": base_url,
                "response_map": config.get("response_map", {})
            }
        }
        tools.append(tool)

    mcp_config = {
        "partner_id": partner_id,
        "partner_name": partner_name,
        "base_url": base_url,
        "capabilities": config.get("capabilities", []),
        "status": config.get("status", "pending"),
        "tools": tools,
        "auth": {
            "type": "oauth2_client_credentials",
            "token_url": config.get("auth", {}).get("token_url", ""),
            "client_id": config.get("auth", {}).get("client_id", ""),
            "scope": config.get("auth", {}).get("scope", "")
        },
        "generated_at": datetime.utcnow().isoformat() + "Z"
    }
    return mcp_config

def save_mcp_config(partner_id: str, mcp_config: dict) -> Path:
    """Сохраняет MCP конфиг в registry/."""
    path = REGISTRY_DIR / f"{partner_id}.json"
    path.write_text(json.dumps(mcp_config, ensure_ascii=False, indent=2))
    print(f"[MCP] Config saved: {path} ({len(mcp_config['tools'])} tools)")
    return path

@app.post("/register")
def register_company(req: RegisterCompanyRequest):
    """Добавить новую компанию в реестр адаптеров."""
    try:
        config = json.loads(req.config_json)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    required = ["id", "name", "base_url", "auth", "actions"]
    missing = [f for f in required if f not in config]
    if missing:
        raise HTTPException(status_code=422, detail=f"Missing fields: {missing}")

    required_actions = ["create_shipment", "track"]
    missing_actions = [a for a in required_actions if a not in (config.get("actions") or {})]
    if missing_actions:
        raise HTTPException(status_code=422, detail=f"Required actions missing: {missing_actions}")

    company_id = config["id"]

    if req.dry_run:
        return {
            "status": "valid",
            "dry_run": True,
            "company_id": company_id,
            "company_name": config.get("name"),
            "actions_count": len(config.get("actions", {})),
            "actions": list(config.get("actions", {}).keys()),
            "message": "Config is valid. Call with dry_run=false to save.",
        }

    # Сохраняем адаптер в БД и в память
    conn = get_db()
    if conn:
        try:
            import json as _json
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO partners (id, name, display_name, base_url, config, status)
                       VALUES (%s, %s, %s, %s, %s, 'active')
                       ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name,
                       display_name=EXCLUDED.display_name, base_url=EXCLUDED.base_url,
                       config=EXCLUDED.config""",
                    (company_id, config.get('name'), config.get('display_name'),
                     config.get('base_url'), _json.dumps(config))
                )
            conn.commit()
            print(f"[DB] Partner {company_id} saved to DB")
        except Exception as e:
            print(f"[DB] save error: {e}")
        finally:
            conn.close()
    REGISTRY[company_id] = config
    if ADAPTERS_DIR.exists():
        adapter_path = ADAPTERS_DIR / f"{company_id}.json"
        try:
            adapter_path.write_text(json.dumps(config, ensure_ascii=False, indent=2))
        except Exception:
            pass
    ADAPTERS[company_id] = config

    # Generate and save MCP config
    mcp_config = generate_mcp_config(config)
    mcp_path = save_mcp_config(company_id, mcp_config)
    print(f"[AUTOCONFIG] {company_id}: {len(mcp_config['tools'])} MCP tools generated")

    # Send welcome email (async, non-blocking)
    partner_email = config.get("contact_email") or config.get("email") or ""
    if partner_email:
        import asyncio as _asyncio
        try:
            loop = _asyncio.get_event_loop()
            loop.create_task(send_email(
                partner_email,
                f"Добро пожаловать на платформу — {config.get('name')}",
                email_welcome(config.get('name',''), company_id, partner_email)
            ))
        except Exception:
            pass

    return {
        "status": "registered",
        "company_id": company_id,
        "company_name": config.get("name"),
        "actions": list(config.get("actions", {}).keys()),
        "mcp_tools": [t["name"] for t in mcp_config["tools"]],
        "mcp_config_path": str(mcp_path),
        "message": f"Company {config.get('name')} registered. {len(mcp_config['tools'])} MCP tools generated.",
    }

@app.put("/companies/{company_id}")
def update_company(company_id: str, req: UpdateCompanyRequest):
    """Обновить поле компании."""
    import re as re_module
    co = REGISTRY.get(company_id) or ADAPTERS.get(company_id)
    if not co:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

    parts = req.field.split(".")
    try:
        parsed = json.loads(req.value)
    except (json.JSONDecodeError, ValueError):
        parsed = req.value

    target = co
    for part in parts[:-1]:
        target = target.setdefault(part, {})
    old = target.get(parts[-1])
    target[parts[-1]] = parsed

    # Сохраняем
    if company_id in REGISTRY:
        path = REGISTRY_DIR / f"{company_id}.json"
        path.write_text(json.dumps(REGISTRY[company_id], ensure_ascii=False, indent=2))
    if company_id in ADAPTERS:
        path = ADAPTERS_DIR / f"{company_id}.json"
        path.write_text(json.dumps(ADAPTERS[company_id], ensure_ascii=False, indent=2))

    return {"status": "updated", "company_id": company_id, "field": req.field, "old_value": old, "new_value": parsed}

# ── Orchestrator ──────────────────────────────────────────────────────────────

@app.get("/orchestrator/capable")
def get_capable_companies(from_city: str, to_city: str, weight_kg: float,
                           deadline_hours: Optional[int] = None,
                           required_capability: Optional[str] = None):
    """Найти компании способные выполнить задачу."""
    suitable = []
    excluded = []

    for cid, co in REGISTRY.items():
        reasons_out = []
        cities = [c.lower() for c in co.get("coverage", {}).get("cities", [])]

        if from_city.lower() not in cities:
            reasons_out.append(f"no coverage in {from_city}")
        if to_city.lower() not in cities:
            reasons_out.append(f"no coverage in {to_city}")

        max_w = co.get("constraints", {}).get("max_weight_kg", 9999)
        if weight_kg > max_w:
            reasons_out.append(f"weight {weight_kg}kg exceeds limit {max_w}kg")

        if required_capability:
            caps = co.get("capabilities", [])
            if required_capability not in caps:
                reasons_out.append(f"capability {required_capability} not available")

        if reasons_out:
            excluded.append({"company_id": cid, "name": co.get("display_name"), "reasons": reasons_out})
            continue

        td = co.get("time_windows", {}).get("transit_days", {})
        transit = td.get(f"{from_city}-{to_city}") or td.get(f"{to_city}-{from_city}")

        suitable.append({
            "company_id": cid,
            "name": co.get("name"),
            "display_name": co.get("display_name"),
            "capabilities": co.get("capabilities", []),
            "transit_days": transit,
            "max_weight_kg": max_w,
            "meets_deadline": (transit * 24 <= deadline_hours) if (transit and deadline_hours) else None,
        })

    suitable.sort(key=lambda x: (0 if x.get("meets_deadline") else 1, x.get("transit_days") or 999))
    return {"from_city": from_city, "to_city": to_city, "weight_kg": weight_kg,
            "suitable": suitable, "excluded": excluded,
            "next_action": "get_slots then build_chain" if suitable else "no_options"}

@app.get("/orchestrator/build-chain")
def build_chain(from_city: str, to_city: str, weight_kg: float,
                company_ids: str, optimize_for: str = "time"):
    """Построить варианты логистической цепочки."""
    ids = [c.strip() for c in company_ids.split(",") if c.strip()]
    companies = {cid: REGISTRY[cid] for cid in ids if cid in REGISTRY}

    dist_map = {"Самара-Москва": 850, "Москва-СПб": 700, "Москва-Казань": 800,
                "Екатеринбург-Москва": 1400, "Новосибирск-Москва": 2800}
    dist = dist_map.get(f"{from_city}-{to_city}") or dist_map.get(f"{to_city}-{from_city}") or 600

    chains = []
    PLATFORM_FEE = 150.0

    for cid, co in companies.items():
        cities = [c.lower() for c in co.get("coverage", {}).get("cities", [])]
        if from_city.lower() in cities and to_city.lower() in cities:
            td = co.get("time_windows", {}).get("transit_days", {})
            transit = td.get(f"{from_city}-{to_city}") or td.get(f"{to_city}-{from_city}") or 2
            pricing = co.get("pricing", {})
            cost = max(
                weight_kg * pricing.get("base_per_kg_rub", 30) + dist * pricing.get("base_per_km_rub", 0.5),
                pricing.get("min_order_rub", 300)
            )
            grand = round(cost + PLATFORM_FEE)
            chains.append({
                "chain_id": f"chain_{cid}_{int(time.time())}",
                "type": "direct",
                "label": f"Direct via {co.get('display_name')}",
                "steps": [{"step": 1, "company_id": cid, "company_name": co.get("display_name"),
                            "role": "full_delivery", "from": from_city, "to": to_city}],
                "transit_days": transit,
                "cost_rub": round(cost),
                "platform_fee_rub": PLATFORM_FEE,
                "grand_total_rub": grand,
                "recommended": False,
            })

    # Relay chains
    carriers = [cid for cid, co in companies.items()
                if "intercity_transit" in co.get("capabilities", []) or
                   "pickup_from_client" in co.get("capabilities", [])]
    ds_ids = [cid for cid, co in companies.items()
              if "last_mile_delivery" in co.get("capabilities", [])]

    for carrier_id in carriers:
        for ds_id in ds_ids:
            if carrier_id == ds_id:
                continue
            carrier = companies[carrier_id]
            ds = companies[ds_id]
            td = carrier.get("time_windows", {}).get("transit_days", {})
            transit = td.get(f"{from_city}-{to_city}") or td.get(f"{to_city}-{from_city}") or 2
            cp = carrier.get("pricing", {})
            dp = ds.get("pricing", {})
            c_cost = max(weight_kg * cp.get("base_per_kg_rub", 30) + dist * cp.get("base_per_km_rub", 0.5), cp.get("min_order_rub", 300))
            d_cost = max(weight_kg * dp.get("base_per_kg_rub", 89) * 0.3, dp.get("min_order_rub", 200))
            total = c_cost + d_cost
            grand = round(total + PLATFORM_FEE * 2)
            chains.append({
                "chain_id": f"chain_{carrier_id}_{ds_id}_{int(time.time())}",
                "type": "relay",
                "label": f"{carrier.get('display_name')} → {ds.get('display_name')} last-mile",
                "steps": [
                    {"step": 1, "company_id": carrier_id, "company_name": carrier.get("display_name"),
                     "role": "transit", "from": from_city, "to": f"СЦ {to_city}"},
                    {"step": 2, "company_id": ds_id, "company_name": ds.get("display_name"),
                     "role": "last_mile", "from": f"СЦ {to_city}", "to": to_city},
                ],
                "transit_days": transit,
                "cost_rub": round(total),
                "platform_fee_rub": PLATFORM_FEE * 2,
                "grand_total_rub": grand,
                "recommended": False,
            })

    if not chains:
        return {"status": "no_chains", "message": "Cannot build chain from available companies"}

    sort_key = {"time": lambda c: c.get("transit_days", 99),
                "cost": lambda c: c.get("grand_total_rub", 9999),
                "reliability": lambda c: -len(c.get("steps", []))}.get(optimize_for, lambda c: c.get("transit_days", 99))
    chains.sort(key=sort_key)
    chains = chains[:3]
    chains[0]["recommended"] = True

    return {"status": "ok", "chains": chains, "optimize_for": optimize_for,
            "recommended_chain_id": chains[0]["chain_id"]}

# ── Logs ──────────────────────────────────────────────────────────────────────

@app.get("/logs")
def get_logs(company_id: Optional[str] = None, limit: int = 20):
    logs = CALL_LOGS
    if company_id:
        logs = [l for l in logs if l.get("company") == company_id]
    stats = {
        "total": len(CALL_LOGS),
        "errors": sum(1 for l in CALL_LOGS if l.get("status", 0) >= 400),
        "avg_latency": int(sum(l.get("latency", 0) for l in CALL_LOGS) / max(len(CALL_LOGS), 1)),
        "success_rate": round(sum(1 for l in CALL_LOGS if l.get("status", 0) < 400) / max(len(CALL_LOGS), 1) * 100, 2),
    }
    return {"logs": logs[:limit], "stats": stats, "total": len(logs)}

# ── Wallet (stub) ─────────────────────────────────────────────────────────────

WALLETS = {
    "client_001": {"balance_rub": 15000.0, "reserved_rub": 0.0, "company_name": "ООО Ромашка"},
    "client_002": {"balance_rub": 800.0, "reserved_rub": 0.0, "company_name": "ИП Петров"},
}
HOLDS = {}

@app.get("/wallet/{client_id}")
def get_balance(client_id: str):
    w = WALLETS.get(client_id)
    if not w:
        raise HTTPException(status_code=404, detail=f"Client {client_id} not found")
    return {**w, "available_rub": w["balance_rub"] - w["reserved_rub"], "client_id": client_id}

@app.post("/wallet/{client_id}/hold")
def hold_funds(client_id: str, amount_rub: float, chain_id: str = ""):
    w = WALLETS.get(client_id)
    if not w:
        raise HTTPException(status_code=404, detail=f"Client {client_id} not found")
    available = w["balance_rub"] - w["reserved_rub"]
    if amount_rub > available:
        raise HTTPException(status_code=402, detail=f"Insufficient funds: need {amount_rub}, available {available}")
    hold_id = hashlib.md5(f"{client_id}{chain_id}{time.time()}".encode()).hexdigest()[:12]
    w["reserved_rub"] += amount_rub
    HOLDS[hold_id] = {"client_id": client_id, "chain_id": chain_id, "amount": amount_rub}
    return {"status": "held", "hold_id": hold_id, "held_rub": amount_rub,
            "balance_after": {"available_rub": w["balance_rub"] - w["reserved_rub"]}}


@app.get("/partner/{partner_id}", response_class=HTMLResponse)
def serve_partner_lk(partner_id: str):
    """Партнёрский личный кабинет."""
    p = Path(__file__).parent / "partner-lk.html"
    if p.exists():
        content = p.read_text(encoding="utf-8")
        print(f"[LK] Serving partner-lk.html: {len(content)} bytes for {partner_id}", flush=True)
        return HTMLResponse(content=content)
    print(f"[LK] partner-lk.html NOT FOUND at {p}", flush=True)
    return HTMLResponse(content="<h1>partner-lk.html not found</h1>", status_code=404)

@app.get("/portal", response_class=HTMLResponse)
def serve_portal():
    """Внутренний дашборд платформы — управление всеми партнёрами."""
    p = Path(__file__).parent / "portal.html"
    if p.exists():
        return HTMLResponse(content=p.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>portal.html not found</h1>", status_code=404)


@app.get("/join", response_class=HTMLResponse)
def serve_join():
    """Страница для нового партнёра — только онбординг."""
    p = Path(__file__).parent / "portal.html"
    if not p.exists():
        return HTMLResponse(content="<h1>portal.html not found</h1>", status_code=404)
    html = p.read_text(encoding="utf-8")
    # Инжектируем mode=partner через мета-тег
    html = html.replace(
        "<title>Logistics Platform · Partner Portal</title>",
        "<title>Подключиться к Logistics API Platform</title>"
    )
    html = html.replace(
        "const APP_MODE = (() => {",
        "const _FORCE_PARTNER = true; const APP_MODE = (() => {"
    )
    html = html.replace(
        "if (path.endsWith('/join') || params.get('mode') === 'partner') return 'partner';",
        "if (_FORCE_PARTNER || path.endsWith('/join') || params.get('mode') === 'partner') return 'partner';"
    )
    return HTMLResponse(content=html)


@app.get("/", response_class=HTMLResponse)
def serve_root():
    """Редирект на /join для новых пользователей."""
    return HTMLResponse(content="""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>Logistics API Platform</title>
<style>
  body{font-family:-apple-system,sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#f7f8fa}
  .card{background:#fff;border:1px solid #e6e8ee;border-radius:12px;padding:40px;text-align:center;max-width:400px;box-shadow:0 2px 8px rgba(0,0,0,.06)}
  h1{font-size:22px;margin-bottom:8px;color:#0f172a}
  p{color:#64748b;margin-bottom:24px;font-size:14px;line-height:1.6}
  .btn{display:inline-block;padding:10px 24px;border-radius:8px;font-size:14px;font-weight:600;text-decoration:none;margin:4px}
  .primary{background:#0f172a;color:#fff}
  .secondary{background:#f1f3f7;color:#334155;border:1px solid #e6e8ee}
</style>
</head><body>
<div class="card">
  <div style="font-size:32px;margin-bottom:16px">⚡</div>
  <h1>Logistics API Platform</h1>
  <p>Подключите ваш логистический API и агент платформы начнёт включать вас в цепочки доставки клиентов.</p>
  <a href="/join" class="btn primary">Подключить компанию →</a>
  <a href="/portal" class="btn secondary">Войти в платформу</a>
</div>
</body></html>""")

# ── Run ───────────────────────────────────────────────────────────────────────

# ── HTTP Adapter ──────────────────────────────────────────────────────────────
# Принимает вызовы от AI агента, получает токен, вызывает реальный API партнёра

import asyncio
from typing import Any

_token_cache: dict = {}  # {partner_id: {token, expires_at}}

async def get_oauth_token(partner_id: str, auth_cfg: dict) -> str:
    """Получить OAuth2 client_credentials токен (с кэшированием)."""
    cached = _token_cache.get(partner_id, {})
    if cached.get("token") and cached.get("expires_at", 0) > time.time() + 60:
        return cached["token"]

    token_url = auth_cfg.get("token_url", "")
    client_id = auth_cfg.get("client_id", "")
    client_secret = auth_cfg.get("client_secret", "")
    scope = auth_cfg.get("scope", "")

    if not token_url or not client_id:
        raise HTTPException(status_code=400, detail=f"OAuth not configured for {partner_id}")

    if httpx is None:
        raise HTTPException(status_code=500, detail="httpx not installed")

    data = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
    if scope:
        data["scope"] = scope

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(token_url, data=data)

    if resp.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"OAuth token failed for {partner_id}: {resp.status_code} {resp.text[:200]}"
        )

    token_data = resp.json()
    token = token_data.get("access_token", "")
    expires_in = token_data.get("expires_in", 3600)
    _token_cache[partner_id] = {"token": token, "expires_at": time.time() + expires_in}
    print(f"[ADAPTER] Token obtained for {partner_id}, expires in {expires_in}s")
    return token


class AdapterCallRequest(BaseModel):
    partner_id: str
    action: str
    params: dict = {}
    dry_run: bool = False


@app.post("/adapter/call")
async def adapter_call(req: AdapterCallRequest):
    """Выполнить action у партнёра через HTTP adapter."""
    partner_id = req.partner_id
    action = req.action

    # Загружаем конфиг партнёра
    config = REGISTRY.get(partner_id) or ADAPTERS.get(partner_id)
    if not config:
        # Пробуем загрузить из MCP registry
        mcp_path = REGISTRY_DIR / f"{partner_id}.json"
        if mcp_path.exists():
            mcp_cfg = json.loads(mcp_path.read_text())
            tool = next((t for t in mcp_cfg.get("tools", []) if t["name"].endswith(f"__{action}")), None)
            if tool:
                meta = tool.get("_meta", {})
                base_url = meta.get("base_url", "")
                method = meta.get("method", "POST")
                path = meta.get("path", f"/{action}")
                auth_cfg = mcp_cfg.get("auth", {})
            else:
                raise HTTPException(status_code=404, detail=f"Action {action} not found for {partner_id}")
        else:
            raise HTTPException(status_code=404, detail=f"Partner {partner_id} not found")
    else:
        actions = config.get("actions", {})
        if action not in actions:
            raise HTTPException(status_code=404, detail=f"Action {action} not configured for {partner_id}")
        action_cfg = actions[action]
        base_url = config.get("base_url", "")
        method = action_cfg.get("method", "POST")
        path = action_cfg.get("path", f"/{action}")
        auth_cfg = config.get("auth", {})

    # Подставляем path параметры из params
    import re
    for key, val in req.params.items():
        path = re.sub(r"\{\{?\s*" + key + r"\s*\}?\}", str(val), path)

    url = base_url.rstrip("/") + "/" + path.lstrip("/")

    # Dry run — не делаем реальный запрос
    if req.dry_run:
        return {
            "status": "dry_run",
            "partner_id": partner_id,
            "action": action,
            "url": url,
            "method": method,
            "params": req.params,
            "message": "Dry run — реальный запрос не отправлен"
        }

    # Получаем OAuth токен
    start = time.time()
    try:
        token = await get_oauth_token(partner_id, auth_cfg)
    except HTTPException as e:
        return {"status": "error", "error": "oauth_failed", "detail": e.detail}

    # Делаем запрос к API партнёра
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-Platform-Request-Id": f"plat_{int(time.time())}_{partner_id}"
    }

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            if method.upper() in ("GET", "DELETE"):
                resp = await client.request(method.upper(), url, params=req.params, headers=headers)
            else:
                resp = await client.request(method.upper(), url, json=req.params, headers=headers)
    except httpx.TimeoutException:
        return {"status": "error", "error": "timeout", "detail": f"Partner API timeout after 15s"}
    except Exception as e:
        return {"status": "error", "error": "connection_error", "detail": str(e)}

    latency_ms = int((time.time() - start) * 1000)

    # Логируем в БД
    conn = get_db()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO request_logs (company_id, action, method, path, status_code, latency_ms, env, created_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())""",
                    (partner_id, action, method, path, resp.status_code, latency_ms, "live")
                )
            conn.commit()
        except Exception as e:
            print(f"[ADAPTER] Log error: {e}")
        finally:
            conn.close()

    print(f"[ADAPTER] {partner_id}.{action} → {resp.status_code} ({latency_ms}ms)")

    try:
        resp_data = resp.json()
    except Exception:
        resp_data = {"raw": resp.text[:500]}

    return {
        "status": "ok" if resp.status_code < 400 else "error",
        "partner_id": partner_id,
        "action": action,
        "http_status": resp.status_code,
        "latency_ms": latency_ms,
        "url": url,
        "response": resp_data
    }


@app.get("/adapter/token/{partner_id}")
async def get_partner_token(partner_id: str):
    """Получить/обновить OAuth токен для партнёра."""
    config = REGISTRY.get(partner_id) or ADAPTERS.get(partner_id)
    if not config:
        raise HTTPException(status_code=404, detail=f"Partner {partner_id} not found")

    auth_cfg = config.get("auth", {})
    start = time.time()
    token = await get_oauth_token(partner_id, auth_cfg)
    latency = int((time.time() - start) * 1000)

    cached = _token_cache.get(partner_id, {})
    ttl = max(0, int(cached.get("expires_at", 0) - time.time()))

    return {
        "partner_id": partner_id,
        "token_preview": token[:20] + "..." if token else "",
        "ttl_seconds": ttl,
        "ttl_formatted": f"{ttl//60}m {ttl%60:02d}s",
        "latency_ms": latency
    }


@app.get("/adapter/test/{partner_id}/{action}")
async def test_adapter(partner_id: str, action: str, dry_run: bool = True):
    """Тестовый вызов action партнёра (dry_run по умолчанию)."""
    req = AdapterCallRequest(
        partner_id=partner_id,
        action=action,
        params={"_test": True},
        dry_run=dry_run
    )
    return await adapter_call(req)


# ── Wallet transaction recording ──────────────────────────────────────────────

class WalletTransactionRequest(BaseModel):
    partner_id: str
    amount: float
    description: str
    chain_id: str = ""
    route: str = ""
    payout_date: str = ""
    status: str = "pending"   # pending | paid

@app.post("/wallet/transaction")
def record_transaction(req: WalletTransactionRequest):
    """Записать транзакцию в кошелёк партнёра (вызывается оркестратором при завершении цепочки)."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=503, detail="DB unavailable")
    try:
        meta = json.dumps({
            "chain": req.chain_id,
            "route": req.route,
            "payout_date": req.payout_date or _next_payout_date()
        })
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO wallet_transactions
                   (partner_id, type, amount, description, status, metadata, created_at)
                   VALUES (%s, 'earning', %s, %s, %s, %s, NOW())
                   RETURNING id""",
                (req.partner_id, req.amount, req.description, req.status, meta)
            )
            tx_id = cur.fetchone()[0]
        conn.commit()
        print(f"[WALLET] +{req.amount}₽ → {req.partner_id} (tx={tx_id})")
        return {"status": "ok", "transaction_id": tx_id, "amount": req.amount, "partner_id": req.partner_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.post("/wallet/payout")
def record_payout(partner_id: str, amount: float, method: str = "bank_transfer"):
    """Зафиксировать выплату партнёру."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=503, detail="DB unavailable")
    try:
        meta = json.dumps({"method": method, "date": datetime.utcnow().strftime("%Y-%m-%d")})
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO wallet_transactions
                   (partner_id, type, amount, description, status, metadata, created_at)
                   VALUES (%s, 'payout', %s, %s, 'paid', %s, NOW())
                   RETURNING id""",
                (partner_id, amount, f"Выплата {datetime.utcnow().strftime('%d.%m.%Y')}", meta)
            )
            tx_id = cur.fetchone()[0]
            # Mark pending earnings as paid
            cur.execute(
                "UPDATE wallet_transactions SET status='paid' WHERE partner_id=%s AND status='pending' AND type='earning'",
                (partner_id,)
            )
        conn.commit()
        print(f"[WALLET] Payout {amount}₽ → {partner_id}")
        return {"status": "ok", "payout_id": tx_id, "amount": amount}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

def _next_payout_date() -> str:
    from datetime import date
    today = date.today()
    if today.day < 15:
        return date(today.year, today.month, 15).isoformat()
    if today.month == 12:
        return date(today.year + 1, 1, 15).isoformat()
    return date(today.year, today.month + 1, 15).isoformat()


# ── Email notifications ───────────────────────────────────────────────────────

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "")
PLATFORM_EMAIL_FROM = os.getenv("PLATFORM_EMAIL_FROM", "noreply@15min.store")

async def send_email(to: str, subject: str, body_html: str) -> bool:
    """Отправить email через SendGrid API."""
    if not SENDGRID_API_KEY:
        print(f"[EMAIL] No SENDGRID_API_KEY — skipping email to {to}: {subject}")
        return False
    if httpx is None:
        print(f"[EMAIL] httpx not available")
        return False
    payload = {
        "personalizations": [{"to": [{"email": to}]}],
        "from": {"email": PLATFORM_EMAIL_FROM, "name": "15min.store Platform"},
        "subject": subject,
        "content": [{"type": "text/html", "value": body_html}]
    }
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://api.sendgrid.com/v3/mail/send",
                json=payload,
                headers={"Authorization": f"Bearer {SENDGRID_API_KEY}"},
                timeout=10.0
            )
        if resp.status_code in (200, 202):
            print(f"[EMAIL] Sent to {to}: {subject}")
            return True
        print(f"[EMAIL] Failed {resp.status_code}: {resp.text[:200]}")
        return False
    except Exception as e:
        print(f"[EMAIL] Error: {e}")
        return False

def email_welcome(partner_name: str, partner_id: str, email: str) -> str:
    return f"""
    <div style="font-family:sans-serif;max-width:600px;margin:0 auto">
      <h2>Добро пожаловать на платформу 15min.store!</h2>
      <p>Компания <strong>{partner_name}</strong> успешно подключена.</p>
      <p>Команда проверит конфигурацию в течение 1-2 рабочих дней и активирует ваш аккаунт.</p>
      <p><a href="https://web-production-93110.up.railway.app/partner/{partner_id}"
            style="background:#5b21b6;color:#fff;padding:10px 20px;border-radius:8px;text-decoration:none">
        Открыть личный кабинет →
      </a></p>
      <p style="color:#64748b;font-size:12px">Вопросы? Напишите на support@15min.store</p>
    </div>
    """

def email_verified(partner_name: str, partner_id: str, email: str) -> str:
    return f"""
    <div style="font-family:sans-serif;max-width:600px;margin:0 auto">
      <h2>Верификация пройдена!</h2>
      <p>Компания <strong>{partner_name}</strong> активирована на платформе.</p>
      <p>Теперь AI агент будет включать вас в логистические цепочки клиентов.</p>
      <p><a href="https://web-production-93110.up.railway.app/partner/{partner_id}"
            style="background:#16a34a;color:#fff;padding:10px 20px;border-radius:8px;text-decoration:none">
        Открыть дашборд →
      </a></p>
    </div>
    """

def email_payout(partner_name: str, amount: float, payout_date: str, email: str) -> str:
    return f"""
    <div style="font-family:sans-serif;max-width:600px;margin:0 auto">
      <h2>Выплата отправлена</h2>
      <p>Компания <strong>{partner_name}</strong>, {amount:,.0f} ₽ переведено на ваши реквизиты {payout_date}.</p>
      <p><a href="https://web-production-93110.up.railway.app/partner"
            style="background:#0c4a6e;color:#fff;padding:10px 20px;border-radius:8px;text-decoration:none">
        Кошелёк →
      </a></p>
    </div>
    """

@app.post("/email/send")
async def send_notification(
    to: str, type: str,
    partner_id: str = "", partner_name: str = "",
    amount: float = 0, payout_date: str = ""
):
    """Отправить email уведомление партнёру."""
    templates = {
        "welcome":  (f"Добро пожаловать на платформу — {partner_name}", lambda: email_welcome(partner_name, partner_id, to)),
        "verified": (f"Верификация пройдена — {partner_name}", lambda: email_verified(partner_name, partner_id, to)),
        "payout":   (f"Выплата {amount:,.0f} ₽ — {payout_date}", lambda: email_payout(partner_name, amount, payout_date, to)),
    }
    if type not in templates:
        raise HTTPException(status_code=400, detail=f"Unknown email type: {type}")
    subject, body_fn = templates[type]
    ok = await send_email(to, subject, body_fn())
    return {"status": "sent" if ok else "skipped", "to": to, "type": type}


@app.get("/wallet/{partner_id}")
def get_wallet(partner_id: str):
    """Wallet данные партнёра — баланс, транзакции, выплаты."""
    # Load transactions from DB
    txs = []
    payouts = []
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, type, amount, description, status, created_at, metadata
            FROM wallet_transactions
            WHERE partner_id = ?
            ORDER BY created_at DESC
            LIMIT 100
        """, (partner_id,))
        rows = cur.fetchall()
        for row in rows:
            meta = {}
            try:
                import json as _json
                meta = _json.loads(row[6] or '{}')
            except: pass
            entry = {
                "id": row[0],
                "type": row[1],
                "amount": float(row[2] or 0),
                "description": row[3] or "Транзакция",
                "status": row[4] or "pending",
                "date": row[5][:10] if row[5] else "",
                "chain": meta.get("chain", ""),
                "route": meta.get("route", ""),
                "payout_date": meta.get("payout_date", ""),
            }
            if row[1] == "payout":
                payouts.append(entry)
            else:
                txs.append(entry)
        conn.close()
    except Exception as e:
        print(f"[WALLET] DB error: {e}")

    pending = sum(t["amount"] for t in txs if t["status"] == "pending")
    month = sum(t["amount"] for t in txs if t["status"] in ("pending","paid"))
    total_paid = sum(p["amount"] for p in payouts)

    return {
        "partner_id": partner_id,
        "balance": pending,
        "pending": pending,
        "month": month,
        "total_paid": total_paid,
        "chains": len(set(t.get("chain","") for t in txs if t.get("chain"))),
        "transactions": txs,
        "payouts": payouts,
    }


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8001))
    print(f"\n{'='*55}")
    print(f"  Logistics Platform API Server v0.2")
    print(f"{'='*55}")
    print(f"  Registry: {len(REGISTRY)} companies — {', '.join(REGISTRY.keys())}")
    print(f"  Adapters: {len(ADAPTERS)} — {', '.join(ADAPTERS.keys())}")
    print(f"  Port    : {port}")
    print(f"  Docs    : http://localhost:{port}/docs")
    print(f"  Portal  : http://localhost:{port}/portal")
    print(f"{'='*55}\n")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
