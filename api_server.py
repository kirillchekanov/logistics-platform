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
sys.path.insert(0, str(Path(__file__).parent.parent / "http-adapter"))

# ── Загрузка реестра ──────────────────────────────────────────────────────────

REGISTRY_DIR = Path(__file__).parent / "registry"
ADAPTERS_DIR = Path(__file__).parent.parent / "http-adapter" / "adapters"

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

@app.get("/")
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
    co = REGISTRY.get(company_id)
    adapter = ADAPTERS.get(company_id, {})
    if not co and not adapter:
        raise HTTPException(status_code=404, detail=f"Company {company_id} not found")
    return {"company": co or adapter, "adapter": adapter}

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

    # Сохраняем адаптер
    if ADAPTERS_DIR.exists():
        adapter_path = ADAPTERS_DIR / f"{company_id}.json"
        adapter_path.write_text(json.dumps(config, ensure_ascii=False, indent=2))
        ADAPTERS[company_id] = config

    return {
        "status": "registered",
        "company_id": company_id,
        "company_name": config.get("name"),
        "actions": list(config.get("actions", {}).keys()),
        "message": f"Company {config.get('name')} registered. Available in portal immediately.",
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

