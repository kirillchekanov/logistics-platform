#!/usr/bin/env python3
"""
Chain API — клиентские эндпоинты + AI цепочки
==============================================
Добавить в api_server.py после секции "Email notifications" (строка ~1480).

Новые эндпоинты:
  POST /clients/register       — регистрация клиента
  GET  /clients/{id}           — данные клиента
  POST /clients/{id}/topup     — пополнение баланса
  GET  /clients/{id}/balance   — баланс
  POST /chain/plan             — промт → план цепочки (Claude API)
  POST /chain/execute          — исполнить утверждённый план
  GET  /chain/history/{id}     — история цепочек клиента

Зависимости уже есть в api_server.py: fastapi, httpx, psycopg2, pydantic.
Нужно добавить в Railway Variables: ANTHROPIC_API_KEY
"""

import json
import uuid
import time
import os
import httpx
from datetime import datetime
from fastapi import HTTPException
from pydantic import BaseModel
from typing import Optional

# ── Pydantic models ───────────────────────────────────────────────────────────

class ClientRegisterRequest(BaseModel):
    company_name: str
    inn: Optional[str] = None
    contact_email: str
    contact_phone: Optional[str] = None

class TopUpRequest(BaseModel):
    amount_rub: float
    description: Optional[str] = "Пополнение баланса"

class ChainPlanRequest(BaseModel):
    client_id: str
    prompt: str                          # "хочу разместить товары на Дарксторах в топ-5 городов"
    budget_rub: Optional[float] = None  # ограничение бюджета

class ChainExecuteRequest(BaseModel):
    client_id: str
    plan_id: str                         # id из ответа /chain/plan
    confirmed: bool = False              # клиент подтвердил план

# ── DB helpers (дублируют стиль get_db() из api_server.py) ───────────────────

def _init_chain_tables(conn):
    """Добавляет таблицы chains и chain_steps если не существуют."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS chains (
                id TEXT PRIMARY KEY,
                client_id TEXT REFERENCES clients(id),
                prompt TEXT NOT NULL,
                plan JSONB,
                status TEXT DEFAULT 'planned',
                total_cost_rub DECIMAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW(),
                executed_at TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS chain_steps (
                id SERIAL PRIMARY KEY,
                chain_id TEXT REFERENCES chains(id),
                step_index INTEGER,
                partner_id TEXT,
                action TEXT,
                params JSONB,
                result JSONB,
                status TEXT DEFAULT 'pending',
                executed_at TIMESTAMP
            );
            -- Добавляем client_id в wallet_transactions если его нет
            ALTER TABLE wallet_transactions
                ADD COLUMN IF NOT EXISTS client_id_ref TEXT REFERENCES clients(id);
        """)
    conn.commit()

# ── AI Chain Planner ──────────────────────────────────────────────────────────

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
PLATFORM_URL = os.getenv("PLATFORM_API_URL", "https://web-production-93110.up.railway.app")

async def build_chain_plan(prompt: str, available_partners: list[dict], budget_rub: float = None) -> dict:
    """
    Отправляет промт в Claude API.
    Claude видит список доступных партнёров и их actions,
    возвращает JSON-план цепочки.
    """
    if not ANTHROPIC_API_KEY:
        # Stub план для разработки без ключа
        return _stub_plan(prompt, available_partners)

    partners_summary = json.dumps([
        {
            "id": p["id"],
            "name": p["name"],
            "capabilities": p.get("capabilities", []),
            "actions": p.get("actions", []),
            "coverage_cities": p.get("coverage_cities", 0),
        }
        for p in available_partners
    ], ensure_ascii=False, indent=2)

    budget_clause = f"\nБюджет клиента: {budget_rub:,.0f} ₽." if budget_rub else ""

    system_prompt = """Ты — оркестратор логистических цепочек платформы 15min.store.
У тебя есть список партнёров (логистических компаний и складов) с их возможностями.
Твоя задача — по запросу клиента построить оптимальный план цепочки доставки.

Отвечай ТОЛЬКО валидным JSON без markdown-обёртки. Формат:
{
  "summary": "Краткое описание цепочки на русском",
  "total_cost_rub": 12500,
  "steps": [
    {
      "step": 1,
      "partner_id": "cdek",
      "action": "calculate_cost",
      "description": "Рассчитать стоимость доставки Москва → Новосибирск",
      "params": {"from_city": "Москва", "to_city": "Новосибирск", "weight_kg": 10},
      "estimated_cost_rub": 2500,
      "depends_on": []
    }
  ],
  "warnings": []
}"""

    user_message = f"""Запрос клиента: {prompt}{budget_clause}

Доступные партнёры:
{partners_summary}

Построй оптимальный план цепочки."""

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-opus-4-5",
                    "max_tokens": 2000,
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": user_message}],
                }
            )
        resp.raise_for_status()
        content = resp.json()["content"][0]["text"]

        # Парсим JSON из ответа
        plan = json.loads(content)
        return plan

    except json.JSONDecodeError as e:
        print(f"[CHAIN] Claude returned invalid JSON: {e}")
        return _stub_plan(prompt, available_partners)
    except Exception as e:
        print(f"[CHAIN] Claude API error: {e}")
        return _stub_plan(prompt, available_partners)


def _stub_plan(prompt: str, partners: list[dict]) -> dict:
    """Stub план для тестирования без Claude API."""
    partner_ids = [p["id"] for p in partners]
    has_darkstore = "darkstore" in partner_ids
    has_cdek = "cdek" in partner_ids

    steps = []
    if has_cdek:
        steps.append({
            "step": 1,
            "partner_id": "cdek",
            "action": "calculate_cost",
            "description": "Рассчитать стоимость доставки до городов",
            "params": {"from_city": "Москва", "weight_kg": 5},
            "estimated_cost_rub": 2500,
            "depends_on": []
        })
    if has_darkstore:
        steps.append({
            "step": 2,
            "partner_id": "darkstore",
            "action": "check_coverage",
            "description": "Проверить доступность Даркстора в городах",
            "params": {"cities": ["Москва", "Санкт-Петербург", "Екатеринбург", "Новосибирск", "Казань"]},
            "estimated_cost_rub": 0,
            "depends_on": []
        })

    return {
        "summary": f"Stub-план для: {prompt[:80]}",
        "total_cost_rub": 12500,
        "steps": steps,
        "warnings": ["STUB: Claude API key не задан, это тестовый план"]
    }

# ── Эндпоинты — добавить в app из api_server.py ──────────────────────────────
# Пример подключения в api_server.py:
#   from chain_api import register_chain_routes
#   register_chain_routes(app, get_db, ADAPTERS)

def register_chain_routes(app, get_db_fn, adapters_ref):
    """Регистрирует все chain/client маршруты в существующем FastAPI app."""

    # ── Clients ───────────────────────────────────────────────────────────────

    @app.post("/clients/register")
    def register_client(req: ClientRegisterRequest):
        """Регистрация нового клиента."""
        conn = get_db_fn()
        if not conn:
            raise HTTPException(status_code=503, detail="DB unavailable")
        client_id = f"cl_{uuid.uuid4().hex[:12]}"
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO clients (id, company_name, inn, contact_email, contact_phone, balance_rub, status, created_at)
                    VALUES (%s, %s, %s, %s, %s, 0, 'active', NOW())
                    RETURNING id, company_name, balance_rub, created_at
                """, (client_id, req.company_name, req.inn, req.contact_email, req.contact_phone))
                row = cur.fetchone()
            conn.commit()
            print(f"[CLIENT] Registered: {client_id} — {req.company_name}")
            return {
                "client_id": row[0],
                "company_name": row[1],
                "balance_rub": float(row[2]),
                "created_at": row[3].isoformat(),
                "status": "active"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            conn.close()

    @app.get("/clients/{client_id}")
    def get_client(client_id: str):
        """Данные клиента."""
        conn = get_db_fn()
        if not conn:
            raise HTTPException(status_code=503, detail="DB unavailable")
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, company_name, inn, contact_email, contact_phone,
                           balance_rub, reserved_rub, status, created_at
                    FROM clients WHERE id = %s
                """, (client_id,))
                row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Client {client_id} not found")
            return {
                "client_id": row[0],
                "company_name": row[1],
                "inn": row[2],
                "contact_email": row[3],
                "contact_phone": row[4],
                "balance_rub": float(row[5] or 0),
                "reserved_rub": float(row[6] or 0),
                "available_rub": float((row[5] or 0) - (row[6] or 0)),
                "status": row[7],
                "created_at": row[8].isoformat() if row[8] else None,
            }
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            conn.close()

    @app.post("/clients/{client_id}/topup")
    def topup_balance(client_id: str, req: TopUpRequest):
        """Пополнение баланса клиента."""
        if req.amount_rub <= 0:
            raise HTTPException(status_code=400, detail="amount_rub должен быть > 0")
        conn = get_db_fn()
        if not conn:
            raise HTTPException(status_code=503, detail="DB unavailable")
        try:
            with conn.cursor() as cur:
                # Проверяем что клиент существует
                cur.execute("SELECT balance_rub FROM clients WHERE id = %s", (client_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail=f"Client {client_id} not found")
                # Пополняем баланс
                cur.execute(
                    "UPDATE clients SET balance_rub = balance_rub + %s WHERE id = %s RETURNING balance_rub",
                    (req.amount_rub, client_id)
                )
                new_balance = cur.fetchone()[0]
                # Пишем транзакцию
                cur.execute("""
                    INSERT INTO wallet_transactions (client_id, type, amount_rub, description, created_at)
                    VALUES (%s, 'topup', %s, %s, NOW())
                """, (client_id, req.amount_rub, req.description))
            conn.commit()
            print(f"[CLIENT] Topup +{req.amount_rub}₽ → {client_id}, balance={new_balance}")
            return {
                "client_id": client_id,
                "added_rub": req.amount_rub,
                "balance_rub": float(new_balance),
            }
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            conn.close()

    @app.get("/clients/{client_id}/balance")
    def get_balance(client_id: str):
        """Баланс клиента."""
        conn = get_db_fn()
        if not conn:
            raise HTTPException(status_code=503, detail="DB unavailable")
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT balance_rub, reserved_rub FROM clients WHERE id = %s",
                    (client_id,)
                )
                row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Client not found")
            return {
                "client_id": client_id,
                "balance_rub": float(row[0] or 0),
                "reserved_rub": float(row[1] or 0),
                "available_rub": float((row[0] or 0) - (row[1] or 0)),
            }
        finally:
            conn.close()

    # ── Chain ─────────────────────────────────────────────────────────────────

    @app.post("/chain/plan")
    async def plan_chain(req: ChainPlanRequest):
        """
        Промт → план цепочки через Claude.
        Возвращает план для подтверждения клиентом.
        Не исполняет ничего, не списывает баланс.
        """
        conn = get_db_fn()
        if not conn:
            raise HTTPException(status_code=503, detail="DB unavailable")

        # Проверяем клиента
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT company_name, balance_rub FROM clients WHERE id = %s", (req.client_id,))
                client = cur.fetchone()
            if not client:
                raise HTTPException(status_code=404, detail=f"Client {req.client_id} not found")
        finally:
            conn.close()

        company_name, balance = client[0], float(client[1] or 0)

        # Получаем список активных партнёров
        partners = []
        for pid, adapter in adapters_ref.items():
            partners.append({
                "id": pid,
                "name": adapter.get("name", pid),
                "capabilities": adapter.get("capabilities", []),
                "actions": list((adapter.get("actions") or {}).keys()),
                "coverage_cities": 0,
            })

        # Строим план через Claude
        t_start = time.time()
        plan = await build_chain_plan(req.prompt, partners, req.budget_rub)
        latency_ms = int((time.time() - t_start) * 1000)

        # Сохраняем план в БД
        plan_id = f"chain_{uuid.uuid4().hex[:12]}"
        conn = get_db_fn()
        try:
            with conn.cursor() as cur:
                # Создаём таблицы если нет
                try:
                    _init_chain_tables(conn)
                except Exception:
                    pass  # таблицы уже есть
                cur.execute("""
                    INSERT INTO chains (id, client_id, prompt, plan, status, total_cost_rub, created_at)
                    VALUES (%s, %s, %s, %s, 'planned', %s, NOW())
                """, (plan_id, req.client_id, req.prompt, json.dumps(plan), plan.get("total_cost_rub", 0)))
            conn.commit()
        except Exception as e:
            print(f"[CHAIN] DB save error: {e}")
        finally:
            conn.close()

        print(f"[CHAIN] Plan {plan_id} built in {latency_ms}ms for {req.client_id}: {req.prompt[:50]}")

        return {
            "plan_id": plan_id,
            "client_id": req.client_id,
            "company_name": company_name,
            "prompt": req.prompt,
            "plan": plan,
            "client_balance_rub": balance,
            "sufficient_funds": balance >= plan.get("total_cost_rub", 0),
            "latency_ms": latency_ms,
            "next_step": "Подтвердите план через POST /chain/execute с confirmed=true",
        }

    @app.post("/chain/execute")
    async def execute_chain(req: ChainExecuteRequest):
        """
        Исполняет утверждённый план цепочки.
        Клиент должен подтвердить: confirmed=true.
        Списывает стоимость с баланса и вызывает actions партнёров.
        """
        if not req.confirmed:
            raise HTTPException(status_code=400, detail="Передайте confirmed=true для исполнения цепочки")

        conn = get_db_fn()
        if not conn:
            raise HTTPException(status_code=503, detail="DB unavailable")

        # Загружаем план
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT client_id, prompt, plan, status, total_cost_rub FROM chains WHERE id = %s",
                    (req.plan_id,)
                )
                chain = cur.fetchone()
            if not chain:
                raise HTTPException(status_code=404, detail=f"Plan {req.plan_id} not found")
            if chain[0] != req.client_id:
                raise HTTPException(status_code=403, detail="Этот план принадлежит другому клиенту")
            if chain[3] == "executed":
                raise HTTPException(status_code=409, detail="Цепочка уже исполнена")
        finally:
            conn.close()

        client_id, prompt, plan_raw, status, total_cost = chain
        plan = plan_raw if isinstance(plan_raw, dict) else json.loads(plan_raw)
        total_cost = float(total_cost or 0)

        # Проверяем баланс
        conn = get_db_fn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT balance_rub FROM clients WHERE id = %s", (client_id,))
                balance_row = cur.fetchone()
            balance = float(balance_row[0] or 0) if balance_row else 0
        finally:
            conn.close()

        if balance < total_cost:
            raise HTTPException(
                status_code=402,
                detail=f"Недостаточно средств: баланс {balance:,.0f}₽, нужно {total_cost:,.0f}₽"
            )

        # Исполняем шаги
        results = []
        for step in plan.get("steps", []):
            partner_id = step.get("partner_id")
            action = step.get("action")
            params = step.get("params", {})

            adapter = adapters_ref.get(partner_id)
            if not adapter:
                results.append({**step, "status": "skipped", "error": f"Partner {partner_id} not found"})
                continue

            # Вызываем через существующий execute_action (stub или реальный)
            try:
                async with httpx.AsyncClient(timeout=20.0) as client:
                    resp = await client.post(
                        f"{PLATFORM_URL}/execute",
                        json={"company_id": partner_id, "action": action, "params": params, "dry_run": False}
                    )
                result = resp.json()
                results.append({**step, "status": "ok", "result": result})
                print(f"[CHAIN] Step {step['step']}: {partner_id}.{action} → ok")
            except Exception as e:
                results.append({**step, "status": "error", "error": str(e)})
                print(f"[CHAIN] Step {step['step']}: {partner_id}.{action} → error: {e}")

        # Списываем с баланса и обновляем статус цепочки
        conn = get_db_fn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE clients SET balance_rub = balance_rub - %s WHERE id = %s",
                    (total_cost, client_id)
                )
                cur.execute("""
                    UPDATE chains SET status = 'executed', executed_at = NOW()
                    WHERE id = %s
                """, (req.plan_id,))
                cur.execute("""
                    INSERT INTO wallet_transactions (client_id, type, amount_rub, description, created_at)
                    VALUES (%s, 'chain_payment', %s, %s, NOW())
                """, (client_id, -total_cost, f"Цепочка {req.plan_id}: {prompt[:60]}"))
            conn.commit()
        except Exception as e:
            print(f"[CHAIN] DB update error: {e}")
        finally:
            conn.close()

        successful = sum(1 for r in results if r.get("status") == "ok")
        print(f"[CHAIN] Executed {req.plan_id}: {successful}/{len(results)} steps ok, -{total_cost}₽")

        return {
            "plan_id": req.plan_id,
            "status": "executed",
            "steps_total": len(results),
            "steps_ok": successful,
            "total_cost_rub": total_cost,
            "results": results,
            "executed_at": datetime.utcnow().isoformat(),
        }

    @app.get("/chain/history/{client_id}")
    def chain_history(client_id: str, limit: int = 20):
        """История цепочек клиента."""
        conn = get_db_fn()
        if not conn:
            return {"chains": [], "total": 0}
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, prompt, status, total_cost_rub, created_at, executed_at
                    FROM chains
                    WHERE client_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (client_id, limit))
                rows = cur.fetchall()
            return {
                "client_id": client_id,
                "chains": [
                    {
                        "chain_id": r[0],
                        "prompt": r[1],
                        "status": r[2],
                        "total_cost_rub": float(r[3] or 0),
                        "created_at": r[4].isoformat() if r[4] else None,
                        "executed_at": r[5].isoformat() if r[5] else None,
                    }
                    for r in rows
                ],
                "total": len(rows),
            }
        except Exception as e:
            print(f"[CHAIN] history error: {e}")
            return {"chains": [], "total": 0}
        finally:
            conn.close()
