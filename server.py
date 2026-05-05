#!/usr/bin/env python3
"""
Logistics Orchestration Platform — MCP Server v0.1
====================================================

Агент-оркестратор логистических цепочек.
Строит оптимальные маршруты из доступных компаний-партнёров
без ручной конфигурации цепочек.

Компании в реестре: Даркстор у дома, СДЭК, ПЭК.
Tools: 8 (разведка, построение, исполнение, реестр).

Версия: stub v0.1 — бизнес-логика выверена, боевые API компаний подключает разработка.
"""

from __future__ import annotations

import json
import os
import hashlib
import time
from datetime import datetime, date, timedelta
from typing import Optional, Literal
from pathlib import Path

from fastmcp import FastMCP

# ── Загрузка реестра ──────────────────────────────────────────────────────────

REGISTRY_DIR = Path(__file__).parent / "registry"

def load_registry() -> dict:
    registry = {}
    if REGISTRY_DIR.exists():
        for f in REGISTRY_DIR.glob("*.json"):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                registry[data["id"]] = data
            except Exception as e:
                print(f"[WARN] Не удалось загрузить {f.name}: {e}")
    return registry

REGISTRY: dict = load_registry()

# ── FastMCP сервер ────────────────────────────────────────────────────────────

mcp = FastMCP(
    name="logistics-orchestration-platform",
    instructions=(
        "Ты — агент-оркестратор логистической платформы 15min.store.\n"
        "Ты помогаешь клиентам логистических компаний организовать доставку.\n\n"

        "── ГЛАВНОЕ ПРАВИЛО ──\n"
        "Никогда не угадывай исполнителей из головы.\n"
        "ВСЕГДА начинай с get_capable_companies — это единственный источник правды о том\n"
        "кто физически может выполнить задачу.\n\n"

        "── ПОРЯДОК РАБОТЫ ──\n"
        "1. get_capable_companies — кто подходит по маршруту, весу, срокам\n"
        "2. get_company_slots — стыкуются ли временны́е окна компаний\n"
        "3. build_chain — строишь 2-3 варианта цепочек\n"
        "4. estimate_chain_cost — рассчитываешь стоимость\n"
        "5. Предлагаешь клиенту лучший вариант с объяснением\n"
        "6. confirm_and_launch(dry_run=True) → превью → подтверждение → dry_run=False\n"
        "7. notify_handover — уведомляешь склады на каждом шаге\n"
        "8. track_chain — отслеживаешь выполнение\n\n"

        "── ПРАВИЛА ОБЩЕНИЯ С КЛИЕНТОМ ──\n"
        "Называй компании по display_name из реестра, не по их реальному имени,\n"
        "если клиент сам не назвал компанию.\n"
        "Например: 'партнёр по магистральной доставке' вместо 'СДЭК'.\n"
        "Всегда показывай цену и сроки перед запуском.\n"
        "Никогда не запускай цепочку без явного подтверждения клиента.\n\n"

        "── ЧТО ДЕЛАТЬ ЕСЛИ ЦЕПОЧКА НЕ СКЛАДЫВАЕТСЯ ──\n"
        "Объясни клиенту что именно не сходится (окна, вес, покрытие).\n"
        "Предложи скорректировать задачу: перенести срок, разбить на части, изменить маршрут.\n"
        "Никогда не предлагай компании которых нет в реестре.\n"
    )
)

# ── Вспомогательные функции ───────────────────────────────────────────────────

def _idempotency_key(data: str) -> str:
    return hashlib.md5(f"{data}{time.time()}".encode()).hexdigest()[:12]

def _company_covers(company: dict, city: str) -> bool:
    cities = [c.lower() for c in company["coverage"].get("cities", [])]
    return city.lower() in cities

def _company_can_handle_weight(company: dict, weight_kg: float) -> bool:
    max_w = company["constraints"].get("max_weight_kg", 9999)
    return weight_kg <= max_w

def _company_can_handle_dimensions(company: dict, dims: list | None) -> bool:
    if not dims:
        return True
    max_d = company["constraints"].get("max_dimensions_cm", [9999, 9999, 9999])
    return all(d <= m for d, m in zip(sorted(dims, reverse=True), sorted(max_d, reverse=True)))

def _company_has_capability(company: dict, capability: str) -> bool:
    return capability in company.get("capabilities", [])

def _transit_days(company: dict, from_city: str, to_city: str) -> int | None:
    td = company.get("time_windows", {}).get("transit_days", {})
    key = f"{from_city}-{to_city}"
    return td.get(key)

def _estimate_cost(company: dict, weight_kg: float, distance_km: float = 500) -> float:
    pricing = company.get("pricing", {})
    base_kg = pricing.get("base_per_kg_rub", 30)
    base_km = pricing.get("base_per_km_rub", 0.5)
    min_order = pricing.get("min_order_rub", 300)
    cost = weight_kg * base_kg + distance_km * base_km
    return max(cost, min_order)

# ── ГРУППА 1: РАЗВЕДКА ────────────────────────────────────────────────────────

@mcp.tool()
def get_capable_companies(
    from_city: str,
    to_city: str,
    weight_kg: float,
    deadline_hours: Optional[int] = None,
    required_capability: Optional[str] = None,
    dimensions_cm: Optional[list] = None,
    cargo_category: Optional[str] = None,
) -> dict:
    """Найти компании способные выполнить задачу.

    ВСЕГДА вызывай первым при любом логистическом запросе.
    Фильтрует реестр по маршруту, весу, габаритам, срокам и категории груза.

    Args:
        from_city: Город отправления.
        to_city: Город назначения.
        weight_kg: Вес груза в кг (общий).
        deadline_hours: Срок доставки в часах от сейчас (опц.).
        required_capability: Требуемая возможность: pickup_from_client, last_mile_delivery, storage, intercity_transit и др.
        dimensions_cm: Габариты [длина, ширина, высота] в см (опц.).
        cargo_category: Категория груза для проверки запретов (опц.).
    """
    suitable = []
    excluded = []

    for company_id, company in REGISTRY.items():
        reasons_out = []

        # Покрытие маршрута
        covers_from = _company_covers(company, from_city)
        covers_to = _company_covers(company, to_city)
        if not covers_from:
            reasons_out.append(f"нет присутствия в {from_city}")
        if not covers_to:
            reasons_out.append(f"нет присутствия в {to_city}")

        # Вес
        if not _company_can_handle_weight(company, weight_kg):
            max_w = company["constraints"].get("max_weight_kg", "?")
            reasons_out.append(f"превышен лимит веса {max_w} кг")

        # Габариты
        if dimensions_cm and not _company_can_handle_dimensions(company, dimensions_cm):
            reasons_out.append("превышены допустимые габариты")

        # Требуемая возможность
        if required_capability and not _company_has_capability(company, required_capability):
            reasons_out.append(f"нет возможности {required_capability}")

        # Категория груза
        if cargo_category:
            forbidden = [f.lower() for f in company["constraints"].get("forbidden_categories", [])]
            if any(cargo_category.lower() in f for f in forbidden):
                reasons_out.append(f"категория '{cargo_category}' запрещена")

        if reasons_out:
            excluded.append({
                "company_id": company_id,
                "name": company["display_name"],
                "excluded_because": reasons_out,
            })
            continue

        # Транзитное время (если дедлайн задан)
        transit = _transit_days(company, from_city, to_city)
        transit_hours = (transit * 24) if transit is not None else None
        meets_deadline = None
        if deadline_hours and transit_hours:
            meets_deadline = transit_hours <= deadline_hours

        suitable.append({
            "company_id": company_id,
            "name": company["name"],
            "display_name": company["display_name"],
            "capabilities": company["capabilities"],
            "transit_days": transit,
            "meets_deadline": meets_deadline,
            "max_weight_kg": company["constraints"].get("max_weight_kg"),
            "adapter_endpoint": company["adapter"]["mcp_endpoint"],
        })

    # Сортируем: сначала те кто укладывается в дедлайн
    suitable.sort(key=lambda x: (0 if x["meets_deadline"] else 1, x["transit_days"] or 999))

    return {
        "status": "ok" if suitable else "no_suitable_companies",
        "from_city": from_city,
        "to_city": to_city,
        "weight_kg": weight_kg,
        "suitable_count": len(suitable),
        "suitable": suitable,
        "excluded_count": len(excluded),
        "excluded": excluded,
        "next_action": "get_company_slots" if suitable else "explain_to_client_why_no_options",
    }


@mcp.tool()
def get_company_slots(
    company_id: str,
    city: str,
    slot_date: Optional[str] = None,
    slot_type: Literal["pickup", "receiving", "delivery"] = "pickup",
) -> dict:
    """Получить доступные временны́е слоты компании.

    Используй для проверки стыковки окон в цепочке.
    Например: успеет ли компания A передать до того как компания B закрывается.

    Args:
        company_id: ID компании из реестра.
        city: Город для которого нужны слоты.
        slot_date: Дата в формате YYYY-MM-DD (по умолчанию — завтра).
        slot_type: Тип слота: pickup (забор), receiving (приём), delivery (доставка).
    """
    company = REGISTRY.get(company_id)
    if not company:
        return {"status": "error", "error": f"Компания {company_id} не найдена в реестре"}

    if not _company_covers(company, city):
        return {
            "status": "not_covered",
            "company_id": company_id,
            "city": city,
            "message": f"{company['name']} не работает в {city}",
        }

    if not slot_date:
        slot_date = (date.today() + timedelta(days=1)).isoformat()

    tw = company.get("time_windows", {})

    # Возвращаем окна из реестра
    if slot_type == "pickup":
        window = tw.get("pickup_window", "09:00-18:00")
        cutoff = tw.get("order_cutoff", "18:00")
        lead_hours = tw.get("pickup_lead_hours", 2)
        return {
            "status": "ok",
            "company_id": company_id,
            "city": city,
            "date": slot_date,
            "slot_type": "pickup",
            "window": window,
            "order_cutoff": cutoff,
            "lead_hours": lead_hours,
            "note": f"Заявку нужно подать за {lead_hours} ч до забора",
        }
    elif slot_type == "receiving":
        window = tw.get("receiving_window", tw.get("terminal_window", "08:00-20:00"))
        return {
            "status": "ok",
            "company_id": company_id,
            "city": city,
            "date": slot_date,
            "slot_type": "receiving",
            "window": window,
            "note": "Склад принимает в указанное окно",
        }
    elif slot_type == "delivery":
        window = tw.get("delivery_window_courier", "09:00-22:00")
        assembly = tw.get("assembly_minutes")
        delivery_avg = tw.get("delivery_minutes_avg")
        note = ""
        if assembly and delivery_avg:
            note = f"Сборка {assembly} мин + доставка {delivery_avg} мин в среднем (не SLA)"
        return {
            "status": "ok",
            "company_id": company_id,
            "city": city,
            "date": slot_date,
            "slot_type": "delivery",
            "window": window,
            "note": note,
        }


# ── ГРУППА 2: ПОСТРОЕНИЕ ЦЕПОЧКИ ─────────────────────────────────────────────

@mcp.tool()
def build_chain(
    from_city: str,
    to_city: str,
    weight_kg: float,
    available_company_ids: list[str],
    optimize_for: Literal["time", "cost", "reliability"] = "time",
    deadline_hours: Optional[int] = None,
    dimensions_cm: Optional[list] = None,
) -> dict:
    """Построить варианты логистической цепочки из доступных компаний.

    Вызывай ПОСЛЕ get_capable_companies и get_company_slots.
    Строит 2-3 варианта цепочек, сортирует по критерию оптимизации.

    Args:
        from_city: Город отправления.
        to_city: Город назначения.
        weight_kg: Вес груза в кг.
        available_company_ids: Список ID компаний из get_capable_companies.
        optimize_for: Критерий: time — скорость, cost — дешевизна, reliability — надёжность.
        deadline_hours: Дедлайн в часах от сейчас.
        dimensions_cm: Габариты [д, ш, в] в см.
    """
    companies = {cid: REGISTRY[cid] for cid in available_company_ids if cid in REGISTRY}

    if not companies:
        return {"status": "error", "error": "Ни одна из компаний не найдена в реестре"}

    chains = []
    DIST_APPROX = {"Самара-Москва": 850, "Москва-СПб": 700, "Москва-Казань": 800,
                   "Екатеринбург-Москва": 1400, "Новосибирск-Москва": 2800}
    dist = DIST_APPROX.get(f"{from_city}-{to_city}",
           DIST_APPROX.get(f"{to_city}-{from_city}", 600))

    # Вариант 1: прямая доставка (если одна компания покрывает весь маршрут)
    for cid, company in companies.items():
        if (_company_covers(company, from_city) and
            _company_covers(company, to_city) and
            _company_has_capability(company, "intercity_transit")):
            transit = _transit_days(company, from_city, to_city)
            cost = _estimate_cost(company, weight_kg, dist)
            chains.append({
                "chain_id": f"chain_{cid}_direct_{_idempotency_key(cid)}",
                "type": "direct",
                "label": f"Прямая доставка через {company['display_name']}",
                "steps": [
                    {"step": 1, "company_id": cid, "company_name": company["display_name"],
                     "role": "pickup_and_deliver", "from": from_city, "to": to_city},
                ],
                "total_transit_days": transit,
                "estimated_cost_rub": round(cost),
                "reliability_score": 0.85,
                "meets_deadline": (transit * 24 <= deadline_hours) if (transit and deadline_hours) else None,
            })

    # Вариант 2: магистраль + last-mile (если есть перевозчик + даркстор)
    carriers = [cid for cid, c in companies.items()
                if _company_has_capability(c, "intercity_transit")
                and _company_covers(c, from_city)]
    darkstore_ids = [cid for cid, c in companies.items()
                     if _company_has_capability(c, "last_mile_delivery")
                     and _company_covers(c, to_city)]

    for carrier_id in carriers:
        for ds_id in darkstore_ids:
            if carrier_id == ds_id:
                continue
            carrier = companies[carrier_id]
            ds = companies[ds_id]
            transit = _transit_days(carrier, from_city, to_city)
            transit_total = (transit or 1) * 24 + 2  # +2ч на last-mile
            carrier_cost = _estimate_cost(carrier, weight_kg, dist)
            ds_cost = _estimate_cost(ds, weight_kg, 0) * 0.5  # last-mile дешевле
            total_cost = carrier_cost + ds_cost
            chains.append({
                "chain_id": f"chain_{carrier_id}_{ds_id}_{_idempotency_key(carrier_id+ds_id)}",
                "type": "relay",
                "label": f"{carrier['display_name']} → {ds['display_name']} (last-mile)",
                "steps": [
                    {"step": 1, "company_id": carrier_id, "company_name": carrier["display_name"],
                     "role": "pickup_and_transit", "from": from_city, "to": f"СЦ {to_city}",
                     "handover_docs": carrier["handover"]["as_sender"]["required_docs"]},
                    {"step": 2, "company_id": ds_id, "company_name": ds["display_name"],
                     "role": "last_mile", "from": f"СЦ {to_city}", "to": to_city,
                     "note": "Last-mile до покупателя"},
                ],
                "total_transit_hours": transit_total,
                "total_transit_days": (transit or 1) + 0,
                "estimated_cost_rub": round(total_cost),
                "reliability_score": 0.92,
                "meets_deadline": (transit_total <= deadline_hours) if deadline_hours else None,
                "handover_points": 1,
            })

    if not chains:
        return {
            "status": "no_chains_possible",
            "message": "Не удалось построить цепочку из доступных компаний",
            "suggestion": "Проверьте покрытие городов или уточните параметры задачи",
        }

    # Сортировка
    if optimize_for == "time":
        chains.sort(key=lambda c: (c.get("total_transit_hours") or (c.get("total_transit_days", 99) * 24)))
    elif optimize_for == "cost":
        chains.sort(key=lambda c: c.get("estimated_cost_rub", 9999))
    elif optimize_for == "reliability":
        chains.sort(key=lambda c: -c.get("reliability_score", 0))

    # Топ 3 варианта
    top = chains[:3]
    top[0]["recommended"] = True

    return {
        "status": "ok",
        "from_city": from_city,
        "to_city": to_city,
        "weight_kg": weight_kg,
        "optimized_for": optimize_for,
        "chains_count": len(top),
        "chains": top,
        "recommended_chain_id": top[0]["chain_id"],
        "next_action": "estimate_chain_cost для уточнения стоимости, затем предложить клиенту",
    }


@mcp.tool()
def estimate_chain_cost(
    chain_id: str,
    weight_kg: float,
    from_city: str,
    to_city: str,
    dimensions_cm: Optional[list] = None,
    declared_value_rub: Optional[float] = None,
) -> dict:
    """Рассчитать стоимость логистической цепочки.

    В боевой версии вызывает pricing API каждой компании в реальном времени.
    В stub — расчёт по тарифной сетке реестра.

    Args:
        chain_id: ID цепочки из build_chain.
        weight_kg: Вес груза в кг.
        from_city: Город отправления.
        to_city: Город назначения.
        dimensions_cm: Габариты для расчёта объёмного веса (опц.).
        declared_value_rub: Объявленная стоимость для страховки (опц.).
    """
    DIST_APPROX = {"Самара-Москва": 850, "Москва-СПб": 700, "Москва-Казань": 800,
                   "Екатеринбург-Москва": 1400, "Новосибирск-Москва": 2800}
    dist = DIST_APPROX.get(f"{from_city}-{to_city}",
           DIST_APPROX.get(f"{to_city}-{from_city}", 600))

    # Объёмный вес
    volumetric_weight = None
    if dimensions_cm and len(dimensions_cm) == 3:
        vol = (dimensions_cm[0] * dimensions_cm[1] * dimensions_cm[2]) / 5000
        volumetric_weight = round(vol, 1)
        billing_weight = max(weight_kg, volumetric_weight)
    else:
        billing_weight = weight_kg

    # Для примера считаем через первые две компании из цепочки
    # В боевой версии здесь вызов реального API каждой компании
    breakdown = []
    total = 0

    for cid, company in REGISTRY.items():
        cost = _estimate_cost(company, billing_weight, dist)
        insurance = 0
        if declared_value_rub:
            ins_pct = company["pricing"].get("insurance_pct", 0)
            insurance = round(declared_value_rub * ins_pct / 100)
        breakdown.append({
            "company_id": cid,
            "company_name": company["display_name"],
            "base_cost_rub": round(cost),
            "insurance_rub": insurance,
            "subtotal_rub": round(cost + insurance),
            "note": "stub — в боевой версии вызов реального API тарифов",
        })
        total += cost + insurance
        if len(breakdown) >= 2:
            break

    return {
        "status": "ok",
        "chain_id": chain_id,
        "billing_weight_kg": billing_weight,
        "volumetric_weight_kg": volumetric_weight,
        "breakdown": breakdown,
        "total_rub": round(total),
        "platform_fee_per_step_rub": PLATFORM_FEE_PER_STEP_RUB,
        "platform_fee_total_rub": round(PLATFORM_FEE_PER_STEP_RUB * len(breakdown)),
        "grand_total_rub": round(total + PLATFORM_FEE_PER_STEP_RUB * len(breakdown)),
        "currency": "RUB",
        "stub_warning": "Цены приблизительные. В боевой версии — реальный API тарифов каждой компании.",
        "next_action": "check_balance затем hold_funds затем confirm_and_launch",
    }



# ── WALLET ────────────────────────────────────────────────────────────────────

# Фиксированная комиссия платформы за шаг (в боевой версии — из конфига)
PLATFORM_FEE_PER_STEP_RUB: float = 150.0

# Stub-балансы клиентов (в боевой версии — Wallet Service)
_WALLET_STUB: dict = {
    "client_001": {"balance_rub": 15000.0, "reserved_rub": 0.0, "company_name": "ООО Ромашка"},
    "client_002": {"balance_rub": 800.0,   "reserved_rub": 0.0, "company_name": "ИП Петров"},
}
_HOLDS_STUB: dict = {}


@mcp.tool()
def check_balance(
    client_id: str,
    required_amount_rub: float,
) -> dict:
    """Проверить баланс клиента перед запуском цепочки.

    ОБЯЗАТЕЛЬНЫЙ шаг — вызывай сразу после estimate_chain_cost.
    Если баланса не хватает — НЕ показывай клиенту цепочку, сразу сообщи о дефиците.

    Args:
        client_id: ID клиента (юрлица) в платформе.
        required_amount_rub: Требуемая сумма (из estimate_chain_cost, включая комиссии).
    """
    wallet = _WALLET_STUB.get(client_id)
    if not wallet:
        return {
            "status": "client_not_found",
            "error": f"Клиент {client_id} не найден в системе",
        }

    available = wallet["balance_rub"] - wallet["reserved_rub"]
    deficit = required_amount_rub - available

    if deficit > 0:
        return {
            "status": "insufficient_funds",
            "client_id": client_id,
            "company_name": wallet["company_name"],
            "balance_rub": wallet["balance_rub"],
            "reserved_rub": wallet["reserved_rub"],
            "available_rub": round(available, 2),
            "required_rub": round(required_amount_rub, 2),
            "deficit_rub": round(deficit, 2),
            "message": f"Недостаточно средств. Не хватает {deficit:.0f} ₽. Пополните баланс.",
            "next_action": "Сообщи клиенту сколько не хватает. Цепочку не запускай.",
        }

    return {
        "status": "sufficient",
        "client_id": client_id,
        "company_name": wallet["company_name"],
        "balance_rub": wallet["balance_rub"],
        "reserved_rub": wallet["reserved_rub"],
        "available_rub": round(available, 2),
        "required_rub": round(required_amount_rub, 2),
        "surplus_rub": round(available - required_amount_rub, 2),
        "next_action": "hold_funds",
    }


@mcp.tool()
def hold_funds(
    client_id: str,
    chain_id: str,
    amount_rub: float,
    description: str,
) -> dict:
    """Холдировать средства клиента на время выполнения цепочки.

    Вызывай ПОСЛЕ check_balance (статус sufficient) и ПЕРЕД confirm_and_launch.
    Средства резервируются — клиент не может использовать их для другой цепочки.
    После каждого шага цепочки платформа автоматически списывает стоимость шага.
    После последнего шага остаток холда возвращается на баланс.

    Args:
        client_id: ID клиента.
        chain_id: ID цепочки из build_chain.
        amount_rub: Сумма холда (полная стоимость цепочки включая комиссии).
        description: Описание для истории транзакций клиента.
    """
    wallet = _WALLET_STUB.get(client_id)
    if not wallet:
        return {"status": "error", "error": f"Клиент {client_id} не найден"}

    available = wallet["balance_rub"] - wallet["reserved_rub"]
    if amount_rub > available:
        return {
            "status": "error",
            "error": "Баланс изменился с момента проверки. Повторите check_balance.",
        }

    hold_id = _idempotency_key(f"{client_id}{chain_id}")
    wallet["reserved_rub"] = round(wallet["reserved_rub"] + amount_rub, 2)
    _HOLDS_STUB[hold_id] = {
        "client_id": client_id,
        "chain_id": chain_id,
        "held_rub": amount_rub,
        "charged_rub": 0.0,
        "description": description,
        "created_at": datetime.utcnow().isoformat(),
    }

    return {
        "status": "held",
        "hold_id": hold_id,
        "client_id": client_id,
        "held_rub": round(amount_rub, 2),
        "balance_after": {
            "total_rub": wallet["balance_rub"],
            "reserved_rub": wallet["reserved_rub"],
            "available_rub": round(wallet["balance_rub"] - wallet["reserved_rub"], 2),
        },
        "stub_warning": "В stub: реального списания нет. Wallet Service подключает разработка.",
        "next_action": "confirm_and_launch с dry_run=False",
    }

# ── ГРУППА 3: ИСПОЛНЕНИЕ ─────────────────────────────────────────────────────

@mcp.tool()
def confirm_and_launch(
    chain_id: str,
    from_city: str,
    to_city: str,
    weight_kg: float,
    client_name: str,
    client_phone: str,
    cargo_description: str,
    dry_run: bool = True,
    idempotency_key: Optional[str] = None,
) -> dict:
    """Запустить логистическую цепочку.

    ВСЕГДА вызывай сначала с dry_run=True — показать клиенту превью.
    После подтверждения клиента — dry_run=False.

    В боевой версии: создаёт накладные у каждой компании, уведомляет склады.
    В stub: возвращает превью или подтверждение без реальных вызовов API.

    Args:
        chain_id: ID цепочки из build_chain.
        from_city: Город отправления.
        to_city: Город назначения.
        weight_kg: Вес груза.
        client_name: Имя клиента.
        client_phone: Телефон клиента.
        cargo_description: Описание груза.
        dry_run: True = превью без реальных действий. False = реальный запуск.
        idempotency_key: Ключ для защиты от дублей (генерируется автоматически если не задан).
    """
    if not idempotency_key:
        idempotency_key = _idempotency_key(f"{chain_id}{client_phone}")

    preview = {
        "chain_id": chain_id,
        "client": {"name": client_name, "phone": client_phone},
        "cargo": {"description": cargo_description, "weight_kg": weight_kg},
        "route": {"from": from_city, "to": to_city},
        "what_will_happen": [
            "Шаг 1: создаётся заявка на забор у первой компании цепочки",
            "Шаг 2: уведомляется склад-отправитель по настроенным каналам",
            "Шаг 3: уведомляется склад-получатель",
            "Шаг 4: запускается трекинг цепочки",
        ],
        "idempotency_key": idempotency_key,
    }

    if dry_run:
        return {
            "status": "preview",
            "dry_run": True,
            "preview": preview,
            "message": "Это превью. Подтвердите запуск — вызовите confirm_and_launch с dry_run=False.",
        }

    # Реальный запуск (stub)
    launch_id = f"launch_{idempotency_key}"
    return {
        "status": "launched",
        "dry_run": False,
        "launch_id": launch_id,
        "chain_id": chain_id,
        "idempotency_key": idempotency_key,
        "created_at": datetime.utcnow().isoformat(),
        "steps_status": [
            {"step": 1, "status": "pending", "action": "Заявка на забор создаётся"},
            {"step": 2, "status": "pending", "action": "Уведомление складов отправляется"},
        ],
        "stub_warning": "В stub: реальные накладные не создаются. Боевой запуск — задача разработки.",
        "next_action": "notify_handover для каждой точки передачи, затем track_chain",
    }


@mcp.tool()
def notify_handover(
    launch_id: str,
    step_index: int,
    company_from_id: str,
    company_to_id: str,
    expected_handover_time: str,
    cargo_description: str,
    required_docs: Optional[list] = None,
) -> dict:
    """Уведомить склады о предстоящей передаче товара.

    Вызывай после confirm_and_launch для каждой точки передачи.
    Уведомляет склад-отправитель и склад-получатель по каналам из реестра.

    Args:
        launch_id: ID запущенной цепочки.
        step_index: Номер шага в цепочке (1, 2, 3...).
        company_from_id: ID компании-отправителя.
        company_to_id: ID компании-получателя.
        expected_handover_time: Ожидаемое время передачи (ISO или "завтра в 10:00").
        cargo_description: Описание груза.
        required_docs: Список документов для передачи (из реестра если не задан).
    """
    company_from = REGISTRY.get(company_from_id)
    company_to = REGISTRY.get(company_to_id)

    if not company_from:
        return {"status": "error", "error": f"Компания {company_from_id} не найдена"}
    if not company_to:
        return {"status": "error", "error": f"Компания {company_to_id} не найдена"}

    docs = required_docs or company_from["handover"]["as_sender"]["required_docs"]
    sender_channels = company_from["handover"]["as_sender"]["notify_channels"]
    receiver_channels = company_to["handover"]["as_receiver"]["notify_channels"]

    return {
        "status": "notified",
        "launch_id": launch_id,
        "step": step_index,
        "handover": {
            "from": {"company": company_from["name"], "notified_via": sender_channels},
            "to": {"company": company_to["name"], "notified_via": receiver_channels},
            "expected_time": expected_handover_time,
            "required_docs": docs,
            "confirmation_expected": company_to["handover"]["as_receiver"]["confirmation_type"],
        },
        "stub_warning": "В stub: реальные уведомления не отправляются. Боевая интеграция — задача разработки.",
    }


@mcp.tool()
def track_chain(
    launch_id: str,
) -> dict:
    """Получить статус логистической цепочки.

    Вызывай для мониторинга выполнения после запуска.
    В боевой версии: опрашивает tracking API каждой компании.
    При просрочке передачи — алерт клиенту и предложение пересчёта.

    Args:
        launch_id: ID запущенной цепочки из confirm_and_launch.
    """
    return {
        "status": "ok",
        "launch_id": launch_id,
        "chain_status": "in_progress",
        "steps": [
            {
                "step": 1,
                "status": "completed",
                "description": "Забор выполнен",
                "completed_at": (datetime.utcnow() - timedelta(hours=2)).isoformat(),
                "confirmation": "Трек-номер получен",
            },
            {
                "step": 2,
                "status": "in_transit",
                "description": "Груз в пути",
                "expected_arrival": (datetime.utcnow() + timedelta(hours=20)).isoformat(),
                "tracking_number": "STUB-123456",
            },
            {
                "step": 3,
                "status": "pending",
                "description": "Last-mile ожидает прибытия груза",
            },
        ],
        "overall_on_time": True,
        "next_alert_check": (datetime.utcnow() + timedelta(hours=1)).isoformat(),
        "stub_warning": "Статусы демонстрационные. Боевой трекинг через API компаний — задача разработки.",
    }


# ── ГРУППА 4: РЕЕСТР ─────────────────────────────────────────────────────────

@mcp.tool()
def register_company(
    company_json: str,
    dry_run: bool = True,
) -> dict:
    """Добавить новую компанию в реестр.

    Принимает JSON-запись по стандартной схеме реестра.
    Dry_run=True — валидация без сохранения.
    Dry_run=False — реальное добавление, агент начинает видеть компанию.

    Args:
        company_json: JSON-строка с записью компании (полная схема реестра).
        dry_run: True = только валидация, False = реальное сохранение.
    """
    try:
        data = json.loads(company_json)
    except json.JSONDecodeError as e:
        return {"status": "error", "error": f"Невалидный JSON: {e}"}

    required_fields = ["id", "name", "capabilities", "coverage", "constraints",
                       "time_windows", "handover", "pricing", "adapter"]
    missing = [f for f in required_fields if f not in data]
    if missing:
        return {
            "status": "validation_error",
            "missing_fields": missing,
            "message": "Заполните обязательные поля перед добавлением",
        }

    company_id = data["id"]

    if dry_run:
        return {
            "status": "valid",
            "dry_run": True,
            "company_id": company_id,
            "company_name": data.get("name"),
            "capabilities": data.get("capabilities"),
            "coverage_cities": len(data.get("coverage", {}).get("cities", [])),
            "message": "Запись валидна. Вызовите с dry_run=False для реального сохранения.",
        }

    # Реальное сохранение
    REGISTRY[company_id] = data
    registry_path = REGISTRY_DIR / f"{company_id}.json"
    registry_path.write_text(json.dumps(data, ensure_ascii=False, indent=2))

    return {
        "status": "registered",
        "dry_run": False,
        "company_id": company_id,
        "company_name": data.get("name"),
        "message": f"Компания {data.get('name')} добавлена в реестр. Агент уже её видит.",
        "capabilities": data.get("capabilities"),
        "coverage_cities": len(data.get("coverage", {}).get("cities", [])),
    }


@mcp.tool()
def update_company_data(
    company_id: str,
    field: str,
    value: str,
) -> dict:
    """Обновить данные компании в реестре без перезапуска сервера.

    Используй для обновления тарифов, временны́х окон, покрытия.

    Args:
        company_id: ID компании в реестре.
        field: Поле для обновления (например: pricing.base_per_kg_rub, time_windows.order_cutoff).
        value: Новое значение (строка, число или JSON).
    """
    company = REGISTRY.get(company_id)
    if not company:
        return {"status": "error", "error": f"Компания {company_id} не найдена"}

    # Поддерживаем точечную нотацию: "pricing.base_per_kg_rub"
    parts = field.split(".")
    try:
        parsed_value = json.loads(value)
    except (json.JSONDecodeError, ValueError):
        parsed_value = value

    target = company
    for part in parts[:-1]:
        if part not in target:
            return {"status": "error", "error": f"Поле '{part}' не найдено"}
        target = target[part]

    old_value = target.get(parts[-1])
    target[parts[-1]] = parsed_value

    # Сохраняем на диск
    registry_path = REGISTRY_DIR / f"{company_id}.json"
    registry_path.write_text(json.dumps(company, ensure_ascii=False, indent=2))

    return {
        "status": "updated",
        "company_id": company_id,
        "field": field,
        "old_value": old_value,
        "new_value": parsed_value,
        "message": f"Данные {company['name']} обновлены. Агент уже использует новые значения.",
    }


# ── Запуск ────────────────────────────────────────────────────────────────────

def main():
    company_names = [c["name"] for c in REGISTRY.values()]
    print(f"Logistics Orchestration Platform v0.1 started")
    print(f"Companies in registry: {len(REGISTRY)} — {', '.join(company_names)}")
    print(f"Tools: 8 (get_capable_companies, get_company_slots, build_chain,")
    print(f"        estimate_chain_cost, confirm_and_launch, notify_handover,")
    print(f"        track_chain, register_company, update_company_data)")
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
