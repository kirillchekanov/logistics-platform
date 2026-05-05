# Logistics Orchestration Platform — MCP Server v0.1

Агент строит оптимальные логистические цепочки из компаний реестра.
Клиент говорит задачу — агент сам находит исполнителей и координирует передачи.

**Версия:** stub v0.1  
**Компании в реестре:** Даркстор у дома, СДЭК, ПЭК  
**Tools:** 9

---

## Быстрый старт

```bash
pip3 install mcp fastmcp
python3 server.py
```

Успешный запуск:
```
Logistics Orchestration Platform v0.1 started
Companies in registry: 3 — СДЭК, Даркстор у дома, ПЭК
Tools: 9 (get_capable_companies, get_company_slots, ...)
```

## Подключение к Claude Desktop

```json
{
  "mcpServers": {
    "logistics-orchestrator": {
      "command": "python3",
      "args": ["/полный/путь/до/server.py"]
    }
  }
}
```

## Структура проекта

```
logistics-platform/
├── server.py              # MCP-сервер, все 9 tools
├── registry/
│   ├── darkstore.json     # Даркстор у дома
│   ├── cdek.json          # СДЭК
│   └── pek.json           # ПЭК
├── test_scenarios.md      # 12 тест-сценариев
└── README.md
```

## Добавить новую компанию

Создайте файл `registry/company_id.json` по схеме существующих записей.
Перезапустите сервер — агент сразу начинает видеть компанию.

Или попросите агента:
> «Добавь компанию X в реестр» — он запросит данные и вызовет `register_company`.

## Tools

| Tool | Группа | Описание |
|------|--------|----------|
| `get_capable_companies` | Разведка | Фильтрует реестр по маршруту, весу, срокам |
| `get_company_slots` | Разведка | Временны́е окна компании |
| `build_chain` | Построение | Строит 2-3 варианта цепочек |
| `estimate_chain_cost` | Построение | Считает стоимость |
| `confirm_and_launch` | Исполнение | Запускает цепочку (dry_run → confirm) |
| `notify_handover` | Исполнение | Уведомляет склады о передаче |
| `track_chain` | Исполнение | Статус и трекинг |
| `register_company` | Реестр | Добавить компанию |
| `update_company_data` | Реестр | Обновить поле без перезапуска |

## Что делает разработка (Phase 2-3)

- Боевые вызовы pricing API каждой компании
- Реальное создание накладных (СДЭК API v2, ПЭК API v1)
- Webhook-уведомления складов
- Трекинг через API компаний
- БД для истории цепочек
