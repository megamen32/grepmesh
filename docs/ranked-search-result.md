## Owner decision — 2026-11-01: supersedes standalone Ranked

One public search, rank=true by default; rank=false opts out. Ranking only
orders already-authorized matches and must not narrow literal/regex, OCR or
historical deleted-file coverage. The old ranked_search tool is removed, not
aliased. Do not install a second ranked index/service or reintroduce its MCP entry.
The independent source-manifest selector remains a corpus policy utility only.

Acceptance: default ranking before top-k; deterministic opt-out; identical match
sets within the reported candidate/deadline budget; compact/job output preserves
relevance order (compact fields remain unchanged); forged ACL denied; removed tool unavailable. Keep original indexers and
SQLite untouched during the ongoing cloud migration. A source commit is NOT a
claim that the native production runtime has been switched.

Current step: 14 focused Rust checks (including real MCP, compact output and
no-index fallback), 1 verified-metadata ACL regression and 9 bridge/source-policy
checks passed: 24 targeted checks. The compact-output regression was fixed without
adding score fields to compact responses; verbose responses carry scores. Native
production release remains pending; the test binary deliberately has no OCR/STT
features and MUST NOT be deployed as a replacement runtime. The owner explicitly retired the standalone variant; only its isolated
server-100-ranked listener may be disabled. Keep canonical port 9419 and its
original indexers running until the approved migration/release gate. No original SQLite/WAL/SHM removal is authorized by this change.

Additional regression note: the old broad mesh fixture suite is not accepted: four
cases failed and an unauthenticated fake-peer fixture stalled. Its isolated test
unit was stopped; no live service was touched. Do not count this as mesh/ACL
production acceptance. Focused new ACL and API contract checks are separate.

Live retirement receipt (2026-11-01): grepmesh-ranked.service is disabled and
inactive. Its dedicated port 19431 was retired; original port 9419 retained PID
3068 without restart. The standalone index.sqlite (496373760 bytes) is preserved.
This is intentional retirement, NOT an incident to auto-repair or re-enable.
Receipt: ~/.local/state/grepmesh-ranked-retired-20261101.json.
Native production rank=true API is NOT installed yet. Do not deploy the no-feature
test binary or stop original migration indexers to make the API look complete.
The legacy asynchronous/fanout fixture suite is still unaccepted; the isolated
async fixture expects a running job but receives an immediate response. No ACL
policy was loosened to make these older fixtures green.

---

# Ранжированный GrepMesh: установленный пилот

Добавлен отдельный инструмент `ranked_search`: вопрос из нескольких слов
превращается в ограниченную выдачу фрагментов с хостом, путём, физическими
строками, оценкой и проверкой текущего исходника. Прежний `search` сохраняет
literal/regex-контракт. Это лексическое ранжирование поверх существующего FTS;
новый нейросетевой сервис не нужен.

Финальная установка проверена на артефакте `c74678c6`: service active/enabled,
13 внутренних групп и HTTPX доступны через реальные поиски. После согласования
manifest с исключениями индексатора допущены и сохранены 11 332 уникальных
пути, пропусков и файлов вне manifest — ноль. Две свежие Luna-сессии в Codex
получили ответы по HTTPX и внутреннему Airlock через установленный MCP;
OpenCode и GPTAdmin также выполнили настоящие поиски. Политика подтверждений
Codex осталась `never`, sandbox — `read-only`; исправлены MCP-аннотации
инструментов чтения. Квитанции: `installed-acceptance.json`,
`native-consumer.json` и `installed-gptadmin.json` в benchmark-каталоге.

## Где пользоваться

- В Codex и OpenCode: MCP `grepmesh-ranked`, инструмент `ranked_search`.
- В GPTAdmin: дочерний MCP `GrepMeshRanked`.
- Локальный отдельный экземпляр: `http://127.0.0.1:19431/mcp`, user unit
  `grepmesh-ranked.service`. Основной fleet-сервис на 9419 не заменяется.

Начните с `list_locations`, затем ограничьте запрос именем проекта:

```json
{"query":"HTTPTransport connection retries","hosts":"local","roots":["httpx-reference"],"limit":3,"context_lines":3}
```

Имена внутренних групп: `serversadmin`, `herder`, `broker`, `telegram`, `tgc`,
`atv`, `commentorch`, `videowatching`, `exmanager`, `airlock`, `airlockadmin`,
`userio`, `autogram`. Внешний `httpx-reference` содержит пять выбранных файлов
HTTPX с commit, лицензией и SHA в manifest; это не поиск по всему GitHub.

Правила выбора установлены отдельным skill для обоих клиентов: XERJ остаётся
основным инструментом поиска по вопросу и внешним Hub, `rg` — для точного
локального пути/идентификатора, прежний Mesh — для обнаружения неизвестного
хоста/проекта. Новый ranked-инструмент доступен для явно выбранного корпуса
и сравнений; его качество пока не оправдывает автоматическую замену XERJ.

## Что показали измерения

Фиксированные 24 запроса, одинаковый docs-корпус и неизменные gold:

| Показатель | Прежний literal | Новый ranked v3 |
|---|---:|---:|
| MRR | 0.2014 | 0.4306 |
| Recall@5 | 25% | 57.5% |
| Natural-language gold hit | 0/7 | 4/7 |

Предварительная цель 85% **не достигнута**. У двух вопросов единственный gold
находится вне общего docs-корпуса; исходные метрики сохранены без изменения
эталонов. Другие промахи также видны в отчёте
`benchmarks/ranked-search/candidate-v3-comparison.md`.

Два слепых прохода настоящей Luna по одинаковым ограниченным фрагментам
дали 0/6 подтверждённых положительных ответов для literal и 2/6 для ranked
и XERJ; каждый режим правильно отказался придумывать PIN в 2/2 случаях.
Это маленький тест без повторных поисков и чтений, а не оценка автономного
агента или доказательство превосходства. Детали и реальные usage лежат в
`benchmarks/ranked-search/luna/`.

Чередующиеся вызовы exact на прежнем и новом экземпляре сохранили одинаковые
результаты в 90/90 парах: P95 151.19 против 151.82 мс. Ranked v3 имел P95
473.48 мс под общей I/O-нагрузкой. Engine-time не измерялся: нельзя приписать
разницу одному ranker. Экономия агентских токенов не доказана.

## Границы корректности

`strong_match` означает совпадение слов, **не достоверность ответа**. Читайте
фрагмент и проверяйте сам факт; отсутствие источника в выдаче не доказывает
его отсутствия в репозитории. Сохраняйте `partial`, `truncated`, предупреждения
и ошибки отдельных хостов.

Положительный manifest допускает выбранные tracked-файлы исходников и
документации; логи, private/runtime/generated деревья и другие исключения
не обещаются. Это фильтр путей, а не классификатор секретов внутри исходника.
Изменения допущенных файлов отслеживаются; для новых/переименованных путей
нужно обновление manifest и перезапуск собственного индексатора. Unit делает
обновление при старте. Не расширяйте corpus ради одного пустого запроса.

Проверены реальные edit/delete/recreate/rename, запрет нового пути, symlink,
отзыв manifest и его повреждение: `runtime-lifecycle-final.json`, 9/9.
Unix ranked-reader открывает компоненты без следования symlink и проверяет
обычный файл на открытом descriptor. Старый ingestion и non-Unix путь не
объявляются sandbox против враждебного процесса, меняющего дерево одновременно.

Окончательные installed/runtime и rollback-квитанции приведены в
`ranked-search-rollout.md`; benchmark v3 относится к артефакту `16519543`,
а последующие исправления metadata и ограничений tokenizer указываются
отдельно. Нельзя выдавать исторический benchmark за повторный полный прогон
нового артефакта.
