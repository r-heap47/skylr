# skylr

![Skylr](docs/skylr.png)

Skylr — распределённый in-memory key-value кеш с горизонтальным масштабированием на Go.

Единая точка входа — **overseer** — принимает запросы клиентов, определяет целевой шард через **consistent hashing** (FNV-1a, виртуальные ноды на кольце) и проксирует запрос напрямую на него. Шарды хранят пары ключ–значение в памяти с TTL и самостоятельно регистрируются на overseer при старте. При изменении топологии ключи мигрируют с минимальными перестройками кольца. **Autoscaler** следит за нагрузкой и автоматически поднимает новые шарды, когда метрики устойчиво превышают пороговые значения.

Назначение: кеш сессий, rate limit, временные данные — там, где важно масштабирование по горизонтали и минимизация перестроек при добавлении/удалении узлов.

## Компоненты

- **skylr-overseer** — центральный gRPC-сервис: роутинг, мониторинг шардов, подъём новых шардов, автоскейлинг
- **skylr-shard** — хранилище ключей, поднимается overseer'ом как процесс; поддерживает политики вытеснения: NoEviction, LRU, LFU, Random
- **skylr-client** — CLI для get/set/delete

## Требования

- Go 1.25+
- `protoc` (для сборки всех трёх компонентов)
- protoc-плагины: `protoc-gen-go`, `protoc-gen-go-grpc`

Установка плагинов:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

## Локальный запуск

### 1. Собрать шард

Overseer сам стартует шарды как subprocess, но бинарник должен уже быть собран:

```bash
cd skylr-shard
make build-bin
```

Бинарник окажется в `skylr-shard/bin/skylr-shard`. Конфиг overseer (`skylr-overseer/config/config.yaml`) ожидает его по пути `../skylr-shard/bin/skylr-shard` относительно директории overseer.

### 2. Запустить overseer

Overseer слушает gRPC на `0.0.0.0:9000`, поднимает до 5 шардов на портах 5000–5999:

```bash
cd skylr-overseer
make run
```

По умолчанию используется `config/config.yaml`. Для другого конфига: `make run CONFIG=path/to/config.yaml`.

При запуске overseer автоматически поднимает 1 начальный шард (`initial_shards: 1`) и запускает autoscaler.

### 3. Собрать клиент

`build-bin` автоматически запускает `pbgen`, поэтому `protoc` и плагины должны быть установлены (см. Требования):

```bash
cd skylr-client
make build-bin
```

Бинарник: `skylr-client/bin/skylr-client`.

### 4. Использовать клиент

Укажи адрес overseer через переменную окружения или флаг:

```bash
export SKYLR_OVERSEER=localhost:9000
./bin/skylr-client set foo bar
./bin/skylr-client get foo
./bin/skylr-client delete foo
```

## Автоскейлинг

Overseer непрерывно собирает метрики с каждого шарда и автоматически поднимает новые шарды при устойчивой нагрузке.

**Как это работает:**

1. **Observer** — на каждый шард заводится отдельная горутина, которая опрашивает `Metrics()` каждые `observer_delay` (1 с) и кеширует последний ответ.
2. **Autoscaler** тикает каждые `eval_interval` (1 с), собирает агрегированные метрики по всем шардам и прогоняет их через список `ScalingRule`.
3. Если хотя бы одно правило срабатывает `sustained_for` тиков подряд — подаётся команда на provisioner для запуска нового шарда.
4. После скейла действует **cooldown** (5 с) — пауза, пока `migrateKeys` перераспределяет ключи на новый шард.

**Текущие правила (`ScalingRule`):**

| Правило | Условие | Описание |
|---------|---------|----------|
| `ItemCountRule` | `TotalItems / ShardCount >= threshold` | Среднее число ключей на шард достигло порога. Знаменатель растёт после скейла — метрика опускается ниже порога без ручного сброса |

**Миграция ключей после скейла:**

После добавления нового шарда в кольцо `migrateKeys` запускается в фоне: сканирует все шарды, находит ключи, которые по новой топологии принадлежат новому шарду, и перемещает их (`Set` на новом + `Delete` на старом). Consistent hashing гарантирует минимальную долю перемещаемых ключей.

**Конфигурация** (`skylr-overseer/config/config.yaml`):

```yaml
autoscaler:
  enabled: true
  eval_interval: "1s"   # частота проверки метрик
  cooldown: "5s"        # пауза между двумя scale-up
  sustained_for: 3      # правило должно сработать N тиков подряд

provisioner:
  process:
    max_shards: 5       # жёсткий лимит шардов
    initial_shards: 1   # шарды при старте overseer

  # autoscaler.rules.item_count.threshold: 100  (ключей/шард)
```

| Параметр | Значение по умолчанию | Описание |
|----------|-----------------------|----------|
| `eval_interval` | `1s` | Как часто autoscaler оценивает метрики |
| `cooldown` | `5s` | Минимальный интервал между scale-up событиями |
| `sustained_for` | `3` | Число подряд идущих тиков для подтверждения нарушения |
| `rules.item_count.threshold` | `100` | Порог ключей/шард для `ItemCountRule` |
| `max_shards` | `5` | Максимальное число шардов |

## skylr-client

CLI для работы с кешем: get, set, delete.

**Команды:**

| Команда | Описание |
|---------|----------|
| `get <key>` | Получить значение по ключу. Пустой вывод и exit 1 — ключ не найден |
| `set <key> <value>` | Записать пару. TTL задаётся флагом `-ttl` (по умолчанию 24h) |
| `delete <key>` | Удалить ключ. Выводит `deleted` или `not found` |

**Флаги:**

| Флаг | Описание | По умолчанию |
|------|----------|--------------|
| `-overseer` | Адрес overseer (host:port) | `$SKYLR_OVERSEER` |
| `-ttl` | TTL для `set` (например `1h`, `30m`) | `24h` |
| `-v` | Подробный вывод в stderr | false |

**Примеры:**

```bash
skylr-client set user:123 "John Doe"
skylr-client get user:123
skylr-client get nonexistent   # error: not found: nonexistent
skylr-client delete user:123
skylr-client -ttl=5m set temp x   # истечёт через 5 минут
skylr-client -v set foo bar       # вывод в stderr
```

**Массовая запись:**

Скрипт `scripts/bulk-set.sh` делает N операций set с ключами `<prefix>:0` … `<prefix>:(N-1)`:

```bash
./scripts/bulk-set.sh 500         # 500 ключей key:0 .. key:499
./scripts/bulk-set.sh 100 user    # 100 ключей user:0 .. user:99
```

## Сборка и тесты

В каждом подпроекте доступны: `make build`, `make build-bin` (shard и client), `make test`, `make lint`.
