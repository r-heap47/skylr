# skylr

![Skylr](docs/skylr.png)

Распределённый key-value кеш с горизонтальным масштабированием.

Клиенты обращаются к единой точке входа — **overseer**, который:
- определяет целевой шард по **consistent hashing** (FNV-1a, виртуальные ноды на кольце);
- проксирует запрос на выбранный шард.

Шарды хранят пары ключ–значение в памяти с TTL. Overseer сам поднимает их как процессы, следит за живыми, удаляет павшие и мигрирует ключи при изменении топологии.

**Назначение:** кеш сессий, rate limit, временные данные — там, где важно масштабирование по горизонтали и минимизация перестроек при добавлении/удалении узлов.

## Компоненты

- **skylr-overseer** — центральный gRPC-сервис: роутинг, мониторинг шардов, подъём новых шардов
- **skylr-shard** — хранилище ключей, поднимается overseer'ом как процесс
- **skylr-client** — CLI для get/set/delete

## Требования

- Go 1.25+
- `protoc` (для сборки overseer и shard)
- protoc-плагины: `protoc-gen-go`, `protoc-gen-go-grpc` (устанавливаются через `make` в каждом подпроекте)

## Локальный запуск

### 1. Собрать шард

Overseer сам стартует шарды, но бинарник должен уже быть собран:

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

### 3. Собрать клиент

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

В каждом подпроекте: `make build`, `make build-bin`, `make test`, `make lint`.
