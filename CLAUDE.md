# Skylr — Code Guidelines

## Go Code Style

### Comments

- Exported types and functions use a single godoc line: `// Foo does X.`
- Simple struct fields (`string`, `int`, `bool`) use **inline** trailing comments: `Timeout int // max wait in seconds`
- `func`-typed fields and `time.Duration` fields use a **godoc line above**:
  ```go
  // ShardCount returns the total number of registered shards.
  ShardCount func() int
  ```
- Inline comments inside functions explain non-obvious logic only — not what the code literally does.

### Error messages

- Always lowercase, no trailing punctuation.
- Wrap with context using `fmt.Errorf("operation: %w", err)`.
- Include relevant identifiers quoted with `%q`: `fmt.Errorf("shard %q not found", addr)`.

### Logging

Format: `[LEVEL] module: message`

```go
log.Printf("[INFO] provisioner: shard %s registered", addr)
log.Printf("[ERROR] shard %s reported failure: %s", addr, err)
```

Levels in use: `[INFO]`, `[WARN]`, `[ERROR]`, `[GRPC]`.

### Mutexes

Always use `defer` for unlock — never unlock manually in conditional branches:

```go
// correct
p.mu.Lock()
defer p.mu.Unlock()
if condition {
    return "", fmt.Errorf("...")
}

// wrong
p.mu.Lock()
if condition {
    p.mu.Unlock()  // easy to forget
    return "", fmt.Errorf("...")
}
p.mu.Unlock()
```

When the critical section is short and must release before I/O, extract it into a dedicated helper method that uses `defer` internally (see `startShard`, `removeShard` in `process.go`).

### Naming

- Receiver names: single letter or short abbreviation — `p` for Provisioner, `ovr` for Overseer, `a` for Autoscaler.
- Config variable: always `cfg`.
- Map fields: `addr -> thing` documented in a comment on the field.

### Switches vs if-else chains

Use a tagged `switch` when comparing the same variable against string constants:

```go
switch cfg.Provisioner.Type {
case "process":
    ...
case "kubernetes":
    ...
}
```

### Integer conversions

When converting `int` to `int32` where overflow is provably impossible (e.g. port numbers), suppress gosec with an inline explanation:

```go
ContainerPort: int32(p.cfg.GRPCPort), //nolint:gosec // port numbers fit in int32
```

### Import grouping (gofmt)

Two groups only — stdlib first, then everything else:

```go
import (
    "context"
    "fmt"

    "github.com/r-heap47/skylr/..."
    "google.golang.org/grpc"
    "k8s.io/..."
)
```

### Timer / ticker pattern

Declare and defer Stop on adjacent lines, no blank line between them:

```go
deadline := time.NewTimer(timeout)
defer deadline.Stop()
ticker := time.NewTicker(interval)
defer ticker.Stop()
```
