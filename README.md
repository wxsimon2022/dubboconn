# dubboconn

> One-package solution for Nacos service discovery + Dubbo consumer connectivity in Go.

`dubboconn` combines a full-featured Nacos client (service discovery, configuration
management) with dubbo-go consumer proxy creation. No separate Nacos wrapper packages
needed — everything lives in this module.

## Features

- **Nacos Client** — `NewNacos()` for standalone service discovery and config management
- **Dubbo Connect** — `Connect()` discovers a provider via Nacos and creates a ready-to-use
  dubbo-go consumer proxy in a single call
- **Provider Change Notification** — `Connection.Watch()` subscribes to Nacos instance changes
- **Config Management** — `GetConfig`, `ListenConfig`, `PublishConfig`, `DeleteConfig`
- **No Extra Dependencies** — Nacos wrapper code is inlined; only `nacos-sdk-go` and `dubbo-go`

## Installation

```bash
go get github.com/wxsimon2022/dubboconn
```

If your project vendors dependencies, run `go mod vendor` after the `go get`.

## Quick Start

### 1. Define your Dubbo service interface as a Go struct

```go
import "context"

type Greeter struct {
    SayHello   func(ctx context.Context, name string) (string, error)
    SayGoodBye func(ctx context.Context, name string) (string, error)
}
```

Each exported field must be a function type matching your Dubbo Java interface method
signature. dubbo-go uses reflection to wire these fields to RPC proxies.

### 2. Connect — discover & create proxy in one call

```go
import "github.com/wxsimon2022/dubboconn"

var svc Greeter

conn, err := dubboconn.Connect(dubboconn.Config{
    // Nacos server
    NacosHost:      "127.0.0.1",
    NacosPort:      8848,
    NacosNamespace: "public",
    NacosUsername:  "nacos",
    NacosPassword:  "nacos",

    // Dubbo service to discover
    ServiceName:   "providers:org.apache.dubbo.demo.Greeter::",
    InterfaceName: "org.apache.dubbo.demo.Greeter",
}, &svc)
if err != nil {
    log.Fatalf("connect: %v", err)
}
```

### 3. Call methods

```go
resp, err := svc.SayHello(context.Background(), "world")
if err != nil {
    log.Printf("RPC failed: %v", err)
}
fmt.Println(resp) // "hello, world" (from Java Dubbo server)
```

### 4. (Optional) Watch for provider changes

```go
conn.Watch(func(newURL string) {
    log.Printf("Provider changed to: %s", newURL)
})
```

## Standalone Nacos Client

If you only need Nacos service discovery or config management (no Dubbo):

```go
client, err := dubboconn.NewNacos(dubboconn.NacosConfig{
    Host:      "127.0.0.1",
    Port:      8848,
    Namespace: "public",
})

// Service discovery
instances, _ := client.GetInstances("my-service")
for _, inst := range instances {
    fmt.Printf("  %s:%d (healthy=%v)\n", inst.Ip, inst.Port, inst.Healthy)
}

// Subscribe to changes
client.Watch("my-service", func(instances []model.Instance) {
    fmt.Printf("now %d instances\n", len(instances))
})

// Configuration
val, _ := client.GetConfig("app.yml", dubboconn.WithGroup("APP"))
client.ListenConfig("app.yml", func(v string) {
    fmt.Println("config updated:", v)
})
```

## Config Reference

### `NacosConfig` — for `NewNacos()`

| Field | Default | Description |
|-------|---------|-------------|
| `Host` | — (required) | Nacos server address |
| `Port` | `8848` | Nacos server port |
| `Namespace` | `"public"` | Nacos namespace ID |
| `Username` | — | Nacos auth username |
| `Password` | — | Nacos auth password |
| `AppName` | — | Application identifier |
| `LogDir` | — | SDK log output directory |
| `LogLevel` | `"info"` | SDK log verbosity |
| `TimeoutMs` | `5000` | Request timeout (ms) |

### `Config` — for `Connect()`

| Field | Default | Description |
|-------|---------|-------------|
| `NacosHost` | — (required) | Nacos server address |
| `NacosPort` | `8848` | Nacos server port |
| `NacosNamespace` | `"public"` | Nacos namespace |
| `NacosUsername` | — | Nacos auth username |
| `NacosPassword` | — | Nacos auth password |
| `ServiceName` | — (required) | Nacos service name to discover |
| `InterfaceName` | — (required) | Dubbo interface fully-qualified name |
| `Protocol` | `"tri"` | RPC protocol (Triple) |
| `Retries` | `"2"` | RPC retry count |
| `RequestTimeout` | `"30s"` | RPC timeout |
| `Serialization` | `"hessian2"` | Serialization format |
| `ProbeTimeout` | `0` (skip) | Connection probe timeout |
| `NacosAppName` | `InterfaceName` | Application identifier sent to Nacos |

## Error Handling

The package returns standard Go errors that can be checked with `errors.Is`:

```go
import "errors"

_, err := dubboconn.Connect(...)
if errors.Is(err, dubboconn.ErrNoInstances) {
    log.Println("no provider available — is the Java service running?")
}
```

Sentinel errors:
- `ErrNamingNotInit` — Nacos naming client is nil
- `ErrConfigNotInit` — Nacos config client is nil (skipped during init)
- `ErrNoInstances` — No available instances found for the service
- `ErrNotConnected` — Operation attempted before connection

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   dubboconn                          │
│                                                      │
│  NewNacos(NacosConfig) → *Client                     │
│    ├─ GetInstances / Watch      (service discovery)  │
│    ├─ Register / Deregister     (service registry)   │
│    └─ GetConfig / ListenConfig  (config management)  │
│                                                      │
│  Connect(Config, &svc) → *Connection                 │
│    ├─ 1. NewNacos()         → init Nacos client      │
│    ├─ 2. GetInstances()     → discover provider      │
│    ├─ 3. ReferenceConfig    → create dubbo proxy     │
│    └─ 4. ref.Refer/Implement → wire function fields  │
│         ↓                                            │
│    svc.SayHello(ctx, "world") → works immediately    │
└─────────────────────────────────────────────────────┘
```

## Requirements

- Go 1.23+
- A running Nacos server with registered Dubbo (Triple) providers
- dubbo-go v3 (imported transitively through this package)

## License

MIT
