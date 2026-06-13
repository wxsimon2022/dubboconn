# dubboconn

> Zero-boilerplate Dubbo consumer for Go — discover providers via Nacos, create a proxy, call methods.

Combines [nacoswrap](https://github.com/wxsimon2022/nacoswrap) (Nacos service discovery) with the dubbo-go consumer into a single `Connect` call. No manual Nacos queries, no hard-coded provider URLs, no boilerplate.

## Features

- **One call** — Nacos discovery + dubbo-go proxy in a single `Connect`
- **No boilerplate** — just define your interface as a Go struct and call methods
- **Provider change notification** — `Watch()` for Nacos instance changes
- **Probe timeout** — optional connection readiness check

## Installation

```bash
go get github.com/wxsimon2022/dubboconn
```

## Quick Start

### 1. Define your Dubbo service interface

```go
type Greeter struct {
    SayHello   func(ctx context.Context, name string) (string, error)
    SayGoodBye func(ctx context.Context, name string) (string, error)
}
```

The struct fields must be function types matching your Dubbo Java interface methods.

### 2. Connect

```go
var svc Greeter

_, err := dubboconn.Connect(dubboconn.Config{
    // Nacos
    NacosHost:     "127.0.0.1",
    NacosPort:     8848,
    NacosNamespace: "public",

    // Dubbo service to discover
    ServiceName:   "providers:org.apache.dubbo.demo.Greeter::",
    InterfaceName: "org.apache.dubbo.demo.Greeter",
}, &svc)
if err != nil {
    log.Fatal(err)
}
```

### 3. Call methods

```go
resp, err := svc.SayHello(context.Background(), "world")
if err != nil {
    log.Printf("RPC failed: %v", err)
}
fmt.Println(resp)
```

### 4. (Optional) Watch for provider changes

```go
conn.Watch(func(newURL string) {
    log.Printf("Provider changed to: %s", newURL)
})
```

## Config Reference

| Field | Default | Description |
|-------|---------|-------------|
| NacosHost | — (required) | Nacos server address |
| NacosPort | 8848 | Nacos server port |
| NacosNamespace | "public" | Nacos namespace |
| NacosUsername | — | Nacos auth username |
| NacosPassword | — | Nacos auth password |
| ServiceName | — (required) | Nacos service name |
| InterfaceName | — (required) | Dubbo interface name |
| Protocol | "tri" | Protocol (triple) |
| Retries | "2" | RPC retry count |
| RequestTimeout | "30s" | RPC timeout |
| Serialization | "hessian2" | Serialization format |
| ProbeTimeout | 0 (skip) | Connection probe timeout |

## How It Works

```
You call Connect(cfg, &svc)
  │
  ├─ 1. nacoswrap.NewClient()        → connect to Nacos
  ├─ 2. client.GetInstances(name)    → discover provider IP:port
  ├─ 3. dubboCfg.ReferenceConfig{...} → create consumer proxy
  └─ 4. ref.Refer() + ref.Implement() → populate struct function fields
                                         ↓
                               svc.SayHello() → works immediately
```

## Requirements

- Go 1.23+
- A running Nacos server with registered Dubbo providers
- dubbo-go v3 (Triple protocol)

## License

MIT
