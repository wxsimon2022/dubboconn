# dubboconn

> One package for Nacos service discovery + Dubbo consumer connectivity in Go.

Combines Nacos client (service discovery, config management) with dubbo-go consumer
proxy creation — no separate packages needed.

## Features

- **NewNacos()** — standalone Nacos client (discovery + config)
- **Connect()** — discover Dubbo provider via Nacos, create consumer proxy
- **Watch()** — subscribe to provider changes
- **GetConfig / ListenConfig** — Nacos configuration management
- No external Nacos wrapper dependency — everything in one package

## Installation

```bash
go get github.com/wxsimon2022/dubboconn
```

## Usage

### Dubbo — Discover & Connect

```go
type Greeter struct {
    SayHello func(ctx context.Context, name string) (string, error)
}

var svc Greeter
_, err := dubboconn.Connect(dubboconn.Config{
    NacosHost:     "127.0.0.1",
    NacosPort:     8848,
    ServiceName:   "providers:org.apache.dubbo.demo.Greeter::",
    InterfaceName: "org.apache.dubbo.demo.Greeter",
}, &svc)
// svc.SayHello(ctx, "world") works now
```

### Nacos — Standalone Client

```go
client, err := dubboconn.NewNacos(dubboconn.NacosConfig{
    Host: "127.0.0.1",
    Port: 8848,
})

instances, _ := client.GetInstances("my-service")
client.Watch("my-service", func(instances []model.Instance) { ... })

val, _ := client.GetConfig("app.yml", dubboconn.WithGroup("APP"))
client.ListenConfig("app.yml", func(v string) { ... })
```

## API Overview

```
NewNacos(cfg NacosConfig) → *Client, error
  ├─ GetInstances(name, opts...) → []model.Instance, error
  ├─ Watch(name, onChange) → error
  ├─ Unwatch(name) → error
  ├─ RegisterInstance / DeregisterInstance
  ├─ GetConfig / PublishConfig / DeleteConfig
  └─ ListenConfig / CancelListenConfig

Connect(cfg Config, svc) → *Connection, error
  └─ Connection.Watch(onChanged) → error
  └─ Connection.Unwatch() → error
  └─ Connection.CurrentURL() → string
```

## Requirements

- Go 1.23+
- Nacos server with registered Dubbo providers

## License

MIT
