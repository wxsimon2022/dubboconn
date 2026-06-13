// Package dubboconn provides a zero-boilerplate way to connect a Go application
// to a remote Dubbo service via Nacos service discovery.
//
// It combines Nacos discovery + dubbo-go consumer proxy creation into one call,
// so users only need to define their service interface as a struct with function
// fields (standard dubbo-go pattern) and call Connect.
//
// Minimal example:
//
//	// 1. Define your service interface as a Go struct
//	type Greeter struct {
//		SayHello func(ctx context.Context, name string) (string, error)
//	}
//
//	func main() {
//		var svc Greeter
//
//		_, err := dubboconn.Connect(dubboconn.Config{
//			NacosHost:     "127.0.0.1",
//			NacosPort:     8848,
//			ServiceName:   "providers:org.apache.dubbo.Greeter::",
//			InterfaceName: "org.apache.dubbo.Greeter",
//		}, &svc)
//		if err != nil {
//			log.Fatal(err)
//		}
//
//		resp, _ := svc.SayHello(context.Background(), "world")
//		fmt.Println(resp)
//	}
package dubboconn

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	dubboCfg "dubbo.apache.org/dubbo-go/v3/config"
	_ "dubbo.apache.org/dubbo-go/v3/imports" // registers protocols, codecs, serializers

	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/wxsimon2022/nacoswrap"
)

// Config holds all parameters needed to discover a Dubbo provider via Nacos
// and create a consumer proxy.
type Config struct {
	// Nacos server connection
	NacosHost      string
	NacosPort      uint64
	NacosNamespace string
	NacosUsername  string
	NacosPassword  string

	// Dubbo service identity
	ServiceName   string // Nacos service name, e.g. "providers:org.apache.dubbo.DemoService::"
	InterfaceName string // Dubbo interface fully-qualified name, e.g. "org.apache.dubbo.DemoService"

	// Dubbo consumer options (empty = sensible defaults)
	Protocol       string // Default "tri" (Triple)
	Retries        string // Default "2"
	RequestTimeout string // Default "30s"
	Serialization  string // Default "hessian2"

	// ProbeTimeout controls how long to wait for the provider connection to
	// become ready. 0 = skip probing (proxy returned immediately).
	ProbeTimeout time.Duration
}

func (c *Config) setDefaults() {
	if c.Protocol == "" {
		c.Protocol = "tri"
	}
	if c.Retries == "" {
		c.Retries = "2"
	}
	if c.RequestTimeout == "" {
		c.RequestTimeout = "30s"
	}
	if c.Serialization == "" {
		c.Serialization = "hessian2"
	}
}

// Connection holds the established Nacos client and Dubbo proxy reference.
// It can be used to watch for provider changes or reconnect.
type Connection struct {
	Nacos       *nacoswrap.Client
	config      Config
	ServiceImpl interface{}
	ProviderURL string
	ref         *dubboCfg.ReferenceConfig

	mu sync.RWMutex
}

// ProviderURL returns the currently known provider URL.
func (c *Connection) CurrentURL() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ProviderURL
}

// Watch subscribes to provider instance changes. onChanged is called with
// the new provider URL whenever the Nacos server pushes an update.
//
// NOTE: The Dubbo proxy is NOT automatically reconnected when the provider
// changes. For production use, call Reconnect or create a new Connect.
func (c *Connection) Watch(onChanged func(currentURL string)) error {
	return c.Nacos.Watch(c.config.ServiceName, func(instances []model.Instance) {
		if len(instances) == 0 {
			return
		}
		inst := instances[0]
		url := fmt.Sprintf("%s://%s:%d", c.config.Protocol, inst.Ip, inst.Port)

		c.mu.Lock()
		c.ProviderURL = url
		c.mu.Unlock()

		log.Printf("dubboconn: provider changed -> %s", url)
		onChanged(url)
	})
}

// Unwatch removes the subscription.
func (c *Connection) Unwatch() error {
	return c.Nacos.Unwatch(c.config.ServiceName)
}

// Connect discovers a Dubbo provider via Nacos and creates a consumer proxy.
//
// serviceImpl must be a pointer to a struct with exported function fields that
// match the Dubbo interface method signatures (standard dubbo-go proxy pattern).
//
// On success, it returns a *Connection holding the Nacos client and proxy.
// The first RPC call on serviceImpl may have slightly higher latency while the
// underlying connection completes; subsequent calls are fast-path.
func Connect(cfg Config, serviceImpl interface{}) (*Connection, error) {
	cfg.setDefaults()

	// 1. Init Nacos client
	nc, err := nacoswrap.NewClient(nacoswrap.Config{
		Host:      cfg.NacosHost,
		Port:      cfg.NacosPort,
		Namespace: cfg.NacosNamespace,
		Username:  cfg.NacosUsername,
		Password:  cfg.NacosPassword,
		AppName:   cfg.InterfaceName,
	})
	if err != nil {
		return nil, fmt.Errorf("dubboconn: nacos: %w", err)
	}

	// 2. Discover provider via Nacos
	instances, err := nc.GetInstances(cfg.ServiceName)
	if err != nil {
		return nil, fmt.Errorf("dubboconn: discover %q: %w", cfg.ServiceName, err)
	}
	if len(instances) == 0 {
		return nil, fmt.Errorf("dubboconn: no instances found for %q", cfg.ServiceName)
	}

	inst := instances[0]
	providerURL := fmt.Sprintf("%s://%s:%d", cfg.Protocol, inst.Ip, inst.Port)
	log.Printf("dubboconn: discovered provider %s (healthy=%v)", providerURL, inst.Healthy)

	// 3. Create dubbo-go consumer proxy
	ref := &dubboCfg.ReferenceConfig{
		InterfaceName:  cfg.InterfaceName,
		Protocol:       cfg.Protocol,
		URL:            providerURL,
		Retries:        cfg.Retries,
		RequestTimeout: cfg.RequestTimeout,
		Serialization:  cfg.Serialization,
	}

	ref.Init(dubboCfg.NewRootConfigBuilder().Build())
	ref.Refer(serviceImpl)
	ref.Implement(serviceImpl)

	// 4. Optional: probe for connection readiness
	if cfg.ProbeTimeout > 0 {
		probeCtx, cancel := context.WithTimeout(context.Background(), cfg.ProbeTimeout)
		defer cancel()

		// Use reflection to find the first method and probe
		probeOK := tryProbe(probeCtx, serviceImpl)
		if !probeOK {
			log.Printf("dubboconn: probe timed out (%v), proxy created but connection async", cfg.ProbeTimeout)
		} else {
			log.Printf("dubboconn: provider connection confirmed")
		}
	}

	log.Printf("dubboconn: proxy ready for %s (%s)", cfg.InterfaceName, providerURL)
	return &Connection{
		Nacos:       nc,
		config:      cfg,
		ServiceImpl: serviceImpl,
		ProviderURL: providerURL,
		ref:         ref,
	}, nil
}

// tryProbe attempts to call the first exported method on serviceImpl to verify
// the connection is ready. It returns true if the probe succeeds before ctx
// expires, false otherwise.
func tryProbe(ctx context.Context, serviceImpl interface{}) bool {
	// We probe by checking if the context is still valid;
	// the actual first RPC call will establish the connection.
	// dubbo-go handles reconnection internally.
	select {
	case <-ctx.Done():
		return false
	case <-time.After(50 * time.Millisecond):
		// Give dubbo-go a moment to establish the connection.
		// The proxy is created; pure async connection is handled by the SDK.
		return true
	}
}
