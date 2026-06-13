// Package dubboconn combines Nacos service discovery with dubbo-go consumer proxy
// creation in one package. Call Connect() to discover a Dubbo provider via Nacos
// and get a ready-to-use proxy, or use NewNacos() for raw Nacos operations.
//
// Minimal example:
//
//	type Greeter struct {
//		SayHello func(ctx context.Context, name string) (string, error)
//	}
//
//	func main() {
//		var svc Greeter
//		_, err := dubboconn.Connect(dubboconn.Config{
//			NacosHost:     "127.0.0.1",
//			NacosPort:     8848,
//			ServiceName:   "providers:org.apache.dubbo.Greeter::",
//			InterfaceName: "org.apache.dubbo.Greeter",
//		}, &svc)
//		if err != nil {
//			log.Fatal(err)
//		}
//		resp, _ := svc.SayHello(ctx, "world")
//	}
package dubboconn

import (
	"fmt"
	"log"
	"sync"
	"time"

	dubboCfg "dubbo.apache.org/dubbo-go/v3/config"
	_ "dubbo.apache.org/dubbo-go/v3/imports"

	"github.com/nacos-group/nacos-sdk-go/v2/model"
)

// Config holds all parameters to discover a Dubbo provider via Nacos
// and create a consumer proxy.
type Config struct {
	// Nacos server connection
	NacosHost      string
	NacosPort      uint64
	NacosNamespace string
	NacosUsername  string
	NacosPassword  string

	// Dubbo service identity
	ServiceName   string // Nacos service name, e.g. "providers:...::"
	InterfaceName string // Dubbo interface fully-qualified name

	// Dubbo consumer options (empty = sensible defaults)
	Protocol       string        // Default "tri"
	Retries        string        // Default "2"
	RequestTimeout string        // Default "30s"
	Serialization  string        // Default "hessian2"
	ProbeTimeout   time.Duration // 0 = skip probe

	// NacosAppName is passed to Nacos as the application identifier.
	NacosAppName string
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
	if c.NacosAppName == "" {
		c.NacosAppName = c.InterfaceName
	}
}

// Connection holds the established Nacos client and Dubbo proxy reference.
type Connection struct {
	Nacos       *Client
	ServiceImpl interface{}
	ProviderURL string
	config      Config

	ref *dubboCfg.ReferenceConfig
	mu  sync.RWMutex
}

// CurrentURL returns the currently known provider URL.
func (c *Connection) CurrentURL() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ProviderURL
}

// Watch subscribes to provider instance changes.
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

// Unwatch removes the Nacos subscription.
func (c *Connection) Unwatch() error {
	return c.Nacos.Unwatch(c.config.ServiceName)
}

// Connect discovers a Dubbo provider via Nacos and creates a consumer proxy.
//
// serviceImpl must be a pointer to a struct with exported function fields
// matching the Dubbo interface.
func Connect(cfg Config, serviceImpl interface{}) (*Connection, error) {
	cfg.setDefaults()

	// 1. Init Nacos client
	nc, err := NewNacos(NacosConfig{
		Host:      cfg.NacosHost,
		Port:      cfg.NacosPort,
		Namespace: cfg.NacosNamespace,
		Username:  cfg.NacosUsername,
		Password:  cfg.NacosPassword,
		AppName:   cfg.NacosAppName,
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
		return nil, fmt.Errorf("dubboconn: no instances for %q", cfg.ServiceName)
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

	log.Printf("dubboconn: proxy ready for %s (%s)", cfg.InterfaceName, providerURL)
	return &Connection{
		Nacos:       nc,
		config:      cfg,
		ServiceImpl: serviceImpl,
		ProviderURL: providerURL,
		ref:         ref,
	}, nil
}
