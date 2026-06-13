package dubboconn

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// NacosConfig holds all connection parameters for Nacos.
type NacosConfig struct {
	Host      string // Nacos server address (required)
	Port      uint64 // Nacos server port, default 8848
	Namespace string // Namespace ID, default "public"
	Username  string // (optional) Nacos auth username
	Password  string // (optional) Nacos auth password
	AppName   string // (optional) application identifier
	LogDir    string // (optional) SDK log directory; empty uses SDK default
	LogLevel  string // SDK log level: "info", "debug", "warn", "error"; default "info"
	TimeoutMs uint64 // request timeout in ms, default 5000
}

func (c *NacosConfig) defaults() {
	if c.Port == 0 {
		c.Port = 8848
	}
	if c.Namespace == "" {
		c.Namespace = "public"
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if c.TimeoutMs == 0 {
		c.TimeoutMs = 5000
	}
}

// Client is a high-level Nacos client with naming (service discovery)
// and config (configuration management) capabilities.
type Client struct {
	namingClient naming_client.INamingClient
	configClient config_client.IConfigClient
	logger       *slog.Logger
}

// NewNacos creates a Nacos Client from NacosConfig.
// Both naming and config client are initialized; if the config client fails,
// the Client is still returned with a warning but config operations will
// return ErrConfigNotInit.
func NewNacos(cfg NacosConfig) (*Client, error) {
	cfg.defaults()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	serverConfigs := []constant.ServerConfig{
		{IpAddr: cfg.Host, Port: cfg.Port, ContextPath: "/nacos"},
	}

	clientConfig := constant.ClientConfig{
		NamespaceId:         cfg.Namespace,
		TimeoutMs:           cfg.TimeoutMs,
		NotLoadCacheAtStart: true,
		LogLevel:            cfg.LogLevel,
		Username:            cfg.Username,
		Password:            cfg.Password,
		AppName:             cfg.AppName,
	}
	if cfg.LogDir != "" {
		clientConfig.LogDir = cfg.LogDir
		clientConfig.CacheDir = cfg.LogDir + "/cache"
	}

	nc, err := clients.NewNamingClient(vo.NacosClientParam{
		ClientConfig:  &clientConfig,
		ServerConfigs: serverConfigs,
	})
	if err != nil {
		return nil, fmt.Errorf("dubboconn: naming client: %w", err)
	}

	var cc config_client.IConfigClient
	cc, err = clients.NewConfigClient(vo.NacosClientParam{
		ClientConfig:  &clientConfig,
		ServerConfigs: serverConfigs,
	})
	if err != nil {
		logger.Warn("dubboconn: config client creation failed (config disabled)", "error", err)
	}

	logger.Info("dubboconn: nacos client initialized",
		"host", cfg.Host, "port", cfg.Port, "namespace", cfg.Namespace)

	return &Client{namingClient: nc, configClient: cc, logger: logger}, nil
}
