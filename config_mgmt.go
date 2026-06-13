package dubboconn

import (
	"fmt"

	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// ConfigOption configures ConfigParam for config operations.
type ConfigOption func(*vo.ConfigParam)

// WithGroup sets the config group name. Default "DEFAULT_GROUP".
func WithGroup(group string) ConfigOption {
	return func(p *vo.ConfigParam) { p.Group = group }
}

// ---- Configuration Management ----

// GetConfig retrieves a config value from Nacos by dataId.
func (c *Client) GetConfig(dataId string, opts ...ConfigOption) (string, error) {
	if c.configClient == nil {
		return "", ErrConfigNotInit
	}
	param := vo.ConfigParam{DataId: dataId, Group: "DEFAULT_GROUP"}
	for _, o := range opts {
		o(&param)
	}
	content, err := c.configClient.GetConfig(param)
	if err != nil {
		return "", fmt.Errorf("dubboconn: get config %q: %w", dataId, err)
	}
	return content, nil
}

// PublishConfig creates or updates a config.
func (c *Client) PublishConfig(dataId, content string, opts ...ConfigOption) (bool, error) {
	if c.configClient == nil {
		return false, ErrConfigNotInit
	}
	param := vo.ConfigParam{DataId: dataId, Content: content, Group: "DEFAULT_GROUP"}
	for _, o := range opts {
		o(&param)
	}
	published, err := c.configClient.PublishConfig(param)
	if err != nil {
		return false, fmt.Errorf("dubboconn: publish config %q: %w", dataId, err)
	}
	return published, nil
}

// DeleteConfig removes a config from Nacos.
func (c *Client) DeleteConfig(dataId string, opts ...ConfigOption) (bool, error) {
	if c.configClient == nil {
		return false, ErrConfigNotInit
	}
	param := vo.ConfigParam{DataId: dataId, Group: "DEFAULT_GROUP"}
	for _, o := range opts {
		o(&param)
	}
	deleted, err := c.configClient.DeleteConfig(param)
	if err != nil {
		return false, fmt.Errorf("dubboconn: delete config %q: %w", dataId, err)
	}
	return deleted, nil
}

// ListenConfig subscribes to config changes.
func (c *Client) ListenConfig(dataId string, onChange func(string), opts ...ConfigOption) error {
	if c.configClient == nil {
		return ErrConfigNotInit
	}
	param := vo.ConfigParam{
		DataId: dataId,
		Group:  "DEFAULT_GROUP",
		OnChange: func(namespace, group, dataId, data string) {
			c.logger.Info("dubboconn: config changed", "dataId", dataId, "group", group)
			onChange(data)
		},
	}
	for _, o := range opts {
		o(&param)
	}
	err := c.configClient.ListenConfig(param)
	if err != nil {
		return fmt.Errorf("dubboconn: listen config %q: %w", dataId, err)
	}
	return nil
}

// CancelListenConfig stops listening for config changes.
func (c *Client) CancelListenConfig(dataId string, opts ...ConfigOption) error {
	if c.configClient == nil {
		return ErrConfigNotInit
	}
	param := vo.ConfigParam{DataId: dataId, Group: "DEFAULT_GROUP"}
	for _, o := range opts {
		o(&param)
	}
	err := c.configClient.CancelListenConfig(param)
	if err != nil {
		return fmt.Errorf("dubboconn: cancel listen config %q: %w", dataId, err)
	}
	return nil
}
