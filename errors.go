package dubboconn

import "errors"

// Nacos client errors.
var (
	ErrNamingNotInit = errors.New("dubboconn: naming client not initialized")
	ErrConfigNotInit = errors.New("dubboconn: config client not initialized")
)

// Dubbo connector errors.
var (
	ErrNoInstances  = errors.New("dubboconn: no available instances")
	ErrNotConnected = errors.New("dubboconn: not connected")
)
