package dubboconn

import "errors"

var (
	// ErrNoInstances is returned when the Nacos service has no available instances.
	ErrNoInstances = errors.New("dubboconn: no available instances")

	// ErrNotConnected is returned when an operation is attempted on a
	// Connection that has not been successfully established.
	ErrNotConnected = errors.New("dubboconn: not connected")
)
