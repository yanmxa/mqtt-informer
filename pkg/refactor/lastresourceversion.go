package refactor

import "sync"

// lastResourceVersion instance for a cluster identifier
type lastResourceVersion struct {
	// version is the resource version token last observed when doing a sync with the underlying store
	// it is thread safe, but not synchronized with the underlying store
	version string
	// isUnavailable is true if the previous list or watch request with version failed with an "expired" or "too large resource version" error.
	isUnavailable bool
	// 	mutex guards read/write access to the version
	mutex sync.RWMutex
}
