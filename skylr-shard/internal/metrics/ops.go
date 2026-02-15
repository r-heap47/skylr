package metrics

import "sync/atomic"

var (
	getOps    atomic.Int64
	setOps    atomic.Int64
	deleteOps atomic.Int64
)

// IncGetOps increments the get operations counter
func IncGetOps() { getOps.Add(1) }

// IncSetOps increments the set operations counter
func IncSetOps() { setOps.Add(1) }

// IncDeleteOps increments the delete operations counter
func IncDeleteOps() { deleteOps.Add(1) }

// TotalGets returns the total number of get operations
func TotalGets() uint64 { return uint64(getOps.Load()) } // nolint: gosec

// TotalSets returns the total number of set operations
func TotalSets() uint64 { return uint64(setOps.Load()) } // nolint: gosec

// TotalDeletes returns the total number of delete operations
func TotalDeletes() uint64 { return uint64(deleteOps.Load()) } // nolint: gosec
