package handler

import (
	"context"
	"time"
)

type SnapshotCache interface {
	Get(ctx context.Context, key string) (value string, found bool, err error)
	Set(ctx context.Context, key string, value string, ttl time.Duration) error
}
