package provisioner

import "context"

// ShardProvisioner provisions and deprovisions shards.
type ShardProvisioner interface {
	// Provision creates a shard and returns its address when ready.
	// A shard is considered ready when it has registered on the overseer.
	// ctx cancellation should abort and cleanup the provisioning attempt.
	Provision(ctx context.Context) (addr string, err error)
	// Deprovision stops and removes the shard at addr.
	// Overseer must Unregister the shard before calling Deprovision.
	Deprovision(ctx context.Context, addr string) error
}
