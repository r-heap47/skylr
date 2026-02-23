package v1

import (
	"log"

	pbshard "github.com/r-heap47/skylr/skylr-shard/internal/pb/skylr-shard"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Scan streams all non-expired entries to the caller.
func (i *Implementation) Scan(_ *emptypb.Empty, stream pbshard.Shard_ScanServer) error {
	ctx := stream.Context()

	for scanEntry, err := range i.shard.Scan(ctx) {
		if err != nil {
			return status.Errorf(codes.Internal, "shard.Scan: %s", err)
		}
		if scanEntry == nil {
			log.Println("[WARN] Implementation.Scan: received nil enry on scan")
			continue
		}

		if err := stream.Send(&pbshard.ScanResponse{
			Entry:        scanEntry.Entry,
			RemainingTtl: durationpb.New(scanEntry.RemainingTTL),
		}); err != nil {
			return err
		}
	}

	return nil
}
