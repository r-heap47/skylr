package boot

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	v1 "github.com/r-heap47/skylr/skylr-overseer/internal/api/grpc/v1"
	"github.com/r-heap47/skylr/skylr-overseer/internal/overseer"
	pbovr "github.com/r-heap47/skylr/skylr-overseer/internal/pb/skylr-overseer"
	"github.com/r-heap47/skylr/skylr-overseer/internal/pkg/utils"
	"google.golang.org/grpc"
)

var (
	port = flag.String("port", "9000", "Port for grpc-server to be run on (default: 9000)")
	host = flag.String("host", "0.0.0.0", "Host interface to listen on (default: 0.0.0.0)")
)

// Run .
func Run() error {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcEndpoint := fmt.Sprintf("%s:%s", *host, *port)

	ovr := overseer.New(ctx, overseer.Config{
		CheckForShardFailuresDelay: utils.Const(time.Second),     // TODO: proper config
		ObserverDelay:              utils.Const(time.Second),     // TODO: proper config
		ObserverMetricsTimeout:     utils.Const(5 * time.Second), // TODO: proper config
		ObserverErrorThreshold:     utils.Const(3),               // TODO: proper config
	})

	impl := v1.New(&v1.Config{
		Ovr: ovr,
	})

	// === GRPC SERVER SETUP ===

	grpcServer := grpc.NewServer()
	pbovr.RegisterOverseerServer(grpcServer, impl)

	lis, err := net.Listen("tcp", grpcEndpoint)
	if err != nil {
		return fmt.Errorf("net.Listen: %w", err)
	}

	go func() {
		log.Printf("[GRPC] grpc server is set up on %s\n", grpcEndpoint)

		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("grpcServer.Serve: %s", err)
		}
	}()

	// === GRACEFUL SHUTDOWN ===

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan

	log.Println("[GRPC] shutting down grpc server...")
	grpcServer.GracefulStop()

	// cancel root context — stops checkForShardFailures and all observer goroutines
	cancel()

	return nil
}
