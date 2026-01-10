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

	v1 "github.com/cutlery47/skylr/skylr-overseer/internal/api/grpc/v1"
	"github.com/cutlery47/skylr/skylr-overseer/internal/overseer"
	pbovr "github.com/cutlery47/skylr/skylr-overseer/internal/pb/skylr-overseer"
	"google.golang.org/grpc"
)

var (
	port = flag.String("port", "9000", "Port for grpc-server to be run on (default: 9000)")
)

// Run .
func Run() error {
	var (
		grpcHost     = "localhost"
		grpcEndpoint string
	)

	flag.Parse()

	grpcEndpoint = fmt.Sprintf("%s:%s", grpcHost, *port)

	ovr := overseer.New(overseer.Config{
		CheckForShardFailuresDelay: func(_ context.Context) time.Duration { return time.Second }, // TODO: proper config
	})

	impl := v1.New(&v1.Config{
		Ovr: ovr,
	})

	// === GRPC SERVER SETUP ===

	grpcServer := grpc.NewServer()

	// TODO: properly configure network interface -- rm nolint
	lis, err := net.Listen("tcp", grpcEndpoint) // nolint: gosec
	if err != nil {
		return fmt.Errorf("net.Listen: %w", err)
	}

	go func() {
		pbovr.RegisterOverseerServer(grpcServer, impl)

		log.Printf("[GRPC] grpc server is set up on %s\n", grpcEndpoint)

		err := grpcServer.Serve(lis)
		if err != nil {
			log.Fatalf("grpcServer.Serve: %s", err)
		}
	}()

	// === GRACEFUL SHUTDOWN ===

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan

	log.Println("[GRPC] shutting down grpc server...")
	grpcServer.GracefulStop()

	return nil
}
