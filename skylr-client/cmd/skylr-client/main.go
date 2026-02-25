package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	client "github.com/r-heap47/skylr/skylr-client"
)

const usage = `usage: skylr-client [-overseer=addr] [-ttl=duration] [-v] get|set|delete [key] [value]
  get <key>       - get value by key
  set <key> <val> - set key-value with TTL
  delete <key>    - delete key`

func main() {
	overseer := flag.String("overseer", os.Getenv("SKYLR_OVERSEER"), "Overseer gRPC address (or SKYLR_OVERSEER env)")
	ttl := flag.Duration("ttl", 24*time.Hour, "TTL for set (default 24h)")
	verbose := flag.Bool("v", false, "log client responses to stderr (verbose)")
	flag.Parse()

	if *overseer == "" {
		fmt.Fprintln(os.Stderr, "error: -overseer or SKYLR_OVERSEER required")
		os.Exit(1)
	}

	args := flag.Args()
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(1)
	}

	cmd, args := args[0], args[1:]
	ctx := context.Background()

	c, err := client.New(ctx, *overseer)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = c.Close() }()

	if err := runCommand(ctx, c, cmd, args, *ttl, *verbose); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func runCommand(ctx context.Context, c *client.Client, cmd string, args []string, ttl time.Duration, verbose bool) error {
	switch cmd {
	case "get":
		return runGet(ctx, c, args, verbose)
	case "set":
		return runSet(ctx, c, args, ttl, verbose)
	case "delete":
		return runDelete(ctx, c, args, verbose)
	default:
		return fmt.Errorf("unknown command: %s", cmd)
	}
}
