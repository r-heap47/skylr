package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	client "github.com/r-heap47/skylr/skylr-client"
	pbshard "github.com/r-heap47/skylr/skylr-client/internal/pb/skylr-shard"
)

func runGet(ctx context.Context, c *client.Client, args []string, verbose bool) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: skylr-client get <key>")
	}

	entry, err := c.Get(ctx, args[0])
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			return fmt.Errorf("not found: %s", args[0])
		}
		return err
	}

	if verbose && entry != nil {
		fmt.Fprintf(os.Stderr, "[verbose] Get response: key=%q value=%s\n", args[0], formatEntryValue(entry))
	}

	val := formatEntryValue(entry)
	if val != "" {
		fmt.Println(val)
	}

	return nil
}

func runSet(ctx context.Context, c *client.Client, args []string, ttl time.Duration, verbose bool) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: skylr-client set <key> <value>")
	}

	if err := c.Set(ctx, args[0], args[1], ttl); err != nil {
		return err
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "[verbose] Set response: key=%q value=%q ttl=%s ok\n", args[0], args[1], ttl)
	}

	fmt.Println("ok")
	return nil
}

func runDelete(ctx context.Context, c *client.Client, args []string, verbose bool) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: skylr-client delete <key>")
	}

	deleted, err := c.Delete(ctx, args[0])
	if err != nil {
		return err
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "[verbose] Delete response: key=%q deleted=%v\n", args[0], deleted)
	}

	if deleted {
		fmt.Println("deleted")
	} else {
		fmt.Println("not found")
	}

	return nil
}

func formatEntryValue(entry *pbshard.Entry) string {
	if entry == nil || entry.Value == nil {
		return ""
	}

	switch v := entry.Value.(type) {
	case *pbshard.Entry_ValueStr:
		return v.ValueStr
	case *pbshard.Entry_ValueInt32:
		return fmt.Sprintf("%d", v.ValueInt32)
	case *pbshard.Entry_ValueInt64:
		return fmt.Sprintf("%d", v.ValueInt64)
	case *pbshard.Entry_ValueFloat:
		return fmt.Sprintf("%g", v.ValueFloat)
	case *pbshard.Entry_ValueDouble:
		return fmt.Sprintf("%g", v.ValueDouble)
	default:
		return ""
	}
}
