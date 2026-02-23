package logwriter

import (
	"bufio"
	"io"
	"log"
)

// LinePrefix returns an io.Writer that reads line-by-line from the writer and logs
// each line with the given prefix via log.Printf. Use for subprocess stdout/stderr
// to distinguish their output from the parent's logs.
func LinePrefix(prefix string) io.Writer {
	pr, pw := io.Pipe()

	go func() {
		defer func() { _ = pr.Close() }()

		s := bufio.NewScanner(pr)
		for s.Scan() {
			log.Printf("[%s] %s", prefix, s.Text())
		}
	}()

	return pw
}
