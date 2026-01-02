package main

import (
	"log"

	"github.com/cutlery47/skylr/skylr-overseer/internal/boot"
)

func main() {
	if err := boot.Run(); err != nil {
		log.Fatalf("[FATAL] %s", err)
	}
}
