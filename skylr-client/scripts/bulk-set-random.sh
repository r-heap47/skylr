#!/usr/bin/env bash
set -e

count="${1:-100}"

for ((i = 0; i < count; i++)); do
	key="key-$(cat /proc/sys/kernel/random/uuid)"
	./bin/skylr-client set "$key" "value-${i}"
done

echo "Set $count keys with random names"
