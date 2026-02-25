#!/usr/bin/env bash
set -e

count="${1:-100}"
prefix="${2:-key}"

for ((i = 0; i < count; i++)); do
	./bin/skylr-client set "${prefix}:${i}" "value-${i}"
done

echo "Set $count keys (${prefix}:0 .. ${prefix}:$((count - 1)))"
