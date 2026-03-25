#!/bin/sh
set -eu

TS="$(date +%Y%m%d_%H%M%S)"
CONTAINERS="haltefest-backend haltefest-parser haltefest-ml haltefest-postgres haltefest-dragonfly haltefest-kafka haltefest-kafka-init seaweedfs-master seaweedfs-volume seaweedfs-filer"

log_line() {
	container="$1"
	message="$2"
	dir="/logs/$container"
	session_log="$dir/${TS}.log"
	current_log="$dir/current.log"

	mkdir -p "$dir"
	printf '[%s] %s\n' "$(date -Iseconds)" "$message" | tee -a "$session_log" >> "$current_log"
}

follow_container_logs() {
	container="$1"
	dir="/logs/$container"
	session_log="$dir/${TS}.log"
	current_log="$dir/current.log"

	mkdir -p "$dir"
	: > "$current_log"
	: > "$session_log"
	log_line "$container" "start runtime logs follow: container=$container session_log=$session_log"

	while true; do
		if ! docker inspect "$container" >/dev/null 2>&1; then
			log_line "$container" "container not found yet, retry in 2s"
			sleep 2
			continue
		fi

		# Runtime mode: only new lines from current moment (tail 0), like live docker logs -f.
		docker logs --timestamps --tail 0 -f "$container" 2>&1 | tee -a "$session_log" >> "$current_log" || true

		log_line "$container" "log stream ended, reconnect in 2s"
		sleep 2
	done
}

for c in $CONTAINERS; do
	follow_container_logs "$c" &
done

wait
