#!/bin/bash

if [ ! -f /var/lib/grafana/.initialized ]; then
  echo "Initializing Grafana..."
  touch /var/lib/grafana/.initialized
fi

echo "Starting main service..."
exec "$@"
