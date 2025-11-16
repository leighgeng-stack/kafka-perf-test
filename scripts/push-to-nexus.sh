#!/bin/bash
# Script to build and push kafka-perf-test images to Nexus registry
#
# Usage:
#   export NEXUS_REGISTRY="registry.example.com"
#   export NEXUS_USER="your-username"
#   export NEXUS_PASSWORD="your-password"
#   export VERSION_TAG="v1.0.0"  # optional, defaults to "latest"
#   ./scripts/push-to-nexus.sh

set -e

# Default values
NEXUS_REGISTRY="${NEXUS_REGISTRY:-registry.example.com}"
NEXUS_USER="${NEXUS_USER:-}"
NEXUS_PASSWORD="${NEXUS_PASSWORD:-}"
VERSION_TAG="${VERSION_TAG:-latest}"

# Image paths
CLI_IMAGE="${NEXUS_REGISTRY}/kafka-perf/kafka-perf-runner"
SPRING_IMAGE="${NEXUS_REGISTRY}/kafka-perf/kafka-perf-spring"

# Validate required variables
if [ -z "$NEXUS_USER" ] || [ -z "$NEXUS_PASSWORD" ]; then
  echo "Error: NEXUS_USER and NEXUS_PASSWORD must be set"
  echo "Usage:"
  echo "  export NEXUS_REGISTRY=\"registry.example.com\""
  echo "  export NEXUS_USER=\"your-username\""
  echo "  export NEXUS_PASSWORD=\"your-password\""
  echo "  export VERSION_TAG=\"v1.0.0\"  # optional"
  echo "  ./scripts/push-to-nexus.sh"
  exit 1
fi

echo "Logging in to Nexus registry: $NEXUS_REGISTRY"
echo "$NEXUS_PASSWORD" | docker login -u "$NEXUS_USER" "$NEXUS_REGISTRY" --password-stdin

echo ""
echo "Building CLI runner image..."
docker build -f runners/cli/docker/Dockerfile . \
  -t "$CLI_IMAGE:$VERSION_TAG" \
  -t "$CLI_IMAGE:latest"

echo ""
echo "Building Spring runner image..."
docker build -f runners/spring/docker/Dockerfile . \
  -t "$SPRING_IMAGE:$VERSION_TAG" \
  -t "$SPRING_IMAGE:latest"

echo ""
echo "Pushing CLI runner images..."
docker push "$CLI_IMAGE:$VERSION_TAG"
docker push "$CLI_IMAGE:latest"

echo ""
echo "Pushing Spring runner images..."
docker push "$SPRING_IMAGE:$VERSION_TAG"
docker push "$SPRING_IMAGE:latest"

echo ""
echo "Successfully pushed images:"
echo "  - $CLI_IMAGE:$VERSION_TAG"
echo "  - $CLI_IMAGE:latest"
echo "  - $SPRING_IMAGE:$VERSION_TAG"
echo "  - $SPRING_IMAGE:latest"

