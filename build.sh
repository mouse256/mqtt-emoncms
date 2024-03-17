#!/bin/bash

./gradlew clean assemble
docker buildx build --push --platform linux/arm64,linux/amd64 --tag ghcr.io/mouse256/mqtt-emoncms:0.0.2-SNAPSHOT -f src/main/docker/Dockerfile.jvm .

