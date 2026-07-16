#!/bin/bash
set -e

# Check prerequisites
./check-deps.sh release

export NODE_ENV=production

NAME=${PWD##*/}
BRANCH=${1:-$(git branch --show-current)}
BRANCH=${BRANCH//\//-}
TIMESTAMP=$(date '+%Y-%m-%d_%H-%M-%S')

CONFIGS_DIR="packages/python/port/configs"

# If VITE_PLATFORM is already set, release only that platform
if [ -n "$VITE_PLATFORM" ]; then
    config_file="$CONFIGS_DIR/${VITE_PLATFORM}_config.json"
    if [ ! -f "$config_file" ]; then
        echo "ERROR: No config found for platform '$VITE_PLATFORM' at $config_file."
        echo "Generate it first with:  pnpm generate-config $VITE_PLATFORM"
        exit 1
    fi
    platforms=("$VITE_PLATFORM")
else
    # Discover platforms from configs/<platform>_config.json files
    platforms=()
    for config_file in "$CONFIGS_DIR"/*_config.json; do
        [ -f "$config_file" ] || continue
        basename="${config_file##*/}"          # e.g. chatgpt_config.json
        platform="${basename%_config.json}"    # e.g. chatgpt
        platforms+=("$platform")
    done

    if [ ${#platforms[@]} -eq 0 ]; then
        echo "ERROR: No platform configs found in $CONFIGS_DIR."
        echo "Generate one first with:  pnpm generate-config <platform>"
        exit 1
    fi
fi

echo "Found ${#platforms[@]} platform(s): ${platforms[*]}"
mkdir -p releases

for PLATFORM in "${platforms[@]}"; do
    echo "Building for platform: ${PLATFORM}..."
    export VITE_PLATFORM=$PLATFORM
    pnpm run build

    RELEASE_NAME="${NAME}_${PLATFORM}_${BRANCH}_${TIMESTAMP}.zip"
    cd packages/data-collector/dist
    zip -r ../../../releases/${RELEASE_NAME} .
    cd ../../..
    echo "Created: releases/${RELEASE_NAME}"
done

echo ""
echo "Done. ${#platforms[@]} platform release(s) created in releases/"
