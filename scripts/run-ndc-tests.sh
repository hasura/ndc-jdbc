#!/bin/bash

set -eo pipefail

# Default port mapping
declare -A CONNECTOR_PORTS=(
    ["snowflake"]="8081"
)

declare -A CONNECTOR_TEST_DIRS=(
    ["snowflake"]="ndc-tests/snowflake"
)

# Function to display usage
usage() {
    echo "Usage: $0 <connector> [options]"
    echo ""
    echo "Available connectors:"
    for connector in "${!CONNECTOR_PORTS[@]}"; do
        echo "  - $connector"
    done
    echo ""
    echo "Options:"
    echo "  -p, --port        Override default port number"
    echo "  -h, --help        Show this help message"
    echo ""
    echo "Required environment variables:"
    echo "  JDBC_URL          JDBC connection URL"
    echo "  JOOQ_PRO_EMAIL    JOOQ Pro Email (if required)"
    echo "  JOOQ_PRO_LICENSE  JOOQ Pro License (if required)"
}

# Check if connector is provided
if [ $# -eq 0 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    usage
    exit 0
fi

CONNECTOR="$1"
shift

# Validate connector
if [ -z "${CONNECTOR_PORTS[$CONNECTOR]}" ]; then
    echo "Error: Invalid connector '$CONNECTOR'"
    echo "Available connectors:"
    for connector in "${!CONNECTOR_PORTS[@]}"; do
        echo "  - $connector"
    done
    exit 1
fi

# Set default values based on connector
PORT="${CONNECTOR_PORTS[$CONNECTOR]}"
TEST_DIR="${CONNECTOR_TEST_DIRS[$CONNECTOR]}"
NDC_VERSION="v0.1.6"

# Parse remaining command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -p|--port)
            PORT="$2"
            shift
            shift
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Check required environment variables
if [ -z "$JDBC_URL" ]; then
    echo "Error: JDBC_URL environment variable is not set"
    exit 1
fi

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check Java version
if ! command_exists java; then
    echo "Error: Java is not installed"
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
if [ "$JAVA_VERSION" -lt 21 ]; then
    echo "Error: Java 21 or higher is required (found version $JAVA_VERSION)"
    exit 1
fi

# Download and setup NDC Test if not present
if ! command_exists ndc-test; then
    echo "Installing ndc-test..."
    ARCH=$(uname -m)
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')

    if [ "$OS" = "darwin" ]; then
        NDC_BINARY="ndc-test-$ARCH-apple-darwin"
    else
        NDC_BINARY="ndc-test-$ARCH-unknown-linux-gnu"
    fi

    curl -L --fail -o ndc-test "https://github.com/hasura/ndc-spec/releases/download/$NDC_VERSION/$NDC_BINARY"
    chmod +x ndc-test
    sudo mv ndc-test /usr/local/bin/
fi

# Create configs directory
mkdir -p "configs/$CONNECTOR"

# Generate configuration
echo "Generating configuration for $CONNECTOR..."
make "run-$CONNECTOR-introspection" > config-generation.log 2>&1 || {
    echo "Configuration generation failed. Last 50 lines of log:"
    tail -n 50 config-generation.log
    exit 1
}

# Run the connector
echo "Starting the $CONNECTOR connector..."
make "run-$CONNECTOR" > connector.log 2>&1 &
SERVER_PID=$!

# Clean up on script exit
cleanup() {
    echo "Cleaning up..."
    if [ ! -z "$SERVER_PID" ]; then
        kill $SERVER_PID 2>/dev/null || true
        sleep 2
        kill -9 $SERVER_PID 2>/dev/null || true
    fi
}
trap cleanup EXIT

# Wait for connector to be ready
echo "Waiting for the connector to be ready on port $PORT..."
timeout 30 bash -c "while ! nc -z localhost $PORT; do sleep 1; done" || {
    echo "Connector failed to start. Last 50 lines of log:"
    tail -n 50 connector.log
    exit 1
}

# Run tests
echo "Running tests for $CONNECTOR..."
curl -L --fail -o ndc-test-temp https://github.com/hasura/ndc-spec/releases/download/v0.1.6/ndc-test-x86_64-unknown-linux-gnu
chmod +x ndc-test-temp
#sudo mv ndc-test /usr/local/bin/

docker run --rm --network="host" -v $(pwd):/app -w /app ubuntu:22.04 bash -c "\
  apt-get update && \
  apt-get install -y curl && \
  curl -L --fail -o ndc-test-temp https://github.com/hasura/ndc-spec/releases/download/v0.1.6/ndc-test-x86_64-unknown-linux-gnu && \
  chmod +x ndc-test-temp && \
  ./ndc-test-temp replay --endpoint http://localhost:8081 --snapshots-dir ndc-tests/snowflake"

# Check test results
if [ ${PIPESTATUS[0]} -eq 1 ]; then
    echo "Tests failed for $CONNECTOR!"
    exit 1
else
    echo "Tests passed successfully for $CONNECTOR!"
fi
