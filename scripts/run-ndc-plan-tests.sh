#!/bin/bash

# SQLConnector Test Runner
# Tests HTTP files against a server and compares responses with expected JSON

# Configuration
SERVER_URL=${1:-"http://localhost:8081"}
TEST_DIR=${2:-"./ndc-tests/snowflake/plan"}
UPDATE_EXPECTED=${UPDATE_EXPECTED:-false}

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Check dependencies
for cmd in curl jq diff; do
  if ! command -v $cmd &>/dev/null; then
    echo -e "${RED}Error: $cmd is required but not installed.${NC}"
    exit 1
  fi
done

# Create test directory if it doesn't exist
mkdir -p "$TEST_DIR"

# Find all HTTP test files or directories
if [ -d "$TEST_DIR" ]; then
  # First check if there are .http files directly in the test directory
  HTTP_FILES=$(find "$TEST_DIR" -maxdepth 1 -name "*.http")

  # Then find all test directories that contain .http files
  TEST_DIRS=$(find "$TEST_DIR" -mindepth 1 -type d)

  # Combine direct .http files and those in subdirectories
  ALL_HTTP_FILES="$HTTP_FILES"
  for dir in $TEST_DIRS; do
    test_http_file=$(find "$dir" -name "*.http" | head -n 1)
    if [ -n "$test_http_file" ]; then
      ALL_HTTP_FILES="$ALL_HTTP_FILES $test_http_file"
    fi
  done

  COUNT=$(echo "$ALL_HTTP_FILES" | wc -w)
else
  echo "Test directory not found: $TEST_DIR"
  exit 1
fi

echo -e "${CYAN}=== SQLConnector Test Runner ===${NC}"
echo -e "Server URL: $SERVER_URL"
echo -e "Test directory: $TEST_DIR"
echo -e "Found $COUNT tests to run\n"

# Track test results
PASSED=0
FAILED=0

# Process each test
for http_file in $ALL_HTTP_FILES; do
  # If the http file is in a subdirectory, use the directory name as the test name
  if [ "$(dirname "$http_file")" != "$TEST_DIR" ]; then
    test_name=$(basename "$(dirname "$http_file")")
    test_dir=$(dirname "$http_file")
  else
    test_name=$(basename "$http_file" .http)
    test_dir="$TEST_DIR"
  fi

  # Look for expected.json in the same directory as the .http file
  expected_file="$test_dir/expected.json"
  actual_file="$test_dir/actual.json"

  # If expected.json doesn't exist, fall back to <name>.expected.json
  if [ ! -f "$expected_file" ]; then
    expected_file="${http_file%.http}.expected.json"
  fi

  echo -e "${CYAN}Running test: $test_name${NC}"

  # Create empty expected file if it doesn't exist
  if [ ! -f "$expected_file" ]; then
    echo -e "${YELLOW}Warning: Expected file not found, creating empty one${NC}"
    echo "[]" >"$expected_file"
  fi

  # Parse HTTP file to extract method, headers and body
  method=$(head -1 "$http_file" | cut -d' ' -f1)
  url="$SERVER_URL$(head -1 "$http_file" | cut -d' ' -f2)"

  # Extract headers
  headers=""
  in_headers=true
  while IFS= read -r line; do
    # Skip first line (request line)
    if [[ "$line" == *"HTTP/1.1"* ]]; then
      continue
    fi

    # Empty line means end of headers
    if [ -z "$line" ]; then
      in_headers=false
      continue
    fi

    # Add header if we're still in the headers section
    if [ "$in_headers" = true ] && [ ! -z "$line" ]; then
      headers="$headers -H \"$line\""
    fi
  done <"$http_file"

  # Extract body (everything after the empty line)
  body=$(awk 'BEGIN{body=0} /^$/{body=1; next} body==1{print}' "$http_file" | tr '\n' ' ')

  # Build curl command
  cmd="curl -s -X $method $headers -d '$body' '$url'"

  # Execute request and save response
  echo -e "  - Sending request to $url"
  eval "$cmd" >"$actual_file"

  # Check if response is valid JSON
  if ! jq empty "$actual_file" 2>/dev/null; then
    echo -e "${RED}  ❌ Response is not valid JSON${NC}"
    echo -e "  Response content:"
    cat "$actual_file"
    FAILED=$((FAILED + 1))
    continue
  fi

  # Format both JSON files for proper comparison
  jq '.' "$expected_file" >"${expected_file}.formatted"
  jq '.' "$actual_file" >"${actual_file}.formatted"

  # Compare response with expected
  if diff -q "${expected_file}.formatted" "${actual_file}.formatted" >/dev/null; then
    echo -e "${GREEN}  ✅ Test passed${NC}"
    PASSED=$((PASSED + 1))
  else
    echo -e "${RED}  ❌ Test failed${NC}"
    echo -e "${YELLOW}  Differences between expected and actual:${NC}"
    diff --color=always "${expected_file}.formatted" "${actual_file}.formatted"

    # Option to update expected file
    if [ "$UPDATE_EXPECTED" = true ]; then
      cp "$actual_file" "$expected_file"
      echo -e "${YELLOW}  📝 Updated expected file with actual response${NC}"
    fi

    FAILED=$((FAILED + 1))
  fi

  # Show formatted outputs (debugging)
  # echo -e "  Expected:"
  # cat "${expected_file}.formatted"
  # echo -e "  Actual:"
  # cat "${actual_file}.formatted"

  # Clean up temporary files
  rm "${expected_file}.formatted" "${actual_file}.formatted"

  echo ""
done

# Print summary
echo -e "${CYAN}=== Test Summary ===${NC}"
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${RED}Failed: $FAILED${NC}"
echo -e "Total: $((PASSED + FAILED))"

if [ $FAILED -eq 0 ]; then
  echo -e "\n${GREEN}All tests passed! 🎉${NC}"
  exit 0
else
  echo -e "\n${RED}Some tests failed. 😟${NC}"
  exit 1
fi
