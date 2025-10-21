#!/bin/bash
# Helm Chart Unit Tests for dremio-mcp
# Tests template rendering with different configurations

# Don't exit on error - we want to run all tests
set +e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# Test output directory
TEST_OUTPUT_DIR="/tmp/helm-test-output"
mkdir -p "$TEST_OUTPUT_DIR"

# Helper functions
print_test() {
    echo -e "${YELLOW}TEST: $1${NC}"
}

print_pass() {
    echo -e "${GREEN}✓ PASS: $1${NC}"
    ((TESTS_PASSED++))
}

print_fail() {
    echo -e "${RED}✗ FAIL: $1${NC}"
    ((TESTS_FAILED++))
}

run_test() {
    ((TESTS_RUN++))
}

# Test function: renders template and saves to file
render_template() {
    local test_name=$1
    shift
    local output_file="$TEST_OUTPUT_DIR/${test_name}.yaml"
    
    helm template test-release ./helm/dremio-mcp "$@" > "$output_file" 2>&1
    echo "$output_file"
}

# Assertion helpers
assert_contains() {
    local file=$1
    local pattern=$2
    local description=$3

    run_test
    if grep -qF -- "$pattern" "$file"; then
        print_pass "$description"
        return 0
    else
        print_fail "$description - Pattern not found: $pattern"
        return 1
    fi
}

assert_not_contains() {
    local file=$1
    local pattern=$2
    local description=$3

    run_test
    if ! grep -qF -- "$pattern" "$file"; then
        print_pass "$description"
        return 0
    else
        print_fail "$description - Pattern should not exist: $pattern"
        return 1
    fi
}

assert_count() {
    local file=$1
    local pattern=$2
    local expected_count=$3
    local description=$4

    run_test
    local actual_count=$(grep -cF -- "$pattern" "$file" || true)

    if [ "$actual_count" -eq "$expected_count" ]; then
        print_pass "$description (count: $actual_count)"
        return 0
    else
        print_fail "$description - Expected: $expected_count, Got: $actual_count"
        return 1
    fi
}

echo "========================================="
echo "Dremio MCP Helm Chart Unit Tests"
echo "========================================="
echo ""

# Test 1: Lint the chart
print_test "1. Helm Lint"
run_test
if helm lint ./helm/dremio-mcp > "$TEST_OUTPUT_DIR/lint.log" 2>&1; then
    print_pass "Chart passes helm lint"
else
    print_fail "Chart fails helm lint"
    cat "$TEST_OUTPUT_DIR/lint.log"
fi
echo ""

# Test 2: Default values (OAuth mode - no PAT)
print_test "2. OAuth Mode (No PAT) - Default Configuration"
OUTPUT=$(render_template "oauth-mode" \
    --set dremio.uri=https://dremio.example.com:9047)

assert_contains "$OUTPUT" "uri: \"https://dremio.example.com:9047\"" "ConfigMap contains Dremio URI"
assert_not_contains "$OUTPUT" "kind: Secret" "No Secret created in OAuth mode"
assert_not_contains "$OUTPUT" "pat:" "No PAT reference in ConfigMap"
assert_contains "$OUTPUT" "server_mode: \"FOR_DATA_PATTERNS\"" "ConfigMap contains default server mode"
assert_contains "$OUTPUT" "name: config" "Config volume defined"
assert_not_contains "$OUTPUT" "name: secrets" "No secrets volume in OAuth mode"
assert_contains "$OUTPUT" "/app/config" "Config mount path present"
assert_contains "$OUTPUT" "--cfg" "Config argument present in command"
assert_contains "$OUTPUT" "/app/config/config.yaml" "Config file path in command"
echo ""

# Test 3: Inline PAT mode
print_test "3. Inline PAT Mode - Development Configuration"
OUTPUT=$(render_template "inline-pat-mode" \
    --set dremio.uri=https://dremio.example.com:9047 \
    --set dremio.pat=test-pat-token)

assert_contains "$OUTPUT" "kind: Secret" "Secret created with inline PAT"
assert_contains "$OUTPUT" "pat: \"test-pat-token\"" "Secret contains PAT value"
assert_contains "$OUTPUT" "pat: \"@/app/secrets/pat\"" "ConfigMap references PAT file"
assert_contains "$OUTPUT" "name: secrets" "Secrets volume defined"
assert_contains "$OUTPUT" "/app/secrets" "Secrets mount path present"
assert_contains "$OUTPUT" "test-release-dremio-mcp-secret" "Auto-generated secret name used"
echo ""

# Test 4: Existing secret mode
print_test "4. Existing Secret Mode"
OUTPUT=$(render_template "existing-secret-mode" \
    --set dremio.uri=https://dremio.example.com:9047 \
    --set dremio.existingSecret=my-custom-secret)

assert_not_contains "$OUTPUT" "kind: Secret" "No Secret created when using existing secret"
assert_contains "$OUTPUT" "pat: \"@/app/secrets/pat\"" "ConfigMap references PAT file"
assert_contains "$OUTPUT" "name: secrets" "Secrets volume defined"
assert_contains "$OUTPUT" "secretName: my-custom-secret" "Uses provided secret name"
assert_not_contains "$OUTPUT" "test-release-dremio-mcp-secret" "Does not use auto-generated secret"
echo ""

# Test 5: Metrics enabled
print_test "5. Metrics Configuration"
OUTPUT=$(render_template "metrics-enabled" \
    --set dremio.uri=https://dremio.example.com:9047 \
    --set metrics.enabled=true \
    --set metrics.port=9091)

assert_contains "$OUTPUT" "enabled: true" "Metrics enabled in ConfigMap"
assert_contains "$OUTPUT" "port: 9091" "Metrics port in ConfigMap"
echo ""

# Test 6: DML enabled
print_test "6. DML Configuration"
OUTPUT=$(render_template "dml-enabled" \
    --set dremio.uri=https://dremio.example.com:9047 \
    --set dremio.allowDml=true)

assert_contains "$OUTPUT" "allow_dml: true" "DML enabled in ConfigMap"
echo ""

# Test 7: Custom server mode
print_test "7. Custom Server Mode"
OUTPUT=$(render_template "custom-server-mode" \
    --set dremio.uri=https://dremio.example.com:9047 \
    --set 'tools.serverMode=FOR_SELF\,FOR_ADMIN')

assert_contains "$OUTPUT" "server_mode: \"FOR_SELF,FOR_ADMIN\"" "Custom server mode in ConfigMap"
echo ""

# Test 8: No environment variables for Dremio config
print_test "8. Environment Variables Removed"
OUTPUT=$(render_template "no-env-vars" \
    --set dremio.uri=https://dremio.example.com:9047)

assert_not_contains "$OUTPUT" "DREMIOAI_DREMIO__URI" "No URI environment variable"
assert_not_contains "$OUTPUT" "DREMIOAI_TOOLS__SERVER_MODE" "No server mode environment variable"
assert_not_contains "$OUTPUT" "DREMIOAI_DREMIO__METRICS" "No metrics environment variables"
echo ""

# Test 9: Volume mounts are read-only
print_test "9. Volume Mounts Security"
OUTPUT=$(render_template "volume-security" \
    --set dremio.uri=https://dremio.example.com:9047 \
    --set dremio.pat=test-pat)

assert_count "$OUTPUT" "readOnly: true" 2 "Both config and secrets mounted read-only"
echo ""

# Test 10: Command structure
print_test "10. Command Structure"
OUTPUT=$(render_template "command-structure" \
    --set dremio.uri=https://dremio.example.com:9047)

assert_contains "$OUTPUT" "- dremio-mcp-server" "Command starts with dremio-mcp-server"
assert_contains "$OUTPUT" "- run" "Run subcommand present"
assert_contains "$OUTPUT" "- --cfg" "Config flag present"
assert_contains "$OUTPUT" "- /app/config/config.yaml" "Config path present"
assert_contains "$OUTPUT" "- --enable-streaming-http" "Streaming HTTP enabled"
assert_contains "$OUTPUT" "- --no-log-to-file" "File logging disabled"
echo ""

# Test 11: Example values files
print_test "11. Example Values Files Render Successfully"

run_test
if render_template "example-oauth-production" -f ./helm/dremio-mcp/examples/values-oauth-production.yaml > /dev/null 2>&1; then
    print_pass "values-oauth-production.yaml renders successfully"
else
    print_fail "values-oauth-production.yaml fails to render"
fi

run_test
if render_template "example-onprem" -f ./helm/dremio-mcp/examples/values-onprem.yaml > /dev/null 2>&1; then
    print_pass "values-onprem.yaml renders successfully"
else
    print_fail "values-onprem.yaml fails to render"
fi

run_test
if render_template "example-with-pat" -f ./helm/dremio-mcp/examples/values-with-pat.yaml > /dev/null 2>&1; then
    print_pass "values-with-pat.yaml renders successfully"
else
    print_fail "values-with-pat.yaml fails to render"
fi

run_test
if render_template "example-existing-secret" -f ./helm/dremio-mcp/examples/values-with-existing-secret.yaml > /dev/null 2>&1; then
    print_pass "values-with-existing-secret.yaml renders successfully"
else
    print_fail "values-with-existing-secret.yaml fails to render"
fi
echo ""

# Test 12: ConfigMap labels
print_test "12. Resource Labels"
OUTPUT=$(render_template "labels" \
    --set dremio.uri=https://dremio.example.com:9047)

assert_contains "$OUTPUT" "app.kubernetes.io/name: dremio-mcp" "Standard labels present"
assert_contains "$OUTPUT" "app.kubernetes.io/instance: test-release" "Instance label present"
echo ""

# Summary
echo "========================================="
echo "Test Summary"
echo "========================================="
echo "Total tests run: $TESTS_RUN"
echo -e "${GREEN}Passed: $TESTS_PASSED${NC}"
if [ $TESTS_FAILED -gt 0 ]; then
    echo -e "${RED}Failed: $TESTS_FAILED${NC}"
else
    echo -e "${GREEN}Failed: $TESTS_FAILED${NC}"
fi
echo ""

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed! ✓${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed! ✗${NC}"
    echo "Check output files in: $TEST_OUTPUT_DIR"
    exit 1
fi

