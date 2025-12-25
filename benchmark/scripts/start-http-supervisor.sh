#!/bin/bash
#
# 启动 HTTP 直连版 Supervisor Agent (交互式)
#
# 前提：已启动 HTTP Weather Agent (9080) 和 Travel Agent (9888)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DIR="$(dirname "$SCRIPT_DIR")"

# 加载环境变量
if [ -f "$SCRIPT_DIR/env.sh" ]; then
    source "$SCRIPT_DIR/env.sh"
fi

# 配置参数
MOCK_DELAY_MS="${MOCK_DELAY_MS:-1000}"
MOCK_SUPERVISOR_MODE="${MOCK_SUPERVISOR_MODE:-AUTO}"

echo "=========================================="
echo "  启动 HTTP Supervisor Agent"
echo "=========================================="
echo "Mock延迟: ${MOCK_DELAY_MS}ms"
echo "响应模式: ${MOCK_SUPERVISOR_MODE}"
echo ""

# 检查JAR是否存在
SUPERVISOR_JAR="$BENCHMARK_DIR/http-agents/supervisor-agent/target/http-supervisor-agent-1.0.0-SNAPSHOT-jar-with-dependencies.jar"
if [ ! -f "$SUPERVISOR_JAR" ]; then
    echo "❌ JAR文件不存在，请先执行: cd $BENCHMARK_DIR && mvn package -DskipTests"
    exit 1
fi

# 检查Worker Agents是否运行
echo "🔍 检查 HTTP Worker Agents..."
if ! curl -s http://localhost:9080/health > /dev/null 2>&1; then
    echo "   ⚠️  Weather Agent (9080) 未运行"
    echo "   请先执行: ./start-http-agents.sh"
    exit 1
fi
if ! curl -s http://localhost:9888/health > /dev/null 2>&1; then
    echo "   ⚠️  Travel Agent (9888) 未运行"
    echo "   请先执行: ./start-http-agents.sh"
    exit 1
fi
echo "   ✅ HTTP Worker Agents 运行中"
echo ""

# 启动 Supervisor Agent (交互式)
echo "🚀 启动 HTTP Supervisor Agent..."
echo "=========================================="
echo ""

java \
    -DmockDelayMs=$MOCK_DELAY_MS \
    -DmockSupervisorMode=$MOCK_SUPERVISOR_MODE \
    -DweatherAgentUrl=http://localhost:9080 \
    -DtravelAgentUrl=http://localhost:9888 \
    -jar "$SUPERVISOR_JAR"

