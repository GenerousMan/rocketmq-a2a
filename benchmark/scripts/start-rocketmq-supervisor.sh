#!/bin/bash
#
# 启动 RocketMQ 版 Supervisor Agent (交互式)
#
# 前提：已启动 Weather Agent 和 Travel Agent
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
ROCKETMQ_NAMESPACE="${ROCKETMQ_NAMESPACE:-}"
ROCKETMQ_AK="${ROCKETMQ_AK:-}"
ROCKETMQ_SK="${ROCKETMQ_SK:-}"

echo "=========================================="
echo "  启动 RocketMQ Supervisor Agent"
echo "=========================================="
echo "Mock延迟: ${MOCK_DELAY_MS}ms"
echo "响应模式: ${MOCK_SUPERVISOR_MODE}"
echo ""

# 检查必需参数
if [ -z "$ROCKETMQ_NAMESPACE" ] || [ -z "$ROCKETMQ_AK" ] || [ -z "$ROCKETMQ_SK" ]; then
    echo "❌ 缺少必需的RocketMQ配置"
    echo "请先执行: source env.sh"
    exit 1
fi

# 检查JAR是否存在
SUPERVISOR_JAR="$BENCHMARK_DIR/rocketmq-agents/supervisor-agent/target/rocketmq-supervisor-agent-1.0.0-SNAPSHOT-jar-with-dependencies.jar"
if [ ! -f "$SUPERVISOR_JAR" ]; then
    echo "❌ JAR文件不存在，请先执行: cd $BENCHMARK_DIR && mvn package -DskipTests"
    exit 1
fi

# 检查Worker Agents是否运行
echo "🔍 检查 Worker Agents..."
if ! curl -s http://localhost:8080/.well-known/agent-card.json > /dev/null 2>&1; then
    echo "   ⚠️  Weather Agent (8080) 未运行"
    echo "   请先执行: ./start-rocketmq-agents.sh"
    exit 1
fi
if ! curl -s http://localhost:8888/.well-known/agent-card.json > /dev/null 2>&1; then
    echo "   ⚠️  Travel Agent (8888) 未运行"
    echo "   请先执行: ./start-rocketmq-agents.sh"
    exit 1
fi
echo "   ✅ Worker Agents 运行中"
echo ""

# 启动 Supervisor Agent (交互式)
echo "🚀 启动 Supervisor Agent..."
echo "=========================================="
echo ""

java \
    -DmockEnabled=true \
    -DmockDelayMs=$MOCK_DELAY_MS \
    -DmockSupervisorMode=$MOCK_SUPERVISOR_MODE \
    -DrocketMQNamespace=$ROCKETMQ_NAMESPACE \
    -DrocketMQAK=$ROCKETMQ_AK \
    -DrocketMQSK=$ROCKETMQ_SK \
    -DworkAgentResponseTopic=WorkerAgentResponse \
    -DworkAgentResponseGroupID=CID_HOST_AGENT_LITE \
    -jar "$SUPERVISOR_JAR"

