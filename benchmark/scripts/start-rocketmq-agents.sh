#!/bin/bash
#
# 一键启动 RocketMQ 版 Agents
# 端口：WeatherAgent=8080, TravelAgent=8888
#
# 必需环境变量:
#   ROCKETMQ_ENDPOINT - RocketMQ端点
#   ROCKETMQ_NAMESPACE - RocketMQ命名空间
#   ROCKETMQ_AK - Access Key
#   ROCKETMQ_SK - Secret Key
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DIR="$(dirname "$SCRIPT_DIR")"

# 配置参数
MOCK_DELAY_MS="${MOCK_DELAY_MS:-1000}"
ROCKETMQ_ENDPOINT="${ROCKETMQ_ENDPOINT:-}"
ROCKETMQ_NAMESPACE="${ROCKETMQ_NAMESPACE:-}"
ROCKETMQ_AK="${ROCKETMQ_AK:-}"
ROCKETMQ_SK="${ROCKETMQ_SK:-}"

echo "=========================================="
echo "  启动 RocketMQ 版 Agents"
echo "=========================================="
echo "Mock延迟: ${MOCK_DELAY_MS}ms"
echo "RocketMQ: ${ROCKETMQ_ENDPOINT}"
echo ""

# 检查必需参数
if [ -z "$ROCKETMQ_ENDPOINT" ] || [ -z "$ROCKETMQ_NAMESPACE" ] || [ -z "$ROCKETMQ_AK" ] || [ -z "$ROCKETMQ_SK" ]; then
    echo "❌ 缺少必需的RocketMQ配置，请设置以下环境变量:"
    echo "   export ROCKETMQ_ENDPOINT=xxx"
    echo "   export ROCKETMQ_NAMESPACE=xxx"
    echo "   export ROCKETMQ_AK=xxx"
    echo "   export ROCKETMQ_SK=xxx"
    exit 1
fi

# 停止已有进程
echo "🔄 停止已有进程..."
pkill -f "quarkus:dev" 2>/dev/null || true
sleep 2

# 启动 Weather Agent
echo "🚀 启动 RocketMQ Weather Agent (端口: 8080)..."
cd "$BENCHMARK_DIR/rocketmq-agents/weather-agent"
nohup mvn quarkus:dev -DskipTests -Dcheckstyle.skip=true \
    -DrocketMQEndpoint=$ROCKETMQ_ENDPOINT \
    -DrocketMQNamespace=$ROCKETMQ_NAMESPACE \
    -DrocketMQAK=$ROCKETMQ_AK \
    -DrocketMQSK=$ROCKETMQ_SK \
    -DbizTopic=WeatherAgentTask \
    -DbizConsumerGroup=WeatherAgentTaskConsumerGroup \
    -DmockDelayMs=$MOCK_DELAY_MS \
    -DmockEnabled=true \
    > /tmp/rocketmq-weather-agent.log 2>&1 &
WEATHER_PID=$!
echo "   PID: $WEATHER_PID"

# 等待Weather Agent启动
sleep 15

# 启动 Travel Agent
echo "🚀 启动 RocketMQ Travel Agent (端口: 8888)..."
cd "$BENCHMARK_DIR/rocketmq-agents/travel-agent"
nohup mvn quarkus:dev -DskipTests -Dcheckstyle.skip=true \
    -Dquarkus.http.port=8888 \
    -Ddebug=5006 \
    -DrocketMQEndpoint=$ROCKETMQ_ENDPOINT \
    -DrocketMQNamespace=$ROCKETMQ_NAMESPACE \
    -DrocketMQAK=$ROCKETMQ_AK \
    -DrocketMQSK=$ROCKETMQ_SK \
    -DbizTopic=TravelAgentTask \
    -DbizConsumerGroup=TravelAgentTaskConsumerGroup \
    -DmockDelayMs=$MOCK_DELAY_MS \
    -DmockEnabled=true \
    > /tmp/rocketmq-travel-agent.log 2>&1 &
TRAVEL_PID=$!
echo "   PID: $TRAVEL_PID"

# 等待启动
echo ""
echo "⏳ 等待服务启动..."
sleep 20

# 检查服务状态
echo ""
echo "🔍 检查服务状态..."
if curl -s http://localhost:8080/.well-known/agent-card.json > /dev/null 2>&1; then
    echo "   ✅ Weather Agent (8080): 运行中"
else
    echo "   ❌ Weather Agent (8080): 启动失败，请检查日志"
fi

if curl -s http://localhost:8888/.well-known/agent-card.json > /dev/null 2>&1; then
    echo "   ✅ Travel Agent (8888): 运行中"
else
    echo "   ❌ Travel Agent (8888): 启动失败，请检查日志"
fi

echo ""
echo "=========================================="
echo "  RocketMQ Agents 启动完成"
echo "=========================================="
echo "日志文件:"
echo "  - Weather: /tmp/rocketmq-weather-agent.log"
echo "  - Travel:  /tmp/rocketmq-travel-agent.log"
echo ""
echo "停止服务: ./stop-rocketmq-agents.sh"

