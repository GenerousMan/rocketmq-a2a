#!/bin/bash
#
# 一键启动 HTTP 直连版 Agents
# 端口：WeatherAgent=9080, TravelAgent=9888
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DIR="$(dirname "$SCRIPT_DIR")"

# 配置参数
MOCK_DELAY_MS="${MOCK_DELAY_MS:-1000}"

echo "=========================================="
echo "  启动 HTTP 直连版 Agents"
echo "=========================================="
echo "Mock延迟: ${MOCK_DELAY_MS}ms"
echo ""

# 检查JAR是否存在
WEATHER_JAR="$BENCHMARK_DIR/http-agents/weather-agent/target/http-weather-agent-1.0.0-SNAPSHOT-jar-with-dependencies.jar"
TRAVEL_JAR="$BENCHMARK_DIR/http-agents/travel-agent/target/http-travel-agent-1.0.0-SNAPSHOT-jar-with-dependencies.jar"

if [ ! -f "$WEATHER_JAR" ] || [ ! -f "$TRAVEL_JAR" ]; then
    echo "❌ JAR文件不存在，请先执行: cd $BENCHMARK_DIR && mvn package -DskipTests"
    exit 1
fi

# 停止已有进程
echo "🔄 停止已有进程..."
pkill -f "http-weather-agent" 2>/dev/null || true
pkill -f "http-travel-agent" 2>/dev/null || true
sleep 1

# 启动 Weather Agent
echo "🚀 启动 HTTP Weather Agent (端口: 9080)..."
java -DmockDelayMs=$MOCK_DELAY_MS \
     -jar "$WEATHER_JAR" \
     > /tmp/http-weather-agent.log 2>&1 &
WEATHER_PID=$!
echo "   PID: $WEATHER_PID"

# 启动 Travel Agent
echo "🚀 启动 HTTP Travel Agent (端口: 9888)..."
java -DmockDelayMs=$MOCK_DELAY_MS \
     -jar "$TRAVEL_JAR" \
     > /tmp/http-travel-agent.log 2>&1 &
TRAVEL_PID=$!
echo "   PID: $TRAVEL_PID"

# 等待启动
echo ""
echo "⏳ 等待服务启动..."
sleep 3

# 检查服务状态
echo ""
echo "🔍 检查服务状态..."
if curl -s http://localhost:9080/health > /dev/null 2>&1; then
    echo "   ✅ Weather Agent (9080): 运行中"
else
    echo "   ❌ Weather Agent (9080): 启动失败"
fi

if curl -s http://localhost:9888/health > /dev/null 2>&1; then
    echo "   ✅ Travel Agent (9888): 运行中"
else
    echo "   ❌ Travel Agent (9888): 启动失败"
fi

echo ""
echo "=========================================="
echo "  HTTP Agents 启动完成"
echo "=========================================="
echo "日志文件:"
echo "  - Weather: /tmp/http-weather-agent.log"
echo "  - Travel:  /tmp/http-travel-agent.log"
echo ""
echo "停止服务: ./stop-http-agents.sh"

