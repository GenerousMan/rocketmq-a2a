#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# 使用说明
usage() {
    echo "Usage: $0 <protocol> <qps> <agent_process_time> <weather_ports> <travel_ports> <test_duration> <max_retry_times>"
    echo ""
    echo "参数说明:"
    echo "  protocol           - 协议类型 (rocketmq-a2a, http, a2a)"
    echo "  qps                - 每秒请求数 (例如: 10)"
    echo "  agent_process_time - Agent处理时间（秒）(例如: 2)"
    echo "  weather_ports      - WeatherAgent端口列表，多个用逗号分隔 (例如: 8082,8083)"
    echo "  travel_ports       - TravelAgent端口列表，多个用逗号分隔 (例如: 8888,8889)"
    echo "  test_duration      - 压测持续时间（秒）(例如: 60)"
    echo "  max_retry_times    - 最大重试次数 (例如: 3)"
    echo ""
    echo "示例:"
    echo "  $0 a2a 10 2 \"8082,8083\" \"8888,8889\" 60 3"
    echo "  $0 http 5 3 \"8080,8081\" \"8888\" 30 2"
    echo "  $0 rocketmq-a2a 20 2 \"8082,8083\" \"8888,8889\" 120 3"
    exit 1
}

# 检查参数
if [ $# -ne 7 ]; then
    echo "❌ 错误: 参数数量不正确"
    usage
fi

PROTOCOL=$1
QPS=$2
AGENT_PROCESS_TIME=$3
WEATHER_PORTS=$4
TRAVEL_PORTS=$5
TEST_DURATION=$6
MAX_RETRY_TIMES=$7

# 检查协议类型
if [ "$PROTOCOL" != "rocketmq-a2a" ] && [ "$PROTOCOL" != "http" ] && [ "$PROTOCOL" != "a2a" ]; then
    echo "❌ 错误: 不支持的协议类型 '$PROTOCOL'"
    echo "   支持的类型: rocketmq-a2a, http, a2a"
    exit 1
fi

# 检查数值参数
if ! [[ "$QPS" =~ ^[1-9][0-9]*$ ]]; then
    echo "❌ 错误: qps 必须是正整数"
    usage
fi

if ! [[ "$AGENT_PROCESS_TIME" =~ ^[1-9][0-9]*$ ]]; then
    echo "❌ 错误: agent_process_time 必须是正整数（秒）"
    usage
fi

if ! [[ "$TEST_DURATION" =~ ^[1-9][0-9]*$ ]]; then
    echo "❌ 错误: test_duration 必须是正整数（秒）"
    usage
fi

if ! [[ "$MAX_RETRY_TIMES" =~ ^[1-9][0-9]*$ ]]; then
    echo "❌ 错误: max_retry_times 必须是正整数"
    usage
fi

# 检查端口参数
if [ -z "$WEATHER_PORTS" ]; then
    echo "❌ 错误: weather_ports 不能为空"
    usage
fi

if [ -z "$TRAVEL_PORTS" ]; then
    echo "❌ 错误: travel_ports 不能为空"
    usage
fi

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 步骤1: 加载环境变量
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📋 步骤 1/3: 加载环境配置"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
source "${SCRIPT_DIR}/env.sh"
echo "✅ 环境配置已加载"
echo ""

# 步骤2: 启动 Weather Agent 和 Travel Agent
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📋 步骤 2/3: 启动 Agent"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "   协议: ${PROTOCOL}"
echo "   处理时间: ${AGENT_PROCESS_TIME} 秒"
echo "   WeatherAgent 端口: ${WEATHER_PORTS}"
echo "   TravelAgent 端口: ${TRAVEL_PORTS}"
echo ""

# 启动 Weather Agent
echo "🚀 启动 WeatherAgent..."
IFS=',' read -ra WEATHER_PORT_ARRAY <<< "$WEATHER_PORTS"
WEATHER_AGENT_URLS=()
for port in "${WEATHER_PORT_ARRAY[@]}"; do
    port=$(echo "$port" | xargs)  # 去除空格
    if ! [[ "$port" =~ ^[0-9]+$ ]]; then
        echo "❌ 错误: 无效的端口号: $port"
        exit 1
    fi
    
    echo "   - 启动 WeatherAgent 在端口 ${port}..."
    # start_agent.sh 会在后台启动 Agent
    "${SCRIPT_DIR}/start_agent.sh" "${PROTOCOL}" "weather" "${AGENT_PROCESS_TIME}" "${port}" > /dev/null 2>&1
    
    # 检查端口是否已监听（最多等待15秒）
    PORT_READY=false
    for i in {1..15}; do
        if lsof -ti tcp:${port} -sTCP:LISTEN > /dev/null 2>&1; then
            WEATHER_AGENT_URLS+=("http://localhost:${port}")
            echo "   ✅ 端口 ${port} 启动成功"
            PORT_READY=true
            break
        fi
        sleep 1
    done
    
    if [ "$PORT_READY" = false ]; then
        echo "   ❌ 端口 ${port} 启动失败（超时）"
        exit 1
    fi
    
    # 等待一下，确保服务完全就绪
    sleep 1
done

echo ""

# 启动 Travel Agent
echo "🚀 启动 TravelAgent..."
IFS=',' read -ra TRAVEL_PORT_ARRAY <<< "$TRAVEL_PORTS"
TRAVEL_AGENT_URLS=()
for port in "${TRAVEL_PORT_ARRAY[@]}"; do
    port=$(echo "$port" | xargs)  # 去除空格
    if ! [[ "$port" =~ ^[0-9]+$ ]]; then
        echo "❌ 错误: 无效的端口号: $port"
        exit 1
    fi
    
    echo "   - 启动 TravelAgent 在端口 ${port}..."
    # start_agent.sh 会在后台启动 Agent
    "${SCRIPT_DIR}/start_agent.sh" "${PROTOCOL}" "travel" "${AGENT_PROCESS_TIME}" "${port}" > /dev/null 2>&1
    
    # 检查端口是否已监听（最多等待15秒）
    PORT_READY=false
    for i in {1..15}; do
        if lsof -ti tcp:${port} -sTCP:LISTEN > /dev/null 2>&1; then
            TRAVEL_AGENT_URLS+=("http://localhost:${port}")
            echo "   ✅ 端口 ${port} 启动成功"
            PORT_READY=true
            break
        fi
        sleep 1
    done
    
    if [ "$PORT_READY" = false ]; then
        echo "   ❌ 端口 ${port} 启动失败（超时）"
        exit 1
    fi
    
    # 等待一下，确保服务完全就绪
    sleep 1
done

# 将URL数组转换为逗号分隔的字符串
WEATHER_URLS_STR=$(IFS=','; echo "${WEATHER_AGENT_URLS[*]}")
TRAVEL_URLS_STR=$(IFS=','; echo "${TRAVEL_AGENT_URLS[*]}")

echo ""
echo "✅ 所有 Agent 启动完成"
echo "   WeatherAgent URLs: ${WEATHER_URLS_STR}"
echo "   TravelAgent URLs: ${TRAVEL_URLS_STR}"
echo ""

# 额外等待，确保所有 Agent 完全就绪
echo "⏳ 等待所有 Agent 完全就绪..."
sleep 3

# 步骤3: 启动 Supervisor Agent
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📋 步骤 3/3: 启动 SupervisorAgent"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "   协议: ${PROTOCOL}"
echo "   QPS: ${QPS}"
echo "   压测时间: ${TEST_DURATION} 秒"
echo "   最大重试次数: ${MAX_RETRY_TIMES}"
echo "   测试消息: \"今天天气怎么样？\""
echo ""

# 根据协议选择测试消息
# SupervisorAgent 会根据消息内容路由到对应的 Agent
# 包含"天气"关键词的消息会路由到 WeatherAgent
# 包含"行程"或"旅行"关键词的消息会路由到 TravelAgent
TEST_MESSAGE="今天天气怎么样？"

echo "🚀 启动 SupervisorAgent..."
echo ""

# 启动 SupervisorAgent（在前台运行）
"${SCRIPT_DIR}/start_supervisor_agent.sh" \
    "${PROTOCOL}" \
    "${WEATHER_URLS_STR}" \
    "${TRAVEL_URLS_STR}" \
    "${MAX_RETRY_TIMES}" \
    "${TEST_MESSAGE}" \
    "${QPS}" \
    "${TEST_DURATION}"

# SupervisorAgent 退出后，清理 Agent 进程
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🧹 清理 Agent 进程..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# 停止 Weather Agent
for port in "${WEATHER_PORT_ARRAY[@]}"; do
    port=$(echo "$port" | xargs)
    echo "   停止 WeatherAgent 端口 ${port}..."
    "${SCRIPT_DIR}/stop_agent.sh" "${port}" > /dev/null 2>&1
done

# 停止 Travel Agent
for port in "${TRAVEL_PORT_ARRAY[@]}"; do
    port=$(echo "$port" | xargs)
    echo "   停止 TravelAgent 端口 ${port}..."
    "${SCRIPT_DIR}/stop_agent.sh" "${port}" > /dev/null 2>&1
done

echo ""
echo "✅ 压测完成，所有 Agent 已停止"

