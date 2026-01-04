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
    echo "Usage: $0 <protocol> <agent_type> <start_port> <count>"
    echo ""
    echo "参数说明:"
    echo "  protocol    - 协议类型 (rocketmq-a2a, http, a2a)"
    echo "  agent_type  - Agent类型 (weather 或 travel)"
    echo "  start_port  - 起始端口号"
    echo "  count       - 启动Agent数量"
    echo ""
    echo "示例:"
    echo "  $0 rocketmq-a2a weather 8080 3  # 在端口 8080, 8081, 8082 启动3个WeatherAgent"
    echo "  $0 a2a weather 8080 2            # 在端口 8080, 8081 启动2个WeatherAgent (A2A协议)"
    echo "  $0 a2a travel 8888 2            # 在端口 8888, 8889 启动2个TravelAgent (A2A协议)"
    exit 1
}

# 检查参数
if [ $# -ne 4 ]; then
    echo "❌ 错误: 参数数量不正确"
    usage
fi

PROTOCOL=$1
AGENT_TYPE=$2
START_PORT=$3
COUNT=$4

# 检查count是否为正整数
if ! [[ "$COUNT" =~ ^[1-9][0-9]*$ ]]; then
    echo "❌ 错误: count必须是正整数"
    usage
fi

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🚀 批量启动 ${COUNT} 个 ${AGENT_TYPE} Agent (${PROTOCOL})"
echo "   起始端口: ${START_PORT}"
echo ""

# 记录启动的Agent信息
SUCCESS_COUNT=0
FAIL_COUNT=0
AGENT_PIDS=()

# 循环启动多个Agent
for ((i=0; i<COUNT; i++)); do
    PORT=$((START_PORT + i))
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "[$((i+1))/${COUNT}] 启动 ${AGENT_TYPE} Agent 在端口 ${PORT}..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # 调用单Agent启动脚本
    "${SCRIPT_DIR}/start_agent.sh" "${PROTOCOL}" "${AGENT_TYPE}" "${PORT}"
    
    if [ $? -eq 0 ]; then
        ((SUCCESS_COUNT++))
        echo "✅ 端口 ${PORT} 启动成功"
    else
        ((FAIL_COUNT++))
        echo "❌ 端口 ${PORT} 启动失败"
    fi
    
    echo ""
    
    # 启动间隔，避免资源竞争
    if [ $i -lt $((COUNT - 1)) ]; then
        sleep 2
    fi
done

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 批量启动完成"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "   成功: ${SUCCESS_COUNT}"
echo "   失败: ${FAIL_COUNT}"
echo "   总计: ${COUNT}"
echo ""

if [ ${SUCCESS_COUNT} -gt 0 ]; then
    echo "💡 批量查看日志:"
    
    # 加载环境变量以获取BENCHMARK_ROOT
    source "${SCRIPT_DIR}/env.sh" > /dev/null 2>&1
    
    if [ "$AGENT_TYPE" == "weather" ]; then
        if [ "$PROTOCOL" == "http" ]; then
            AGENT_DIR="${BENCHMARK_ROOT}/http/WeatherAgent"
        elif [ "$PROTOCOL" == "a2a" ]; then
            AGENT_DIR="${BENCHMARK_ROOT}/a2a/WeatherAgent"
        else
            AGENT_DIR="${BENCHMARK_ROOT}/rocketmq-a2a/WeatherAgent"
        fi
    elif [ "$AGENT_TYPE" == "travel" ]; then
        if [ "$PROTOCOL" == "http" ]; then
            AGENT_DIR="${BENCHMARK_ROOT}/http/TravelAgent"
        elif [ "$PROTOCOL" == "a2a" ]; then
            AGENT_DIR="${BENCHMARK_ROOT}/a2a/TravelAgent"
        else
            AGENT_DIR="${BENCHMARK_ROOT}/rocketmq-a2a/TravelAgent"
        fi
    fi
    
    echo "   tail -f ${AGENT_DIR}/logs/${AGENT_TYPE}_*.log"
    echo ""
    echo "💡 批量停止服务:"
    if [ "$PROTOCOL" == "http" ]; then
        echo "   # HTTP协议使用Spring Boot，通过端口停止"
        for ((i=0; i<COUNT; i++)); do
            PORT=$((START_PORT + i))
            echo "   ${SCRIPT_DIR}/stop_agent.sh ${PORT}"
        done
    else
        echo "   # Quarkus应用通过端口停止"
        for ((i=0; i<COUNT; i++)); do
            PORT=$((START_PORT + i))
            echo "   ${SCRIPT_DIR}/stop_agent.sh ${PORT}"
        done
    fi
fi

exit ${FAIL_COUNT}
