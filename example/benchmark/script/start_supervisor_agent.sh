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
    echo "Usage: $0 <protocol> <weather_agent_urls> <travel_agent_urls> <max_retry_times> <test_message> <qps> <max_test_time>"
    echo ""
    echo "参数说明:"
    echo "  protocol           - 协议类型 (rocketmq-a2a, http, a2a)"
    echo "  weather_agent_urls - WeatherAgent URL列表，多个用逗号分隔 (例如: http://localhost:8080,http://localhost:8081)"
    echo "  travel_agent_urls  - TravelAgent URL列表，多个用逗号分隔 (例如: http://localhost:8888,http://localhost:8889)"
    echo "  max_retry_times    - 最大重试次数 (例如: 3)"
    echo "  test_message       - 测试消息内容 (例如: \"今天天气怎么样？\")"
    echo "  qps                - 每秒请求数 (例如: 10)"
    echo "  max_test_time      - 最大测试时间（秒）(例如: 60)"
    echo ""
    echo "示例:"
    echo "  $0 a2a \"http://localhost:8080,http://localhost:8081\" \"http://localhost:8888,http://localhost:8889\" 3 \"今天天气怎么样？\" 10 60"
    echo "  $0 http \"http://localhost:8080\" \"http://localhost:8888\" 3 \"查询天气\" 5 30"
    echo "  $0 rocketmq-a2a \"http://localhost:8080\" \"http://localhost:8888\" 3 \"天气查询\" 20 120"
    exit 1
}

# 检查参数
if [ $# -ne 7 ]; then
    echo "❌ 错误: 参数数量不正确"
    usage
fi

PROTOCOL=$1
WEATHER_AGENT_URLS=$2
TRAVEL_AGENT_URLS=$3
MAX_RETRY_TIMES=$4
TEST_MESSAGE=$5
QPS=$6
MAX_TEST_TIME=$7

# 检查协议类型
if [ "$PROTOCOL" != "rocketmq-a2a" ] && [ "$PROTOCOL" != "http" ] && [ "$PROTOCOL" != "a2a" ]; then
    echo "❌ 错误: 不支持的协议类型 '$PROTOCOL'"
    echo "   支持的类型: rocketmq-a2a, http, a2a"
    exit 1
fi

# 检查必需参数
if [ -z "$WEATHER_AGENT_URLS" ]; then
    echo "❌ 错误: weather_agent_urls 不能为空"
    usage
fi

if [ -z "$TRAVEL_AGENT_URLS" ]; then
    echo "❌ 错误: travel_agent_urls 不能为空"
    usage
fi

if [ -z "$TEST_MESSAGE" ]; then
    echo "❌ 错误: test_message 不能为空"
    usage
fi

# 检查数值参数
if ! [[ "$MAX_RETRY_TIMES" =~ ^[1-9][0-9]*$ ]]; then
    echo "❌ 错误: max_retry_times 必须是正整数"
    usage
fi

if ! [[ "$QPS" =~ ^[1-9][0-9]*$ ]]; then
    echo "❌ 错误: qps 必须是正整数"
    usage
fi

if ! [[ "$MAX_TEST_TIME" =~ ^[1-9][0-9]*$ ]]; then
    echo "❌ 错误: max_test_time 必须是正整数（秒）"
    usage
fi

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 加载环境变量
echo "📋 加载环境配置..."
source "${SCRIPT_DIR}/env.sh"

# 设置SupervisorAgent路径
if [ "$PROTOCOL" == "http" ]; then
    AGENT_DIR="${BENCHMARK_ROOT}/http/SupervisorAgent"
    JAR_FILE="target/HttpSupervisorAgent-1.0.0-SNAPSHOT.jar"
elif [ "$PROTOCOL" == "a2a" ]; then
    AGENT_DIR="${BENCHMARK_ROOT}/a2a/SupervisorAgent"
    JAR_FILE="target/A2ASupervisorAgent-1.0.0-SNAPSHOT.jar"
else
    AGENT_DIR="${BENCHMARK_ROOT}/rocketmq-a2a/SupervisorAgent"
    JAR_FILE="target/supervisor-agent-simple-jar-with-dependencies.jar"
fi

echo ""
echo "🚀 启动 SupervisorAgent (${PROTOCOL})"
echo "   - WeatherAgent URLs: ${WEATHER_AGENT_URLS}"
echo "   - TravelAgent URLs: ${TRAVEL_AGENT_URLS}"
echo "   - 最大重试次数: ${MAX_RETRY_TIMES}"
echo "   - 测试消息: ${TEST_MESSAGE}"
echo "   - QPS: ${QPS}"
echo "   - 最大测试时间: ${MAX_TEST_TIME} 秒"
echo "   - 目录: ${AGENT_DIR}"

# 检查Agent目录是否存在
if [ ! -d "$AGENT_DIR" ]; then
    echo "❌ 错误: Agent目录不存在: ${AGENT_DIR}"
    exit 1
fi

# 进入Agent目录
cd "${AGENT_DIR}" || exit 1

# 检查是否已构建
if [ ! -f "$JAR_FILE" ]; then
    echo ""
    echo "📦 首次运行，正在构建 SupervisorAgent..."
    mvn clean package -DskipTests -q
    if [ $? -ne 0 ]; then
        echo "❌ 构建失败"
        exit 1
    fi
    echo "✅ 构建完成"
fi

# 创建日志目录
LOG_DIR="${AGENT_DIR}/logs"
mkdir -p "${LOG_DIR}"

# 设置日志文件
LOG_FILE="${LOG_DIR}/supervisor_${PROTOCOL}_$(date +%Y%m%d_%H%M%S).log"

echo ""
echo "📤 启动 SupervisorAgent..."
echo "   - 日志文件: ${LOG_FILE}"

# 根据协议类型启动
case "$PROTOCOL" in
    rocketmq-a2a)
        # RocketMQ-A2A协议启动逻辑
        # 使用数组构建JVM参数，确保包含空格的参数被正确处理
        JVM_ARGS=(
            "-DweatherAgentUrls=${WEATHER_AGENT_URLS}"
            "-DtravelAgentUrls=${TRAVEL_AGENT_URLS}"
            "-DmaxRetryTimes=${MAX_RETRY_TIMES}"
            "-DtestMessage=${TEST_MESSAGE}"
            "-Dqps=${QPS}"
            "-DmaxTestTime=${MAX_TEST_TIME}"
        )
        
        # RocketMQ相关配置从环境变量读取
        if [ -n "${ROCKETMQ_NAMESPACE:-}" ]; then
            JVM_ARGS+=("-DrocketMQNamespace=${ROCKETMQ_NAMESPACE}")
        fi
        if [ -n "${ROCKETMQ_AK:-}" ]; then
            JVM_ARGS+=("-DrocketMQAK=${ROCKETMQ_AK}")
        fi
        if [ -n "${ROCKETMQ_SK:-}" ]; then
            JVM_ARGS+=("-DrocketMQSK=${ROCKETMQ_SK}")
        fi
        if [ -n "${WORK_AGENT_RESPONSE_TOPIC:-}" ]; then
            JVM_ARGS+=("-DworkAgentResponseTopic=${WORK_AGENT_RESPONSE_TOPIC}")
        fi
        if [ -n "${WORK_AGENT_RESPONSE_GROUP_ID:-}" ]; then
            JVM_ARGS+=("-DworkAgentResponseGroupID=${WORK_AGENT_RESPONSE_GROUP_ID}")
        fi
        
        # 启动应用（使用 -jar，因为JAR文件包含主类）
        # 在前台运行，支持 Ctrl+C 停止
        echo "💡 提示: 按 Ctrl+C 可以停止 SupervisorAgent"
        echo ""
        java "${JVM_ARGS[@]}" -jar "${JAR_FILE}" 2>&1 | tee "${LOG_FILE}"
        ;;
    
    http)
        # HTTP协议启动逻辑
        JVM_ARGS=(
            "-DweatherAgentUrls=${WEATHER_AGENT_URLS}"
            "-DtravelAgentUrls=${TRAVEL_AGENT_URLS}"
            "-DmaxRetryTimes=${MAX_RETRY_TIMES}"
            "-DtestMessage=${TEST_MESSAGE}"
            "-Dqps=${QPS}"
            "-DmaxTestTime=${MAX_TEST_TIME}"
        )
        
        # HTTP协议使用Spring Boot，可以通过server.port设置端口
        if [ -n "${SUPERVISOR_PORT:-}" ]; then
            JVM_ARGS+=("-Dserver.port=${SUPERVISOR_PORT}")
        else
            JVM_ARGS+=("-Dserver.port=9090")
        fi
        
        # 启动Spring Boot应用
        # 在前台运行，支持 Ctrl+C 停止
        echo "💡 提示: 按 Ctrl+C 可以停止 SupervisorAgent"
        echo ""
        java "${JVM_ARGS[@]}" -jar "${JAR_FILE}" 2>&1 | tee "${LOG_FILE}"
        ;;
    
    a2a)
        # A2A协议启动逻辑
        JVM_ARGS=(
            "-DweatherAgentUrls=${WEATHER_AGENT_URLS}"
            "-DtravelAgentUrls=${TRAVEL_AGENT_URLS}"
            "-DmaxRetryTimes=${MAX_RETRY_TIMES}"
            "-DtestMessage=${TEST_MESSAGE}"
            "-Dqps=${QPS}"
            "-DmaxTestTime=${MAX_TEST_TIME}"
        )
        
        # 启动应用（使用 -jar，因为JAR文件包含主类）
        # 在前台运行，支持 Ctrl+C 停止
        echo "💡 提示: 按 Ctrl+C 可以停止 SupervisorAgent"
        echo ""
        java "${JVM_ARGS[@]}" -jar "${JAR_FILE}" 2>&1 | tee "${LOG_FILE}"
        ;;
    
    *)
        echo "❌ 不支持的协议: ${PROTOCOL}"
        exit 1
        ;;
esac

# 注意：应用在前台运行，当用户按 Ctrl+C 时会自动停止
# 如果应用异常退出，脚本也会随之退出

