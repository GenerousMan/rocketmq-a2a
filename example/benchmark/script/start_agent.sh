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
    echo "Usage: $0 <protocol> <agent_type> <process_time_seconds> <port>"
    echo ""
    echo "参数说明:"
    echo "  protocol           - 协议类型 (rocketmq-a2a, http, a2a)"
    echo "  agent_type         - Agent类型 (weather 或 travel)"
    echo "  process_time_seconds - Agent处理时间（秒）"
    echo "  port               - 启动端口号"
    echo ""
    echo "示例:"
    echo "  $0 rocketmq-a2a weather 2 8080"
    echo "  $0 http weather 5 8080"
    echo "  $0 a2a weather 3 8080"
    echo "  $0 a2a travel 2 8888"
    exit 1
}

# 检查参数
if [ $# -ne 4 ]; then
    echo "❌ 错误: 参数数量不正确"
    usage
fi

PROTOCOL=$1
AGENT_TYPE=$2
PROCESS_TIME_SECONDS=$3
PORT=$4

# 检查处理时间是否为正整数
if ! [[ "$PROCESS_TIME_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
    echo "❌ 错误: 处理时间必须是正整数（秒）"
    usage
fi

# 检查协议类型
if [ "$PROTOCOL" != "rocketmq-a2a" ] && [ "$PROTOCOL" != "http" ] && [ "$PROTOCOL" != "a2a" ]; then
    echo "❌ 错误: 不支持的协议类型 '$PROTOCOL'"
    echo "   支持的类型: rocketmq-a2a, http, a2a"
    exit 1
fi

# 检查Agent类型
if [ "$AGENT_TYPE" != "weather" ] && [ "$AGENT_TYPE" != "travel" ]; then
    echo "❌ 错误: 不支持的Agent类型 '$AGENT_TYPE'"
    echo "   支持的类型: weather, travel"
    exit 1
fi

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 加载环境变量
echo "📋 加载环境配置..."
source "${SCRIPT_DIR}/env.sh"

# 设置Agent名称和路径
if [ "$AGENT_TYPE" == "weather" ]; then
    AGENT_NAME="WeatherAgent"
    if [ "$PROTOCOL" == "http" ]; then
        AGENT_DIR="${BENCHMARK_ROOT}/http/WeatherAgent"
    elif [ "$PROTOCOL" == "a2a" ]; then
        AGENT_DIR="${BENCHMARK_ROOT}/a2a/WeatherAgent"
    else
        AGENT_DIR="${BENCHMARK_ROOT}/rocketmq-a2a/WeatherAgent"
    fi
elif [ "$AGENT_TYPE" == "travel" ]; then
    AGENT_NAME="TravelAgent"
    if [ "$PROTOCOL" == "http" ]; then
        AGENT_DIR="${BENCHMARK_ROOT}/http/TravelAgent"
    elif [ "$PROTOCOL" == "a2a" ]; then
        AGENT_DIR="${BENCHMARK_ROOT}/a2a/TravelAgent"
    else
        AGENT_DIR="${BENCHMARK_ROOT}/rocketmq-a2a/TravelAgent"
    fi
fi

echo ""
echo "🚀 启动 ${AGENT_NAME} (${PROTOCOL})"
echo "   - 端口: ${PORT}"
echo "   - 处理时间: ${PROCESS_TIME_SECONDS} 秒"
echo "   - 目录: ${AGENT_DIR}"

# 检查端口占用情况
echo ""
echo "🔍 检查端口 ${PORT} 占用情况..."

# 查找监听该端口的进程（只杀监听端口的进程，不杀连接到该端口的客户端）
LISTENING_PIDS=$(lsof -ti tcp:${PORT} -sTCP:LISTEN 2>/dev/null)

if [ -n "$LISTENING_PIDS" ]; then
    echo "⚠️  端口 ${PORT} 已被以下进程监听: ${LISTENING_PIDS}"
    echo "   正在停止进程..."
    
    for PID in $LISTENING_PIDS; do
        # 获取进程名称以确认
        PROC_NAME=$(ps -p ${PID} -o comm= 2>/dev/null | tail -1)
        echo "   - 停止进程 ${PID} (${PROC_NAME})..."
        kill -15 ${PID} 2>/dev/null
        
        # 等待进程优雅退出
        for i in {1..5}; do
            sleep 1
            if ! ps -p ${PID} > /dev/null 2>&1; then
                echo "   ✅ 进程 ${PID} 已停止"
                break
            fi
        done
        
        # 如果还没停止，强制终止
        if ps -p ${PID} > /dev/null 2>&1; then
            echo "   强制终止进程 ${PID}..."
            kill -9 ${PID} 2>/dev/null
            sleep 1
        fi
    done
    
    # 再次检查端口
    sleep 1
    LISTENING_PIDS=$(lsof -ti tcp:${PORT} -sTCP:LISTEN 2>/dev/null)
    if [ -n "$LISTENING_PIDS" ]; then
        echo "❌ 无法释放端口 ${PORT}，仍有进程 ${LISTENING_PIDS} 监听，请手动处理"
        exit 1
    fi
    echo "✅ 端口 ${PORT} 已释放"
else
    echo "✅ 端口 ${PORT} 可用"
fi

# 检查Agent目录是否存在
if [ ! -d "$AGENT_DIR" ]; then
    echo "❌ 错误: Agent目录不存在: ${AGENT_DIR}"
    exit 1
fi

# 进入Agent目录
cd "${AGENT_DIR}" || exit 1

# 检查是否已构建
if [ "$PROTOCOL" == "http" ]; then
    JAR_FILE="target/Http${AGENT_NAME}-1.0.0-SNAPSHOT.jar"
elif [ "$PROTOCOL" == "a2a" ]; then
    JAR_FILE="target/quarkus-app/quarkus-run.jar"
    AGENT_JAR_NAME="A2A${AGENT_NAME}"
else
    JAR_FILE="target/quarkus-app/quarkus-run.jar"
fi

if [ ! -f "$JAR_FILE" ]; then
    echo ""
    echo "📦 首次运行，正在构建 ${AGENT_NAME}..."
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
LOG_FILE="${LOG_DIR}/${AGENT_TYPE}_${PORT}.log"

echo ""
echo "📤 启动 ${AGENT_NAME}..."
echo "   - 日志文件: ${LOG_FILE}"

# 根据协议类型启动
case "$PROTOCOL" in
    rocketmq-a2a)
        # 设置RocketMQ相关的系统属性
        JVM_OPTS="-Dquarkus.http.port=${PORT}"
        JVM_OPTS="${JVM_OPTS} -DdelaySeconds=${PROCESS_TIME_SECONDS}"
        JVM_OPTS="${JVM_OPTS} -DrocketMQNamespace=${ROCKETMQ_NAMESPACE}"
        JVM_OPTS="${JVM_OPTS} -DrocketMQAK=${ROCKETMQ_AK}"
        JVM_OPTS="${JVM_OPTS} -DrocketMQSK=${ROCKETMQ_SK}"
        JVM_OPTS="${JVM_OPTS} -DrocketMQEndpoint=${ROCKETMQ_ENDPOINT}"
        JVM_OPTS="${JVM_OPTS} -DworkAgentResponseTopic=${WORK_AGENT_RESPONSE_TOPIC}"
        JVM_OPTS="${JVM_OPTS} -DworkAgentResponseGroupID=${WORK_AGENT_RESPONSE_GROUP_ID}"
        JVM_OPTS="${JVM_OPTS} -DapiKey=${API_KEY}"
        
        # 根据Agent类型设置业务Topic和ConsumerGroup
        if [ "$AGENT_TYPE" == "weather" ]; then
            JVM_OPTS="${JVM_OPTS} -DbizTopic=${WEATHER_AGENT_BIZ_TOPIC}"
            JVM_OPTS="${JVM_OPTS} -DbizConsumerGroup=${WEATHER_AGENT_BIZ_CONSUMER_GROUP}"
        elif [ "$AGENT_TYPE" == "travel" ]; then
            JVM_OPTS="${JVM_OPTS} -DbizTopic=${TRAVEL_AGENT_BIZ_TOPIC}"
            JVM_OPTS="${JVM_OPTS} -DbizConsumerGroup=${TRAVEL_AGENT_BIZ_CONSUMER_GROUP}"
        fi
        
        # 启动Quarkus应用
        nohup java ${JVM_OPTS} -jar target/quarkus-app/quarkus-run.jar > "${LOG_FILE}" 2>&1 &
        AGENT_PID=$!
        ;;
    
    http)
        # HTTP协议启动逻辑
        JVM_OPTS="-Dserver.port=${PORT}"
        JVM_OPTS="${JVM_OPTS} -DdelaySeconds=${PROCESS_TIME_SECONDS}"
        
        # 启动Spring Boot应用
        JAR_FILE="target/Http${AGENT_NAME}-1.0.0-SNAPSHOT.jar"
        nohup java ${JVM_OPTS} -jar "${JAR_FILE}" > "${LOG_FILE}" 2>&1 &
        AGENT_PID=$!
        ;;
    
    a2a)
        # A2A协议启动逻辑（使用Quarkus，纯A2A协议，不需要RocketMQ配置）
        JVM_OPTS="-Dquarkus.http.port=${PORT}"
        JVM_OPTS="${JVM_OPTS} -DagentUrl=http://localhost:${PORT}"
        JVM_OPTS="${JVM_OPTS} -DdelaySeconds=${PROCESS_TIME_SECONDS}"
        
        # 可选配置：固定响应内容
        if [ -n "${A2A_FIXED_RESPONSE:-}" ]; then
            JVM_OPTS="${JVM_OPTS} -DfixedResponse=\"${A2A_FIXED_RESPONSE}\""
        fi
        
        # 启动Quarkus应用
        nohup java ${JVM_OPTS} -jar target/quarkus-app/quarkus-run.jar > "${LOG_FILE}" 2>&1 &
        AGENT_PID=$!
        ;;
    
    *)
        echo "❌ 不支持的协议: ${PROTOCOL}"
        exit 1
        ;;
esac

# 等待几秒钟
sleep 3

# 检查进程是否存活
if ps -p ${AGENT_PID} > /dev/null 2>&1; then
    echo ""
    echo "✅ ${AGENT_NAME} 启动成功!"
    echo "   - PID: ${AGENT_PID}"
    echo "   - 端口: ${PORT}"
    echo "   - 处理时间: ${PROCESS_TIME_SECONDS} 秒"
    echo "   - 日志: ${LOG_FILE}"
    echo ""
    echo "💡 查看日志: tail -f ${LOG_FILE}"
    echo "💡 停止服务: kill ${AGENT_PID}"
else
    echo ""
    echo "❌ ${AGENT_NAME} 启动失败"
    echo "   请查看日志文件: ${LOG_FILE}"
    exit 1
fi
