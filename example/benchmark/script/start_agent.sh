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
    echo "Usage: $0 <protocol> <agent_type> <port>"
    echo ""
    echo "参数说明:"
    echo "  protocol    - 协议类型 (目前仅支持: rocketmq-a2a)"
    echo "  agent_type  - Agent类型 (weather 或 travel)"
    echo "  port        - 启动端口号"
    echo ""
    echo "示例:"
    echo "  $0 rocketmq-a2a weather 8080"
    echo "  $0 rocketmq-a2a travel 8888"
    exit 1
}

# 检查参数
if [ $# -ne 3 ]; then
    echo "❌ 错误: 参数数量不正确"
    usage
fi

PROTOCOL=$1
AGENT_TYPE=$2
PORT=$3

# 检查协议类型
if [ "$PROTOCOL" != "rocketmq-a2a" ]; then
    echo "❌ 错误: 不支持的协议类型 '$PROTOCOL'"
    echo "   目前仅支持: rocketmq-a2a"
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
    AGENT_DIR="${BENCHMARK_ROOT}/WeatherAgent"
elif [ "$AGENT_TYPE" == "travel" ]; then
    AGENT_NAME="TravelAgent"
    AGENT_DIR="${BENCHMARK_ROOT}/TravelAgent"
fi

echo ""
echo "🚀 启动 ${AGENT_NAME} (${PROTOCOL})"
echo "   - 端口: ${PORT}"
echo "   - 目录: ${AGENT_DIR}"

# 检查端口占用情况
echo ""
echo "🔍 检查端口 ${PORT} 占用情况..."
PID=$(lsof -ti tcp:${PORT} 2>/dev/null)

if [ -n "$PID" ]; then
    echo "⚠️  端口 ${PORT} 已被进程 ${PID} 占用"
    echo "   正在杀掉进程..."
    kill -9 ${PID} 2>/dev/null
    sleep 2
    
    # 再次检查
    PID=$(lsof -ti tcp:${PORT} 2>/dev/null)
    if [ -n "$PID" ]; then
        echo "❌ 无法杀掉进程 ${PID}，请手动处理"
        exit 1
    fi
    echo "✅ 进程已终止"
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
if [ ! -f "target/quarkus-app/quarkus-run.jar" ]; then
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
        JVM_OPTS="${JVM_OPTS} -DrocketMQNamespace=${ROCKETMQ_NAMESPACE}"
        JVM_OPTS="${JVM_OPTS} -DrocketMQAK=${ROCKETMQ_AK}"
        JVM_OPTS="${JVM_OPTS} -DrocketMQSK=${ROCKETMQ_SK}"
        JVM_OPTS="${JVM_OPTS} -DworkAgentResponseTopic=${WORK_AGENT_RESPONSE_TOPIC}"
        JVM_OPTS="${JVM_OPTS} -DworkAgentResponseGroupID=${WORK_AGENT_RESPONSE_GROUP_ID}"
        JVM_OPTS="${JVM_OPTS} -DapiKey=${API_KEY}"
        
        # 启动Quarkus应用
        nohup java ${JVM_OPTS} -jar target/quarkus-app/quarkus-run.jar > "${LOG_FILE}" 2>&1 &
        AGENT_PID=$!
        ;;
    
    http)
        # TODO: HTTP协议启动逻辑
        echo "⚠️  HTTP协议启动逻辑待实现"
        exit 1
        ;;
    
    a2a)
        # TODO: A2A协议启动逻辑
        echo "⚠️  A2A协议启动逻辑待实现"
        exit 1
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
