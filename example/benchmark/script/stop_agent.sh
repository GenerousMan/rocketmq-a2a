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
    echo "Usage: $0 [port|all]"
    echo ""
    echo "参数说明:"
    echo "  port - 指定端口号，停止该端口上的Agent"
    echo "  all  - 停止所有Agent (WeatherAgent 和 TravelAgent)"
    echo ""
    echo "示例:"
    echo "  $0 8080        # 停止端口8080上的Agent"
    echo "  $0 all         # 停止所有Agent"
    exit 1
}

# 检查参数
if [ $# -ne 1 ]; then
    echo "❌ 错误: 参数数量不正确"
    usage
fi

TARGET=$1

# 停止指定端口的Agent
stop_by_port() {
    local PORT=$1
    echo "🔍 查找端口 ${PORT} 上的进程..."
    
    PID=$(lsof -ti tcp:${PORT} 2>/dev/null)
    
    if [ -n "$PID" ]; then
        echo "⚠️  找到进程 ${PID}"
        echo "   正在停止..."
        kill -15 ${PID} 2>/dev/null
        
        # 等待进程优雅退出
        for i in {1..10}; do
            sleep 1
            if ! ps -p ${PID} > /dev/null 2>&1; then
                echo "✅ 进程已停止"
                return 0
            fi
        done
        
        # 如果还没停止，强制杀掉
        echo "   进程未响应，强制终止..."
        kill -9 ${PID} 2>/dev/null
        sleep 1
        
        if ps -p ${PID} > /dev/null 2>&1; then
            echo "❌ 无法停止进程 ${PID}"
            return 1
        else
            echo "✅ 进程已强制停止"
            return 0
        fi
    else
        echo "ℹ️  端口 ${PORT} 上没有运行的进程"
        return 0
    fi
}

# 停止所有Agent
stop_all() {
    echo "🛑 停止所有 Agent..."
    echo ""
    
    # 查找所有WeatherAgent和TravelAgent进程
    PIDS=$(ps aux | grep -E '(WeatherAgent|TravelAgent)' | grep -v grep | grep -v "$0" | awk '{print $2}')
    
    if [ -z "$PIDS" ]; then
        echo "ℹ️  没有找到运行中的Agent"
        return 0
    fi
    
    TOTAL=$(echo "$PIDS" | wc -l | tr -d ' ')
    echo "📋 找到 ${TOTAL} 个运行中的Agent进程"
    echo ""
    
    SUCCESS=0
    FAIL=0
    
    for PID in $PIDS; do
        # 获取端口信息
        PORT=$(lsof -Pan -p ${PID} -i tcp 2>/dev/null | grep LISTEN | awk '{print $9}' | cut -d: -f2 | head -1)
        
        if [ -n "$PORT" ]; then
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo "停止 Agent (PID: ${PID}, 端口: ${PORT})..."
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        else
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo "停止 Agent (PID: ${PID})..."
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        fi
        
        kill -15 ${PID} 2>/dev/null
        
        # 等待进程优雅退出
        for i in {1..10}; do
            sleep 1
            if ! ps -p ${PID} > /dev/null 2>&1; then
                echo "✅ 进程已停止"
                ((SUCCESS++))
                break
            fi
        done
        
        # 检查是否还在运行
        if ps -p ${PID} > /dev/null 2>&1; then
            echo "   进程未响应，强制终止..."
            kill -9 ${PID} 2>/dev/null
            sleep 1
            
            if ps -p ${PID} > /dev/null 2>&1; then
                echo "❌ 无法停止进程 ${PID}"
                ((FAIL++))
            else
                echo "✅ 进程已强制停止"
                ((SUCCESS++))
            fi
        fi
        
        echo ""
    done
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "📊 停止完成"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "   成功: ${SUCCESS}"
    echo "   失败: ${FAIL}"
    echo "   总计: ${TOTAL}"
    
    return ${FAIL}
}

# 主逻辑
if [ "$TARGET" == "all" ]; then
    stop_all
elif [[ "$TARGET" =~ ^[0-9]+$ ]]; then
    stop_by_port $TARGET
else
    echo "❌ 错误: 无效的参数 '$TARGET'"
    usage
fi

exit $?
