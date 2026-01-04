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

# JVM进程监控脚本
# 用法: ./monitor_jvm.sh [PID或端口号] [监控间隔(秒)]
# 示例: ./monitor_jvm.sh 8080 5
#       ./monitor_jvm.sh 12345 2

TARGET=${1}
INTERVAL=${2:-5}

# 判断输入是PID还是端口号
if [ -z "$TARGET" ]; then
    echo "用法: $0 [PID或端口号] [监控间隔(秒)]"
    echo "示例: $0 8080 5"
    echo "      $0 12345 2"
    exit 1
fi

# 判断是端口号还是PID
if [[ "$TARGET" =~ ^[0-9]+$ ]]; then
    # 尝试通过端口查找PID
    PID=$(lsof -ti tcp:${TARGET} -sTCP:LISTEN 2>/dev/null)
    if [ -z "$PID" ]; then
        # 如果端口找不到，假设是PID
        PID=${TARGET}
        if ! ps -p ${PID} > /dev/null 2>&1; then
            echo "❌ 未找到进程 PID: ${PID}"
            exit 1
        fi
    fi
else
    echo "❌ 无效的PID或端口号: ${TARGET}"
    exit 1
fi

# 验证进程是否存在
if ! ps -p ${PID} > /dev/null 2>&1; then
    echo "❌ 进程不存在: PID ${PID}"
    exit 1
fi

# 检查 jstat 是否可用
if ! command -v jstat &> /dev/null; then
    echo "❌ 错误: jstat 命令不可用，请确保已安装JDK"
    exit 1
fi

# 记录脚本启动时间
START_TIME=$(date +%s)

# 获取初始值（用于计算增量）
INITIAL_GC_INFO=$(jstat -gc ${PID} 2>/dev/null | awk 'NR==2 {
    S0U=$3; S1U=$4; EU=$5; OU=$6;
    S0C=$7; S1C=$8; EC=$9; OC=$10;
    FGC=$17;
    HEAP_USED = S0U + S1U + EU + OU;
    printf "%.2f %d", HEAP_USED/1024, FGC;
}')

if [ -z "$INITIAL_GC_INFO" ]; then
    echo "❌ 无法获取进程 ${PID} 的GC信息，请确保是Java进程"
    exit 1
fi

INITIAL_HEAP_MB=$(echo $INITIAL_GC_INFO | awk '{print $1}')
INITIAL_FGC=$(echo $INITIAL_GC_INFO | awk '{print $2}')

# 打印表头（仅一次）
printf "%-12s %-14s %-18s %-20s %-12s\n" "运行时间(秒)" "CPU使用率(%)" "堆内存(MB)" "堆内存增长速率(MB/s)" "FULL GC次数"
printf "%-12s %-14s %-18s %-20s %-12s\n" "------------" "--------------" "------------------" "----------------------" "------------"

# 监控循环
while true; do
    # 计算运行时间
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    
    # 获取CPU使用率
    CPU=$(ps -p ${PID} -o %cpu --no-headers 2>/dev/null | tr -d ' ')
    if [ -z "$CPU" ]; then
        CPU="0.0"
    fi
    
    # 获取堆内存使用量(MB)和FULL GC次数
    GC_INFO=$(jstat -gc ${PID} 2>/dev/null | awk 'NR==2 {
        S0U=$3; S1U=$4; EU=$5; OU=$6;
        FGC=$17;
        HEAP_USED = S0U + S1U + EU + OU;
        printf "%.2f %d", HEAP_USED/1024, FGC;
    }')
    
    if [ -n "$GC_INFO" ]; then
        HEAP_MB=$(echo $GC_INFO | awk '{print $1}')
        FGC=$(echo $GC_INFO | awk '{print $2}')
        
        # 计算堆内存增长速率 (MB/秒)
        if [ "$ELAPSED" -gt 0 ]; then
            HEAP_GROWTH=$(echo "$HEAP_MB $INITIAL_HEAP_MB $ELAPSED" | awk '{printf "%.4f", ($1 - $2) / $3}')
        else
            HEAP_GROWTH="0.0000"
        fi
        
        # 输出结果
        printf "%-12d %-14s %-18.2f %-20.4f %-12d\n" "$ELAPSED" "$CPU" "$HEAP_MB" "$HEAP_GROWTH" "$FGC"
    else
        # 如果无法获取GC信息，可能进程已退出
        printf "%-12d %-14s %-18s %-20s %-12s\n" "$ELAPSED" "$CPU" "N/A" "N/A" "N/A"
    fi
    
    # 检查进程是否还存在
    if ! ps -p ${PID} > /dev/null 2>&1; then
        echo ""
        echo "⚠️  进程 ${PID} 已退出"
        break
    fi
    
    sleep ${INTERVAL}
done

