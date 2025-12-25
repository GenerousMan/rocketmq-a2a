#!/bin/bash
#
# 停止 RocketMQ 版 Agents
#

echo "🛑 停止 RocketMQ Agents..."
pkill -f "quarkus:dev" 2>/dev/null || true
sleep 2

echo "✅ RocketMQ Agents 已停止"

