#!/bin/bash
#
# 停止 HTTP 直连版 Agents
#

echo "🛑 停止 HTTP Agents..."
pkill -f "http-weather-agent" 2>/dev/null || true
pkill -f "http-travel-agent" 2>/dev/null || true
sleep 1

echo "✅ HTTP Agents 已停止"

