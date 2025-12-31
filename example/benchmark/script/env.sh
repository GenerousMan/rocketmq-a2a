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

# RocketMQ 配置
export ROCKETMQ_NAMESPACE="rmq-cn-2ml4l0cyu05"
export ROCKETMQ_AK="159iBRyW0k7Qjl6D"
export ROCKETMQ_SK="42QASgxip3LMQy50"
export ROCKETMQ_ENDPOINT="rmq-cn-2ml4l0cyu05.cn-hangzhou.rmq.aliyuncs.com:8080"
export WORK_AGENT_RESPONSE_TOPIC="WorkerAgentResponse"
export WORK_AGENT_RESPONSE_GROUP_ID="CID_HOST_AGENT_LITE"

# Agent 业务 Topic 配置
export WEATHER_AGENT_BIZ_TOPIC="WeatherAgentTask"
export WEATHER_AGENT_BIZ_CONSUMER_GROUP="WeatherAgentTaskConsumerGroup"
export TRAVEL_AGENT_BIZ_TOPIC="TravelAgentTask"
export TRAVEL_AGENT_BIZ_CONSUMER_GROUP="TravelAgentTaskConsumerGroup"

# API Key 配置
export API_KEY="your_api_key"

# 项目路径
export SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export BENCHMARK_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
export PROJECT_ROOT="$(cd "${BENCHMARK_ROOT}/../.." && pwd)"

echo "✅ 环境变量已加载"
echo "   - RocketMQ Namespace: ${ROCKETMQ_NAMESPACE}"
echo "   - RocketMQ Topic: ${WORK_AGENT_RESPONSE_TOPIC}"
echo "   - RocketMQ GroupID: ${WORK_AGENT_RESPONSE_GROUP_ID}"
echo "   - Project Root: ${PROJECT_ROOT}"
echo "   - Benchmark Root: ${BENCHMARK_ROOT}"
echo "   - Script Dir: ${SCRIPT_DIR}"
