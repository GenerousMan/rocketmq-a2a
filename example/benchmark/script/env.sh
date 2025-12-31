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
export ROCKETMQ_NAMESPACE="your_namespace"
export ROCKETMQ_AK="your_access_key"
export ROCKETMQ_SK="your_secret_key"
export WORK_AGENT_RESPONSE_TOPIC="WorkerAgentResponse"
export WORK_AGENT_RESPONSE_GROUP_ID="CID_HOST_AGENT_LITE"

# API Key 配置
export API_KEY="your_api_key"

# 项目路径
export PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export BENCHMARK_ROOT="${PROJECT_ROOT}/example/benchmark/rocketmq-a2a"

echo "✅ 环境变量已加载"
echo "   - RocketMQ Namespace: ${ROCKETMQ_NAMESPACE}"
echo "   - RocketMQ Topic: ${WORK_AGENT_RESPONSE_TOPIC}"
echo "   - RocketMQ GroupID: ${WORK_AGENT_RESPONSE_GROUP_ID}"
echo "   - Project Root: ${PROJECT_ROOT}"
