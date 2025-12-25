/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.benchmark.rocketmq.travel;

import java.util.Collections;
import java.util.List;

import io.a2a.server.PublicAgentCard;
import io.a2a.spec.AgentCapabilities;
import io.a2a.spec.AgentCard;
import io.a2a.spec.AgentSkill;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import static org.apache.rocketmq.a2a.common.RocketMQA2AConstant.ROCKETMQ_PROTOCOL;

/**
 * 行程Agent Card配置 - 描述Agent的能力和技能 (Mock版)
 */
@ApplicationScoped
public class AgentCardProducer {
    
    private static final String ROCKETMQ_ENDPOINT = System.getProperty("rocketMQEndpoint", "");
    private static final String ROCKETMQ_NAMESPACE = System.getProperty("rocketMQNamespace", "");
    private static final String BIZ_TOPIC = System.getProperty("bizTopic", "TravelAgentTask");

    @Produces
    @PublicAgentCard
    public AgentCard agentCard() {
        return new AgentCard.Builder()
            .name("TravelAgent")
            .description("行程规划助手 (Mock版)")
            .url(buildRocketMQUrl())
            .version("1.0.0")
            .documentationUrl("http://example.com/docs")
            .capabilities(new AgentCapabilities.Builder()
                .streaming(true)
                .pushNotifications(true)
                .stateTransitionHistory(true)
                .build())
            .defaultInputModes(Collections.singletonList("text"))
            .defaultOutputModes(Collections.singletonList("text"))
            .skills(Collections.singletonList(new AgentSkill.Builder()
                .id("travel_planning")
                .name("行程规划")
                .description("制定详细的旅行计划")
                .tags(List.of("行程", "规划", "旅游", "出行"))
                .examples(List.of("帮我规划杭州到上海的2天行程"))
                .build()))
            .preferredTransport(ROCKETMQ_PROTOCOL)
            .protocolVersion("0.3.0")
            .build();
    }

    private static String buildRocketMQUrl() {
        if (ROCKETMQ_ENDPOINT.isEmpty() || BIZ_TOPIC.isEmpty()) {
            // 返回默认HTTP URL用于本地测试
            return "http://localhost:8888";
        }
        return "http://" + ROCKETMQ_ENDPOINT + "/" + ROCKETMQ_NAMESPACE + "/" + BIZ_TOPIC;
    }
}
