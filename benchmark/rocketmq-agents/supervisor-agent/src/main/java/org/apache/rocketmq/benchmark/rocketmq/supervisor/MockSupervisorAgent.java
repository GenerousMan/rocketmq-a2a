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
package org.apache.rocketmq.benchmark.rocketmq.supervisor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.alibaba.fastjson.JSON;
import com.google.adk.agents.BaseAgent;
import com.google.adk.agents.LlmAgent;
import com.google.adk.artifacts.InMemoryArtifactService;
import com.google.adk.events.Event;
import com.google.adk.runner.Runner;
import com.google.adk.sessions.InMemorySessionService;
import com.google.adk.sessions.Session;
import com.google.common.collect.ImmutableList;
import com.google.genai.types.Content;
import com.google.genai.types.Part;
import io.a2a.A2A;
import io.a2a.client.Client;
import io.a2a.client.ClientEvent;
import io.a2a.client.TaskUpdateEvent;
import io.a2a.client.http.A2ACardResolver;
import io.a2a.spec.AgentCard;
import io.a2a.spec.Artifact;
import io.a2a.spec.Task;
import io.a2a.spec.TaskState;
import io.a2a.spec.TextPart;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import org.apache.rocketmq.a2a.transport.RocketMQTransport;
import org.apache.rocketmq.a2a.transport.RocketMQTransportConfig;
import org.apache.rocketmq.benchmark.common.Mission;
import org.apache.rocketmq.benchmark.common.MockLlmModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mock版SupervisorAgent - 仅Mock LLM调用，保留所有RocketMQ交互
 * 
 * <p>与原版SupervisorAgent完全相同的RocketMQ通信逻辑，
 * 唯一区别是使用MockLlmModel替换真实的QWModel。
 * 
 * <p>配置参数：
 * <ul>
 *   <li>rocketMQNamespace - RocketMQ命名空间</li>
 *   <li>workAgentResponseTopic - LiteTopic名称</li>
 *   <li>workAgentResponseGroupID - Lite消费者ID</li>
 *   <li>rocketMQAK - RocketMQ AccessKey</li>
 *   <li>rocketMQSK - RocketMQ SecretKey</li>
 *   <li>mockDelayMs - Mock延迟时间</li>
 *   <li>mockSupervisorMode - 响应模式(AUTO/WEATHER_DISPATCH/TRAVEL_DISPATCH/DIRECT)</li>
 * </ul>
 */
public class MockSupervisorAgent {
    
    private static final Logger log = LoggerFactory.getLogger(MockSupervisorAgent.class);
    
    private static final String AGENT_NAME = "SupervisorAgent";
    private static final String USER_ID = "benchmark_user";
    private static final String APP_NAME = "benchmark_a2a";
    private static final String WEATHER_AGENT_NAME = "WeatherAgent";
    private static final String WEATHER_AGENT_URL = "http://localhost:8080";
    private static final String TRAVEL_AGENT_NAME = "TravelAgent";
    private static final String TRAVEL_AGENT_URL = "http://localhost:8888";
    
    // RocketMQ配置 - 从系统属性读取
    private static final String WORK_AGENT_RESPONSE_TOPIC = System.getProperty("workAgentResponseTopic");
    private static final String WORK_AGENT_RESPONSE_GROUP_ID = System.getProperty("workAgentResponseGroupID");
    private static final String ROCKETMQ_NAMESPACE = System.getProperty("rocketMQNamespace");
    private static final String ACCESS_KEY = System.getProperty("rocketMQAK");
    private static final String SECRET_KEY = System.getProperty("rocketMQSK");
    
    private static final String YOU = "You";
    private static final String AGENT = "Agent";
    
    private static String lastQuestion = "";
    private static InMemorySessionService sessionService;
    private static final Map<String, Client> AgentClientMap = new HashMap<>();
    private static String sessionId;
    private static Runner runner;

    public static void main(String[] args) {
        if (!checkConfigParam()) {
            log.error("配置参数不完整，请检查参数配置情况");
            return;
        }
        
        // 唯一的Mock点：使用MockLlmModel替换QWModel
        BaseAgent baseAgent = initAgentWithMockLlm(WEATHER_AGENT_NAME, TRAVEL_AGENT_NAME);
        
        printSystemInfo("🚀 启动 Mock版 " + AGENT_NAME + " (LLM已Mock，RocketMQ通信保持原样)");
        printSystemInfo("📋 初始化会话...");
        
        InMemoryArtifactService artifactService = new InMemoryArtifactService();
        sessionService = new InMemorySessionService();
        runner = new Runner(baseAgent, APP_NAME, artifactService, sessionService, null);
        
        Session session = runner
            .sessionService()
            .createSession(APP_NAME, USER_ID)
            .blockingGet();
        
        printSystemSuccess("✅ 会话创建成功: " + session.id());
        sessionId = session.id();
        
        // 保留原有的RocketMQ Client初始化逻辑
        initAgentCardInfo(ACCESS_KEY, SECRET_KEY, WEATHER_AGENT_NAME, WEATHER_AGENT_URL);
        initAgentCardInfo(ACCESS_KEY, SECRET_KEY, TRAVEL_AGENT_NAME, TRAVEL_AGENT_URL);
        
        printSystemInfo("💡 输入 'quit' 退出，输入 'help' 查看帮助");
        
        try (Scanner scanner = new Scanner(System.in, String.valueOf(StandardCharsets.UTF_8))) {
            while (true) {
                printPrompt(YOU);
                String userInput = scanner.nextLine().trim();
                
                if ("quit".equalsIgnoreCase(userInput)) {
                    printSystemInfo("👋 再见！");
                    System.exit(0);
                    break;
                }
                if ("help".equalsIgnoreCase(userInput)) {
                    printHelp();
                    continue;
                }
                if (userInput.isEmpty()) {
                    printSystemInfo("请不要输入空值");
                    continue;
                }
                
                printSystemInfo("🤔 正在思考...");
                log.info("用户输入: {}", userInput);
                
                Content userMsg = Content.fromParts(Part.fromText(userInput));
                Flowable<Event> events = runner.runAsync(USER_ID, session.id(), userMsg);
                
                events.blockingForEach(event -> {
                    String content = event.stringifyContent();
                    dealEventContent(content);
                });
            }
        }
    }

    private static boolean checkConfigParam() {
        if (isEmpty(WORK_AGENT_RESPONSE_TOPIC) || isEmpty(WORK_AGENT_RESPONSE_GROUP_ID)) {
            if (isEmpty(WORK_AGENT_RESPONSE_TOPIC)) {
                log.error("请配置RocketMQ的轻量消息Topic workAgentResponseTopic");
            }
            if (isEmpty(WORK_AGENT_RESPONSE_GROUP_ID)) {
                log.error("请配置RocketMQ的轻量消息消费者 workAgentResponseGroupID");
            }
            return false;
        }
        return true;
    }
    
    private static boolean isEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    /**
     * 使用MockLlmModel初始化Agent - 这是唯一被Mock的地方
     * 原版使用QWModelRegistry.getModel(API_KEY)获取真实模型
     */
    private static BaseAgent initAgentWithMockLlm(String weatherAgent, String travelAgent) {
        if (isEmpty(weatherAgent) || isEmpty(travelAgent)) {
            log.error("initAgent 参数缺失");
            return null;
        }
        
        // 唯一的Mock点：使用MockLlmModel替换QWModel
        MockLlmModel mockModel = MockLlmModel.fromSystemProperties();
        log.info("[Mock] 使用MockLlmModel替换真实LLM，mode={}", 
            System.getProperty("mockSupervisorMode", "AUTO"));
        
        return LlmAgent.builder()
            .name(APP_NAME)
            .model(mockModel)  // 使用Mock模型
            .description("你是一位专业的行程规划专家")
            .instruction("# 角色\n"
                + "你是一位专业的行程规划专家，擅长任务分解与协调安排。\n"
                + "## 技能\n"
                + "### 技能 1: 收集必要信息\n"
                + "- 询问用户关于目的地、出行时间\n"
                + "### 技能 2: 查询天气信息\n"
                + "- 使用" + weatherAgent + "工具查询目的地的天气情况\n"
                + "- 示例: {\"messageInfo\":\"杭州下周三的天气?\",\"agent\":\"" + weatherAgent + "\"}\n"
                + "### 技能 3: 制定行程规划\n"
                + "- 使用" + travelAgent + "工具制定详细的行程规划\n"
                + "- 示例: {\"messageInfo\":\"杭州到上海2天行程规划\",\"agent\":\"" + travelAgent + "\"}\n"
                + "## 限制\n"
                + "- 只处理与行程安排相关的问题。"
            )
            .build();
    }

    private static void dealEventContent(String content) {
        if (isEmpty(content)) {
            return;
        }
        if (content.startsWith("{")) {
            try {
                Mission mission = JSON.parseObject(content, Mission.class);
                if (null != mission) {
                    printPrompt(AGENT);
                    System.out.println(AGENT_NAME + " 转发请求到其他的Agent, Agent: " 
                        + mission.getAgent() + " 问题: " + mission.getMessageInfo());
                    dealMissionByMessage(mission);
                }
            } catch (Exception e) {
                log.error("解析过程出现异常", e);
            }
        } else {
            printPrompt(AGENT);
            System.out.println(content);
        }
    }

    private static void dealMissionByMessage(Mission mission) {
        if (null == mission || isEmpty(mission.getAgent()) || isEmpty(mission.getMessageInfo())) {
            return;
        }
        try {
            String agentName = mission.getAgent().replaceAll(" ", "");
            Client client = AgentClientMap.get(agentName);
            if (client != null) {
                client.sendMessage(A2A.toUserMessage(mission.getMessageInfo()));
                log.info("Sending message to {}: {}", agentName, mission.getMessageInfo());
            } else {
                log.warn("Agent client not found: {}", agentName);
            }
        } catch (Exception e) {
            log.error("发送消息失败: {}", e.getMessage());
        }
    }

    /**
     * 初始化Agent Card和RocketMQ Client - 保持原有逻辑不变
     */
    private static void initAgentCardInfo(String accessKey, String secretKey, 
            String agentName, String agentUrl) {
        if (isEmpty(agentName) || isEmpty(agentUrl)) {
            log.error("initAgentCardInfo param error");
            return;
        }
        
        AgentCard finalAgentCard = new A2ACardResolver(agentUrl).getAgentCard();
        log.info("Successfully fetched agent card: {}", finalAgentCard.description());
        
        List<BiConsumer<ClientEvent, AgentCard>> consumers = new ArrayList<>();
        consumers.add((event, agentCard) -> {
            if (event instanceof TaskUpdateEvent taskUpdateEvent) {
                Task task = taskUpdateEvent.getTask();
                if (null == task) {
                    return;
                }
                List<Artifact> artifacts = task.getArtifacts();
                if (null != artifacts && artifacts.size() == 1) {
                    printPrompt(AGENT);
                }
                if (artifacts != null && !artifacts.isEmpty()) {
                    TaskState state = task.getStatus().state();
                    System.out.print(extractTextFromMessage(artifacts.get(artifacts.size() - 1)));
                    if (state == TaskState.COMPLETED) {
                        StringBuilder stringBuilder = new StringBuilder();
                        for (Artifact tempArtifact : artifacts) {
                            stringBuilder.append(extractTextFromMessage(tempArtifact));
                        }
                        dealAgentResponse(stringBuilder.toString());
                    }
                }
            }
        });
        
        Consumer<Throwable> streamingErrorHandler = (error) -> {
            log.error("Streaming error occurred: {}", error.getMessage());
        };
        
        // 保持原有的RocketMQ配置逻辑
        RocketMQTransportConfig rocketMQTransportConfig = new RocketMQTransportConfig();
        rocketMQTransportConfig.setNamespace(ROCKETMQ_NAMESPACE);
        rocketMQTransportConfig.setAccessKey(accessKey);
        rocketMQTransportConfig.setSecretKey(secretKey);
        rocketMQTransportConfig.setWorkAgentResponseGroupID(WORK_AGENT_RESPONSE_GROUP_ID);
        rocketMQTransportConfig.setWorkAgentResponseTopic(WORK_AGENT_RESPONSE_TOPIC);
        
        Client client = Client.builder(finalAgentCard)
            .addConsumers(consumers)
            .streamingErrorHandler(streamingErrorHandler)
            .withTransport(RocketMQTransport.class, rocketMQTransportConfig)
            .build();
        
        AgentClientMap.put(agentName, client);
        log.info("Agent client initialized: {}", agentName);
    }

    private static String extractTextFromMessage(Artifact artifact) {
        if (null == artifact) {
            return "";
        }
        List<io.a2a.spec.Part<?>> parts = artifact.parts();
        if (parts == null || parts.isEmpty()) {
            return "";
        }
        StringBuilder textBuilder = new StringBuilder();
        for (io.a2a.spec.Part<?> part : parts) {
            if (part instanceof TextPart textPart) {
                textBuilder.append(textPart.getText());
            }
        }
        return textBuilder.toString();
    }

    private static void dealAgentResponse(String result) {
        if (isEmpty(result)) {
            return;
        }
        Maybe<Session> sessionMaybe = sessionService.getSession(APP_NAME, USER_ID, sessionId, Optional.empty());
        Event event = Event.builder()
            .id(UUID.randomUUID().toString())
            .invocationId(UUID.randomUUID().toString())
            .author(APP_NAME)
            .content(buildContent(result))
            .build();
        Session session = sessionMaybe.blockingGet();
        sessionService.appendEvent(session, event);
        
        Content userMsg = Content.fromParts(Part.fromText(result));
        Flowable<Event> events = runner.runAsync(USER_ID, session.id(), userMsg);
        
        events.blockingForEach(eventSub -> {
            boolean equals = lastQuestion.equals(eventSub.stringifyContent());
            if (equals) {
                return;
            }
            lastQuestion = eventSub.stringifyContent();
            String content = lastQuestion;
            if (!isEmpty(content) && content.startsWith("{")) {
                try {
                    Mission mission = JSON.parseObject(content, Mission.class);
                    if (null != mission && !isEmpty(mission.getMessageInfo()) && !isEmpty(mission.getAgent())) {
                        printPrompt(AGENT);
                        System.out.println("转发到其他的Agent: " + mission.getAgent());
                        dealMissionByMessage(mission);
                    }
                } catch (Exception e) {
                    log.error("解析过程出现异常", e);
                }
            }
        });
        printPrompt(YOU);
    }

    private static Content buildContent(String content) {
        if (isEmpty(content)) {
            return null;
        }
        return Content.builder()
            .role(APP_NAME)
            .parts(ImmutableList.of(Part.builder().text(content).build()))
            .build();
    }

    private static void printSystemInfo(String message) {
        System.out.println("\u001B[34m[SYSTEM] " + message + "\u001B[0m");
        log.info(message);
    }

    private static void printSystemSuccess(String message) {
        System.out.println("\u001B[32m[SUCCESS] " + message + "\u001B[0m");
        log.info(message);
    }

    private static void printPrompt(String role) {
        System.out.print("\n\u001B[36m" + role + " > \u001B[0m");
    }

    private static void printHelp() {
        System.out.println("\n\u001B[35m📖 帮助信息:\u001B[0m");
        System.out.println("  • 询问天气: '杭州明天的天气情况怎么样'");
        System.out.println("  • 帮忙安排行程: '帮我做一个明天杭州周边自驾游方案'");
        System.out.println("  • 退出程序: 'quit'");
        System.out.println("  • 显示帮助: 'help'");
    }
}

