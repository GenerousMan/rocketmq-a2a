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
package agent;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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
import org.apache.rocketmq.a2a.transport.RocketMQTransport;
import org.apache.rocketmq.a2a.transport.RocketMQTransportConfig;
import org.apache.rocketmq.shaded.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

public class SupervisorAgentA2ASDKMainStream {
    private static final Logger log = LoggerFactory.getLogger(SupervisorAgentA2ASDKMainStream.class);
    private static final String AGENT_NAME = "SupervisorAgent";
    private static final String WEATHER_AGENT_NAME = "WeatherAgent";
    private static final String WEATHER_AGENT_URL = "http://localhost:8080";
    private static final String TRAVEL_AGENT_NAME = "TravelAgent";
    private static final String TRAVEL_AGENT_URL = "http://localhost:8888";
    private static final String WORK_AGENT_RESPONSE_TOPIC = System.getProperty("workAgentResponseTopic");
    private static final String WORK_AGENT_RESPONSE_GROUP_ID = System.getProperty("workAgentResponseGroupID");
    private static final String ROCKETMQ_NAMESPACE = System.getProperty("rocketMQNamespace");
    private static final String ACCESS_KEY = System.getProperty("rocketMQAK");
    private static final String SECRET_KEY = System.getProperty("rocketMQSK");
    private static final String YOU = "You";
    private static final String AGENT = "Agent";
    private static final String KEYWORD_WEATHER = "天气";
    private static final String KEYWORD_TRAVEL = "行程";
    private static final Map<String, Client> AgentClientMap = new HashMap<>();

    public static void main(String[] args) {
        if (!checkConfigParam()) {
            System.out.println("配置参数不完整，请检查参数配置情况");
            return;
        }
        printSystemInfo("🚀 启动 " + AGENT_NAME + "，根据关键字匹配处理天气问题与行程安排规划问题，在本例中使用RocketMQ LiteTopic版本实现多个Agent之间的通讯");
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
                if (StringUtils.isEmpty(userInput)) {
                    printSystemInfo("请不要输入空值");
                    continue;
                }
                log.info("用户输入: {}", userInput);
                processUserInput(userInput);
            }
        }
    }

    private static boolean checkConfigParam() {
        if (StringUtils.isEmpty(WORK_AGENT_RESPONSE_TOPIC) || StringUtils.isEmpty(WORK_AGENT_RESPONSE_GROUP_ID)) {
            if (StringUtils.isEmpty(WORK_AGENT_RESPONSE_TOPIC)) {
                System.out.println("请配置RocketMQ 的轻量消息Topic workAgentResponseTopic");
            }
            if (StringUtils.isEmpty(WORK_AGENT_RESPONSE_GROUP_ID)) {
                System.out.println("请配置RocketMQ 的轻量消息消费者 workAgentResponseGroupID");
            }
            return false;
        }
        return true;
    }

    private static void processUserInput(String userInput) {
        boolean hasWeather = userInput.contains(KEYWORD_WEATHER);
        boolean hasTravel = userInput.contains(KEYWORD_TRAVEL);
        
        if (!hasWeather && !hasTravel) {
            printPrompt(AGENT);
            System.out.println("未检测到'天气'或'行程'关键字，无法处理该请求。");
            return;
        }
        
        printPrompt(AGENT);
        if (hasWeather && hasTravel) {
            System.out.println(AGENT_NAME + " 检测到天气和行程请求，将转发到 " + WEATHER_AGENT_NAME + " 和 " + TRAVEL_AGENT_NAME);
            sendMessageToAgent(WEATHER_AGENT_NAME, userInput);
            sendMessageToAgent(TRAVEL_AGENT_NAME, userInput);
        } else if (hasWeather) {
            System.out.println(AGENT_NAME + " 检测到天气请求，将转发到 " + WEATHER_AGENT_NAME);
            sendMessageToAgent(WEATHER_AGENT_NAME, userInput);
        } else if (hasTravel) {
            System.out.println(AGENT_NAME + " 检测到行程请求，将转发到 " + TRAVEL_AGENT_NAME);
            sendMessageToAgent(TRAVEL_AGENT_NAME, userInput);
        }
    }

    private static void sendMessageToAgent(String agentName, String message) {
        try {
            Client client = AgentClientMap.get(agentName);
            if (client == null) {
                System.out.println("错误: 未找到 " + agentName + " 的客户端");
                return;
            }
            client.sendMessage(A2A.toUserMessage(message));
            System.out.println("已发送消息到 " + agentName + ": " + message);
        } catch (Exception e) {
            System.out.println("发送消息到 " + agentName + " 时出错: " + e.getMessage());
            log.error("Error sending message to " + agentName, e);
        }
    }


    private static void initAgentCardInfo(String accessKey, String secretKey, String agentName, String agentUrl) {
        if (StringUtils.isEmpty(agentName) || StringUtils.isEmpty(agentUrl)) {
            System.out.println("initAgentCardInfo param error");
            return;
        }
        AgentCard finalAgentCard = new A2ACardResolver(agentUrl).getAgentCard();
        System.out.println("Successfully fetched public agent card: " + finalAgentCard.description());
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
                if (!CollectionUtils.isEmpty(artifacts)) {
                    TaskState state = task.getStatus().state();
                    System.out.print(extractTextFromMessage(artifacts.get(artifacts.size() - 1)));
                    if (state == TaskState.COMPLETED) {
                        StringBuilder stringBuilder = new StringBuilder();
                        for (Artifact tempArtifact : artifacts) {
                            stringBuilder.append(extractTextFromMessage(tempArtifact));
                        }
                        dealAgentResponse(stringBuilder.toString(), agentName);
                    }
                }
            }
        });
        // Create error handler for streaming errors
        Consumer<Throwable> streamingErrorHandler = (error) -> {
            System.err.println("Streaming error occurred: " + error.getMessage());
        };
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
        System.out.println("init success");
    }

    private static String extractTextFromMessage(Artifact artifact) {
        if (null == artifact) {
            return "";
        }
        List<io.a2a.spec.Part<?>> parts = artifact.parts();
        if (CollectionUtils.isEmpty(parts)) {
            return "";
        }
        StringBuilder textBuilder = new StringBuilder();
        for (io.a2a.spec.Part part : parts) {
            if (part instanceof TextPart textPart) {
                textBuilder.append(textPart.getText());
            }
        }
        return textBuilder.toString();
    }

    private static void dealAgentResponse(String result, String agentName) {
        if (StringUtils.isEmpty(result)) {
            return;
        }
        printPrompt(AGENT);
        System.out.println("收到 " + agentName + " 的响应: " + result);
        printPrompt(YOU);
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
