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
 * distributed on an "AS IS" BASIS,
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import io.a2a.A2A;
import io.a2a.client.Client;
import io.a2a.client.ClientEvent;
import io.a2a.client.TaskUpdateEvent;
import io.a2a.client.http.A2ACardResolver;
import io.a2a.client.transport.jsonrpc.JSONRPCTransport;
import io.a2a.client.transport.jsonrpc.JSONRPCTransportConfig;
import io.a2a.spec.AgentCard;
import io.a2a.spec.Artifact;
import io.a2a.spec.Task;
import io.a2a.spec.TaskState;
import io.a2a.spec.TextPart;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SupervisorAgent {
    private static final Logger log = LoggerFactory.getLogger(SupervisorAgent.class);
    private static final String WEATHER_AGENT_NAME = "WeatherAgent";
    private static final String WEATHER_AGENT_URL = System.getProperty("weatherAgentUrl", "http://localhost:8080");
    private static final String TRAVEL_AGENT_NAME = "TravelAgent";
    private static final String TRAVEL_AGENT_URL = System.getProperty("travelAgentUrl", "http://localhost:8888");
    
    private static final Map<String, Client> agentClientMap = new HashMap<>();
    private static final RequestStatistics statistics = new RequestStatistics();

    public static void main(String[] args) {
        printSystemInfo("🚀 启动 SupervisorAgent，使用纯 A2A 协议（JSONRPC）实现 Agent 间交互");
        
        // 初始化 Agent 客户端
        initAgentClient(WEATHER_AGENT_NAME, WEATHER_AGENT_URL);
        initAgentClient(TRAVEL_AGENT_NAME, TRAVEL_AGENT_URL);
        
        printSystemInfo("💡 输入 'quit' 退出，输入 'stats' 查看统计信息，输入 'help' 查看帮助");
        
        try (Scanner scanner = new Scanner(System.in, String.valueOf(StandardCharsets.UTF_8))) {
            while (true) {
                printPrompt("You");
                String userInput = scanner.nextLine().trim();
                
                if ("quit".equalsIgnoreCase(userInput)) {
                    printSystemInfo("👋 再见！");
                    printStatistics();
                    System.exit(0);
                    break;
                }
                
                if ("help".equalsIgnoreCase(userInput)) {
                    printHelp();
                    continue;
                }
                
                if ("stats".equalsIgnoreCase(userInput)) {
                    printStatistics();
                    continue;
                }
                
                if (StringUtils.isEmpty(userInput)) {
                    printSystemInfo("请不要输入空值");
                    continue;
                }
                
                // 根据关键词路由到对应的 Agent
                String agentName = routeToAgent(userInput);
                if (agentName == null) {
                    printSystemInfo("无法识别请求类型，请包含'天气'或'行程'相关关键词");
                    continue;
                }
                
                // 发送请求并处理响应
                sendRequestAndHandleResponse(agentName, userInput);
            }
        }
    }

    private static void initAgentClient(String agentName, String agentUrl) {
        try {
            AgentCard agentCard = new A2ACardResolver(agentUrl).getAgentCard();
            log.info("Successfully fetched agent card for {}: {}", agentName, agentCard.description());
            
            List<BiConsumer<ClientEvent, AgentCard>> consumers = new ArrayList<>();
            consumers.add((event, card) -> {
                if (event instanceof TaskUpdateEvent taskUpdateEvent) {
                    Task task = taskUpdateEvent.getTask();
                    if (task == null) {
                        return;
                    }
                    
                    List<Artifact> artifacts = task.getArtifacts();
                    if (artifacts != null && !artifacts.isEmpty()) {
                        TaskState state = task.getStatus().state();
                        String responseText = extractTextFromArtifacts(artifacts);
                        
                        // 使用 task 的 toString 或生成一个唯一 ID 来跟踪
                        String taskId = java.util.UUID.randomUUID().toString();
                        if (state == TaskState.COMPLETED) {
                            statistics.recordSuccess(agentName, taskId);
                            printPrompt("Agent");
                            System.out.println(agentName + " 响应: " + responseText);
                        } else if (state == TaskState.FAILED) {
                            statistics.recordFailure(agentName, taskId);
                            printPrompt("Agent");
                            System.out.println(agentName + " 处理失败");
                        }
                    }
                }
            });
            
            // 创建错误处理器
            Consumer<Throwable> streamingErrorHandler = (error) -> {
                // 忽略流取消错误，这是正常的
                // 当服务端支持流式传输但客户端使用非流式方法时，流会在完成后被取消
                if (error != null) {
                    String errorMsg = error.getMessage();
                    String errorClass = error.getClass().getSimpleName();
                    
                    // 检查是否是流取消相关的错误
                    if (errorMsg != null && (
                        errorMsg.contains("cancelled") || 
                        errorMsg.contains("Stream") && errorMsg.contains("cancelled") ||
                        errorMsg.contains("Stream 1 cancelled") ||
                        errorMsg.contains("Stream cancelled")
                    )) {
                        log.debug("Stream cancelled (normal behavior): {}", errorMsg);
                        return;
                    }
                    
                    // 检查异常类型
                    if (errorClass.contains("Cancellation") || 
                        (errorMsg != null && errorMsg.toLowerCase().contains("cancel"))) {
                        log.debug("Stream cancellation (normal): {}", errorMsg);
                        return;
                    }
                }
                
                // 其他错误才真正记录
                log.error("Streaming error occurred: {}", error != null ? error.getMessage() : "Unknown error", error);
                printSystemError("❌ 流式传输错误: " + (error != null ? error.getMessage() : "Unknown error"));
            };
            
            // 创建 JSONRPC 传输配置
            JSONRPCTransportConfig jsonrpcConfig = new JSONRPCTransportConfig();
            
            // 使用 JSONRPC 传输
            Client client = Client.builder(agentCard)
                    .addConsumers(consumers)
                    .streamingErrorHandler(streamingErrorHandler)
                    .withTransport(JSONRPCTransport.class, jsonrpcConfig)
                    .build();
            
            agentClientMap.put(agentName, client);
            printSystemSuccess("✅ " + agentName + " 客户端初始化成功");
        } catch (Exception e) {
            log.error("Failed to initialize agent client for {}: {}", agentName, e.getMessage(), e);
            printSystemError("❌ " + agentName + " 客户端初始化失败: " + e.getMessage());
        }
    }

    private static String routeToAgent(String userInput) {
        String lowerInput = userInput.toLowerCase();
        if (lowerInput.contains("天气") || lowerInput.contains("weather")) {
            return WEATHER_AGENT_NAME;
        } else if (lowerInput.contains("行程") || lowerInput.contains("travel") || 
                   lowerInput.contains("规划") || lowerInput.contains("plan")) {
            return TRAVEL_AGENT_NAME;
        }
        return null;
    }

    private static void sendRequestAndHandleResponse(String agentName, String userInput) {
        Client client = agentClientMap.get(agentName);
        if (client == null) {
            printSystemError("❌ " + agentName + " 客户端未初始化");
            return;
        }
        
        String taskId = statistics.startRequest(agentName);
        printSystemInfo("📤 发送请求到 " + agentName + ": " + userInput);
        
        try {
            // 使用我们自己的 taskId 来跟踪请求
            io.a2a.spec.Message message = A2A.toUserMessage(userInput);
            // 如果 message 支持设置 taskId，我们可以设置它
            client.sendMessage(message);
        } catch (Exception e) {
            log.error("Failed to send message to {}: {}", agentName, e.getMessage(), e);
            statistics.recordFailure(agentName, taskId);
            printSystemError("❌ 发送请求失败: " + e.getMessage());
        }
    }

    private static String extractTextFromArtifacts(List<Artifact> artifacts) {
        if (artifacts == null || artifacts.isEmpty()) {
            return "";
        }
        StringBuilder textBuilder = new StringBuilder();
        for (Artifact artifact : artifacts) {
            List<io.a2a.spec.Part<?>> parts = artifact.parts();
            if (parts != null) {
                for (io.a2a.spec.Part part : parts) {
                    if (part instanceof TextPart textPart) {
                        textBuilder.append(textPart.getText());
                    }
                }
            }
        }
        return textBuilder.toString();
    }

    private static void printSystemInfo(String message) {
        System.out.println("\u001B[34m[SYSTEM] " + message + "\u001B[0m");
        log.info(message);
    }

    private static void printSystemSuccess(String message) {
        System.out.println("\u001B[32m[SUCCESS] " + message + "\u001B[0m");
        log.info(message);
    }

    private static void printSystemError(String message) {
        System.out.println("\u001B[31m[ERROR] " + message + "\u001B[0m");
        log.error(message);
    }

    private static void printPrompt(String role) {
        System.out.print("\n\u001B[36m" + role + " > \u001B[0m");
    }

    private static void printHelp() {
        System.out.println("\n\u001B[35m📖 帮助信息:\u001B[0m");
        System.out.println("  • 询问天气: '杭州明天的天气情况怎么样'");
        System.out.println("  • 安排行程: '帮我做一个明天杭州周边自驾游方案'");
        System.out.println("  • 查看统计: 'stats'");
        System.out.println("  • 退出程序: 'quit'");
        System.out.println("  • 显示帮助: 'help'");
    }

    private static void printStatistics() {
        System.out.println("\n\u001B[33m📊 请求统计信息:\u001B[0m");
        statistics.printStatistics();
    }

    // 请求统计类
    static class RequestStatistics {
        private final Map<String, AtomicLong> successCount = new HashMap<>();
        private final Map<String, AtomicLong> failureCount = new HashMap<>();
        private final Map<String, List<Long>> requestDurations = new HashMap<>();
        private final Map<String, Map<String, Long>> requestStartTimes = new HashMap<>();

        public String startRequest(String agentName) {
            String taskId = java.util.UUID.randomUUID().toString();
            requestStartTimes.computeIfAbsent(agentName, k -> new HashMap<>())
                    .put(taskId, System.currentTimeMillis());
            return taskId;
        }

        public void recordSuccess(String agentName, String taskId) {
            successCount.computeIfAbsent(agentName, k -> new AtomicLong(0)).incrementAndGet();
            recordDuration(agentName, taskId);
        }

        public void recordFailure(String agentName, String taskId) {
            failureCount.computeIfAbsent(agentName, k -> new AtomicLong(0)).incrementAndGet();
            recordDuration(agentName, taskId);
        }

        private void recordDuration(String agentName, String taskId) {
            Map<String, Long> startTimes = requestStartTimes.get(agentName);
            if (startTimes != null) {
                Long startTime = startTimes.remove(taskId);
                if (startTime != null) {
                    long duration = System.currentTimeMillis() - startTime;
                    requestDurations.computeIfAbsent(agentName, k -> new ArrayList<>()).add(duration);
                }
            }
        }

        public void printStatistics() {
            for (String agentName : new java.util.HashSet<>(successCount.keySet())) {
                long success = successCount.getOrDefault(agentName, new AtomicLong(0)).get();
                long failure = failureCount.getOrDefault(agentName, new AtomicLong(0)).get();
                long total = success + failure;
                double successRate = total > 0 ? (double) success / total * 100 : 0.0;
                
                List<Long> durations = requestDurations.getOrDefault(agentName, new ArrayList<>());
                double avgDuration = durations.isEmpty() ? 0 : 
                    durations.stream().mapToLong(Long::longValue).average().orElse(0.0);
                long minDuration = durations.isEmpty() ? 0 : 
                    durations.stream().mapToLong(Long::longValue).min().orElse(0);
                long maxDuration = durations.isEmpty() ? 0 : 
                    durations.stream().mapToLong(Long::longValue).max().orElse(0);
                
                System.out.println("  " + agentName + ":");
                System.out.println("    总请求数: " + total);
                System.out.println("    成功数: " + success);
                System.out.println("    失败数: " + failure);
                System.out.println("    成功率: " + String.format("%.2f%%", successRate));
                System.out.println("    平均耗时: " + String.format("%.2f", avgDuration) + " ms");
                System.out.println("    最小耗时: " + minDuration + " ms");
                System.out.println("    最大耗时: " + maxDuration + " ms");
            }
        }
    }

}

