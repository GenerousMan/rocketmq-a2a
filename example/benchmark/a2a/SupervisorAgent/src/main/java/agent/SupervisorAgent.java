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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    private static final String WEATHER_AGENT_URLS = System.getProperty("weatherAgentUrls", "http://localhost:8080");
    private static final String TRAVEL_AGENT_NAME = "TravelAgent";
    private static final String TRAVEL_AGENT_URLS = System.getProperty("travelAgentUrls", "http://localhost:8888");
    private static final int MAX_RETRY_TIMES = Integer.parseInt(System.getProperty("maxRetryTimes", "3"));
    
    // 测试参数
    private static final String TEST_MESSAGE = System.getProperty("testMessage", "今天天气怎么样？");
    private static final int QPS = Integer.parseInt(System.getProperty("qps", "10"));
    private static final int MAX_TEST_TIME = Integer.parseInt(System.getProperty("maxTestTime", "60")); // 秒
    private static final int STATS_INTERVAL = Integer.parseInt(System.getProperty("statsInterval", "10")); // 秒
    
    // Agent客户端映射 - 支持多副本
    private static final Map<String, List<Client>> agentClientListMap = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> agentRoundRobinIndexMap = new ConcurrentHashMap<>();
    
    // 任务跟踪映射
    private static final Map<String, CompletableFuture<Void>> taskIdToFutureMap = new ConcurrentHashMap<>();
    private static final Map<String, String> taskIdToMessageIdMap = new ConcurrentHashMap<>();
    
    private static final RequestStatistics statistics = new RequestStatistics();
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private static volatile boolean running = true;
    
    // Session ID
    private static final String SESSION_ID = "supervisor_session_" + System.currentTimeMillis();

    public static void main(String[] args) {
        printSystemInfo("🚀 启动 SupervisorAgent，使用纯 A2A 协议（JSONRPC）实现 Agent 间交互");
        printSystemInfo("📋 测试配置:");
        printSystemInfo("  - 测试消息: " + TEST_MESSAGE);
        printSystemInfo("  - QPS: " + QPS);
        printSystemInfo("  - 最大运行时长: " + MAX_TEST_TIME + " 秒");
        printSystemInfo("  - 统计输出间隔: " + STATS_INTERVAL + " 秒");
        printSystemInfo("  - 最大重试次数: " + MAX_RETRY_TIMES);
        
        // 初始化 Agent 客户端（支持多副本）
        initAgentClients(WEATHER_AGENT_NAME, WEATHER_AGENT_URLS);
        initAgentClients(TRAVEL_AGENT_NAME, TRAVEL_AGENT_URLS);
        
        // 检查是否有可用的 Agent
        if (agentClientListMap.isEmpty()) {
            printSystemError("❌ 没有可用的 Agent 客户端，退出");
            System.exit(1);
        }
        
        // 根据消息内容路由到对应的 Agent
        String targetAgent = routeToAgent(TEST_MESSAGE);
        if (targetAgent == null) {
            printSystemError("❌ 无法识别消息类型，请确保消息包含'天气'或'行程'相关关键词");
            System.exit(1);
        }
        
        printSystemInfo("📤 目标 Agent: " + targetAgent);
        printSystemInfo("⏱️  开始压测...");
        
        // 启动定期统计输出
        scheduler.scheduleAtFixedRate(() -> {
            if (running) {
                printStatistics();
            }
        }, STATS_INTERVAL, STATS_INTERVAL, TimeUnit.SECONDS);
        
        // 计算发送间隔（毫秒）
        long intervalMs = 1000L / QPS;
        if (intervalMs < 1) {
            intervalMs = 1;
        }
        
        // 启动固定 QPS 发送
        long startTime = System.currentTimeMillis();
        long endTime = startTime + MAX_TEST_TIME * 1000L;
        AtomicLong requestCounter = new AtomicLong(0);
        
        scheduler.scheduleAtFixedRate(() -> {
            if (!running || System.currentTimeMillis() >= endTime) {
                running = false;
                scheduler.shutdown();
                printSystemInfo("⏹️  测试时间到达，停止发送请求");
                printFinalStatistics();
                System.exit(0);
                return;
            }
            
            try {
                String messageId = "request-" + requestCounter.incrementAndGet();
                sendRequestWithRetry(targetAgent, TEST_MESSAGE, messageId, 0);
            } catch (Exception e) {
                log.error("Failed to send request: {}", e.getMessage(), e);
            }
        }, 0, intervalMs, TimeUnit.MILLISECONDS);
        
        // 等待测试完成
        try {
            Thread.sleep(MAX_TEST_TIME * 1000L + 5000); // 额外等待5秒确保所有响应都收到
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        running = false;
        scheduler.shutdown();
        printFinalStatistics();
    }

    /**
     * 初始化Agent客户端（支持多副本）
     */
    private static void initAgentClients(String agentName, String agentUrls) {
        if (StringUtils.isEmpty(agentName) || StringUtils.isEmpty(agentUrls)) {
            printSystemError("❌ Agent配置参数错误: " + agentName);
            return;
        }
        
        // 解析URL列表，支持逗号分隔
        String[] urls = agentUrls.split(",");
        List<Client> clients = new ArrayList<>();
        
        printSystemInfo("🔧 开始初始化 " + agentName + " 的多副本客户端，总数: " + urls.length);
        
        for (int i = 0; i < urls.length; i++) {
            String url = urls[i].trim();
            if (StringUtils.isEmpty(url)) {
                continue;
            }
            
            try {
                AgentCard agentCard = new A2ACardResolver(url).getAgentCard();
                log.info("Successfully fetched agent card for {} [{}]: {}", agentName, i, agentCard.description());
                
                List<BiConsumer<ClientEvent, AgentCard>> consumers = new ArrayList<>();
                consumers.add((event, card) -> {
                    if (event instanceof TaskUpdateEvent taskUpdateEvent) {
                        Task task = taskUpdateEvent.getTask();
                        if (task == null) {
                            return;
                        }
                        
                        String taskId = task.getId();
                        TaskState state = task.getStatus().state();
                        
                        // 标记消息发送状态
                        CompletableFuture<Void> messageResponse = taskIdToFutureMap.get(taskId);
                        if (messageResponse != null && !messageResponse.isDone()) {
                            if (state == TaskState.COMPLETED) {
                                messageResponse.complete(null);
                            } else if (state == TaskState.FAILED || state == TaskState.CANCELED) {
                                messageResponse.completeExceptionally(new RuntimeException("Task failed: " + state));
                            }
                        }
                        
                        // 处理完成的任务
                        List<Artifact> artifacts = task.getArtifacts();
                        if (artifacts != null && !artifacts.isEmpty() && state == TaskState.COMPLETED) {
                            String responseText = extractTextFromArtifacts(artifacts);
                            
                            // 获取对应的messageId
                            String messageId = taskIdToMessageIdMap.get(taskId);
                            if (messageId != null) {
                                // 验证 taskId 是否在我们的记录中
                                if (statistics.hasPendingRequest(agentName, taskId)) {
                                    statistics.recordSuccess(agentName, taskId);
                                    log.debug("Request {} completed for {}", taskId, agentName);
                                }
                                
                                // 清理映射
                                taskIdToFutureMap.remove(taskId);
                                taskIdToMessageIdMap.remove(taskId);
                            }
                        } else if (state == TaskState.FAILED) {
                            String messageId = taskIdToMessageIdMap.get(taskId);
                            if (messageId != null) {
                                if (statistics.hasPendingRequest(agentName, taskId)) {
                                    statistics.recordFailure(agentName, taskId);
                                    log.debug("Request {} failed for {}", taskId, agentName);
                                }
                                taskIdToFutureMap.remove(taskId);
                                taskIdToMessageIdMap.remove(taskId);
                            }
                        }
                    }
                });
                
                // 创建错误处理器
                Consumer<Throwable> streamingErrorHandler = (error) -> {
                    // 忽略流取消错误，这是正常的
                    if (error != null) {
                        String errorMsg = error.getMessage();
                        String errorClass = error.getClass().getSimpleName();
                        
                        if (errorMsg != null && (
                            errorMsg.contains("cancelled") || 
                            errorMsg.contains("Stream") && errorMsg.contains("cancelled") ||
                            errorMsg.contains("Stream 1 cancelled") ||
                            errorMsg.contains("Stream cancelled")
                        )) {
                            log.debug("Stream cancelled (normal behavior): {}", errorMsg);
                            return;
                        }
                        
                        if (errorClass.contains("Cancellation") || 
                            (errorMsg != null && errorMsg.toLowerCase().contains("cancel"))) {
                            log.debug("Stream cancellation (normal): {}", errorMsg);
                            return;
                        }
                    }
                    
                    log.error("Streaming error occurred for {} [{}]: {}", agentName, url, 
                        error != null ? error.getMessage() : "Unknown error", error);
                };
                
                // 创建 JSONRPC 传输配置
                JSONRPCTransportConfig jsonrpcConfig = new JSONRPCTransportConfig();
                
                // 使用 JSONRPC 传输
                Client client = Client.builder(agentCard)
                        .addConsumers(consumers)
                        .streamingErrorHandler(streamingErrorHandler)
                        .withTransport(JSONRPCTransport.class, jsonrpcConfig)
                        .build();
                
                clients.add(client);
                printSystemSuccess("✅ " + agentName + " 客户端初始化成功 [" + i + "]: " + url);
            } catch (Exception e) {
                log.error("Failed to initialize agent client for {} [{}]: {}", agentName, i, e.getMessage(), e);
                printSystemError("❌ " + agentName + " 客户端初始化失败 [" + i + "]: " + url + " - " + e.getMessage());
            }
        }
        
        if (clients.isEmpty()) {
            printSystemError("❌ 无法为 " + agentName + " 创建任何有效的客户端");
            return;
        }
        
        agentClientListMap.put(agentName, clients);
        agentRoundRobinIndexMap.put(agentName, new AtomicLong(0));
        printSystemSuccess("✅ " + agentName + " 总计初始化客户端数: " + clients.size());
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

    /**
     * 带重试机制的发送消息，支持切换Client
     */
    private static void sendRequestWithRetry(String agentName, String message, String messageId, int retryCount) {
        List<Client> clients = agentClientListMap.get(agentName);
        if (clients == null || clients.isEmpty()) {
            log.error("Agent客户端列表为空: {}", agentName);
            statistics.recordFailure(agentName, messageId);
            return;
        }
        
        // 轮询选择Client
        AtomicLong index = agentRoundRobinIndexMap.get(agentName);
        int clientIndex = (int) (index.getAndIncrement() % clients.size());
        Client client = clients.get(clientIndex);
        
        log.debug("发送尝试 ID: {}, Agent: {}, Client索引: {}/{}, 重试次数: {}", 
            messageId, agentName, clientIndex, clients.size() - 1, retryCount);
        
        CompletableFuture<Void> messageResponse = sendToAgent(message, messageId, agentName, client, clientIndex);
        
        // 异步处理发送结果
        messageResponse
            .orTimeout(1, TimeUnit.SECONDS)
            .whenComplete((result, throwable) -> {
                if (throwable instanceof TimeoutException) {
                    // 消息发送成功（超时表示发送成功，等待响应）
                    log.debug("发送成功 ID: {}, Client索引: {}", messageId, clientIndex);
                } else if (throwable != null) {
                    // 消息发送失败，尝试重试
                    log.warn("发送失败 ID: {}, Client索引: {}, 原因: {}", 
                        messageId, clientIndex, throwable.getMessage());
                    
                    // 清理当前失败的映射
                    String failedTaskId = taskIdToMessageIdMap.entrySet().stream()
                        .filter(entry -> entry.getValue().equals(messageId))
                        .map(Map.Entry::getKey)
                        .findFirst()
                        .orElse(null);
                    if (failedTaskId != null) {
                        taskIdToFutureMap.remove(failedTaskId);
                        taskIdToMessageIdMap.remove(failedTaskId);
                        statistics.recordFailure(agentName, failedTaskId);
                    }
                    
                    // 检查是否达到最大重试次数
                    if (retryCount < MAX_RETRY_TIMES - 1) {
                        // 尝试下一个Client
                        log.info("准备重试 ID: {}, 尝试下一个Client, 重试次数: {}/{}", 
                            messageId, retryCount + 1, MAX_RETRY_TIMES - 1);
                        sendRequestWithRetry(agentName, message, messageId, retryCount + 1);
                    } else {
                        // 所有Client都尝试失败
                        log.error("最终失败 ID: {}, 所有Client均尝试失败，总重试次数: {}", 
                            messageId, retryCount);
                        statistics.recordFailure(agentName, messageId);
                    }
                }
            });
    }

    /**
     * 发送消息到Agent（指定Client）
     */
    private static CompletableFuture<Void> sendToAgent(String message, String messageId, String agentName, 
                                                        Client client, int clientIndex) {
        if (client == null) {
            CompletableFuture<Void> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(new IllegalStateException("Client为空"));
            return failedFuture;
        }
        
        try {
            String taskId = UUID.randomUUID().toString();
            taskIdToMessageIdMap.put(taskId, messageId);
            
            CompletableFuture<Void> messageResponse = new CompletableFuture<>();
            taskIdToFutureMap.put(taskId, messageResponse);
            
            // 记录请求开始时间
            statistics.startRequest(agentName, taskId);
            
            client.sendMessage(A2A.createUserTextMessage(message, SESSION_ID, taskId));
            log.trace("消息已发送 ID: {}, Agent: {}, Client索引: {}, TaskId: {}", 
                messageId, agentName, clientIndex, taskId);
            return messageResponse;
        } catch (Exception e) {
            log.error("发送消息到Agent失败: {}, Client索引: {}", agentName, clientIndex, e);
            CompletableFuture<Void> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(e);
            return failedFuture;
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

    private static void printStatistics() {
        System.out.println("\n\u001B[33m📊 实时统计信息:\u001B[0m");
        statistics.printStatistics();
    }

    private static void printFinalStatistics() {
        System.out.println("\n\u001B[33m📊 最终统计信息:\u001B[0m");
        statistics.printStatistics();
        statistics.printDetailedStatistics();
    }

    // 请求统计类
    static class RequestStatistics {
        private final Map<String, AtomicLong> successCount = new HashMap<>();
        private final Map<String, AtomicLong> failureCount = new HashMap<>();
        private final Map<String, List<Long>> requestDurations = new HashMap<>();
        private final Map<String, Map<String, Long>> requestStartTimes = new HashMap<>();
        private final Map<String, List<MessageInfo>> messageDetails = new HashMap<>();

        public void startRequest(String agentName, String taskId) {
            requestStartTimes.computeIfAbsent(agentName, k -> new HashMap<>())
                    .put(taskId, System.currentTimeMillis());
        }

        public boolean hasPendingRequest(String agentName, String taskId) {
            Map<String, Long> startTimes = requestStartTimes.get(agentName);
            return startTimes != null && startTimes.containsKey(taskId);
        }

        public void recordSuccess(String agentName, String taskId) {
            successCount.computeIfAbsent(agentName, k -> new AtomicLong(0)).incrementAndGet();
            recordDuration(agentName, taskId, true);
        }

        public void recordFailure(String agentName, String taskId) {
            failureCount.computeIfAbsent(agentName, k -> new AtomicLong(0)).incrementAndGet();
            recordDuration(agentName, taskId, false);
        }

        private void recordDuration(String agentName, String taskId, boolean success) {
            Map<String, Long> startTimes = requestStartTimes.get(agentName);
            if (startTimes != null) {
                Long startTime = startTimes.remove(taskId);
                if (startTime != null) {
                    long duration = System.currentTimeMillis() - startTime;
                    requestDurations.computeIfAbsent(agentName, k -> new ArrayList<>()).add(duration);
                    
                    // 记录详细信息
                    MessageInfo info = new MessageInfo(taskId, startTime, System.currentTimeMillis(), duration, success);
                    messageDetails.computeIfAbsent(agentName, k -> new ArrayList<>()).add(info);
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
                
                // 计算 P50, P90, P99
                long p50 = calculatePercentile(durations, 50);
                long p90 = calculatePercentile(durations, 90);
                long p99 = calculatePercentile(durations, 99);
                
                System.out.println("  " + agentName + ":");
                System.out.println("    总请求数: " + total);
                System.out.println("    成功数: " + success);
                System.out.println("    失败数: " + failure);
                System.out.println("    成功率: " + String.format("%.2f%%", successRate));
                System.out.println("    平均耗时: " + String.format("%.2f", avgDuration) + " ms");
                System.out.println("    最小耗时: " + minDuration + " ms");
                System.out.println("    最大耗时: " + maxDuration + " ms");
                System.out.println("    P50耗时: " + p50 + " ms");
                System.out.println("    P90耗时: " + p90 + " ms");
                System.out.println("    P99耗时: " + p99 + " ms");
            }
        }

        public void printDetailedStatistics() {
            System.out.println("\n\u001B[36m📋 每条消息详情:\u001B[0m");
            for (Map.Entry<String, List<MessageInfo>> entry : messageDetails.entrySet()) {
                String agentName = entry.getKey();
                List<MessageInfo> messages = entry.getValue();
                System.out.println("  " + agentName + " (" + messages.size() + " 条消息):");
                for (MessageInfo info : messages) {
                    String status = info.success ? "✅" : "❌";
                    System.out.println(String.format("    %s [%s] 耗时: %d ms (开始: %d, 结束: %d)", 
                        status, info.taskId, info.duration, info.startTime, info.endTime));
                }
            }
        }

        private long calculatePercentile(List<Long> durations, int percentile) {
            if (durations == null || durations.isEmpty()) {
                return 0;
            }
            List<Long> sorted = new ArrayList<>(durations);
            sorted.sort(Long::compareTo);
            int index = (int) Math.ceil(sorted.size() * percentile / 100.0) - 1;
            if (index < 0) index = 0;
            if (index >= sorted.size()) index = sorted.size() - 1;
            return sorted.get(index);
        }
    }

    // 消息详细信息类
    static class MessageInfo {
        final String taskId;
        final long startTime;
        final long endTime;
        final long duration;
        final boolean success;

        MessageInfo(String taskId, long startTime, long endTime, long duration, boolean success) {
            this.taskId = taskId;
            this.startTime = startTime;
            this.endTime = endTime;
            this.duration = duration;
            this.success = success;
        }
    }
}
