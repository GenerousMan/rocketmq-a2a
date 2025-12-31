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

import java.util.ArrayList;
import java.util.Collections;
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
import common.Mission;
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

/**
 * 简化版SupervisorAgent - 批量压测模式
 * 功能：
 * 1. 以固定QPS触发任务
 * 2. 根据关键词路由到下游Agent
 * 3. 记录每个任务的触发时间和完成时间
 * 4. 统计触发成功率、完成率、平均耗时和百分位数
 */
public class SimpleSupervisorAgent {
    private static final Logger log = LoggerFactory.getLogger(SimpleSupervisorAgent.class);
    
    // RocketMQ配置
    private static final String WORK_AGENT_RESPONSE_TOPIC = System.getProperty("workAgentResponseTopic");
    private static final String WORK_AGENT_RESPONSE_GROUP_ID = System.getProperty("workAgentResponseGroupID");
    private static final String ROCKETMQ_NAMESPACE = System.getProperty("rocketMQNamespace");
    private static final String ACCESS_KEY = System.getProperty("rocketMQAK");
    private static final String SECRET_KEY = System.getProperty("rocketMQSK");
    
    // Agent配置 - 支持多副本，格式: url1,url2,url3
    private static final String WEATHER_AGENT_NAME = "WeatherAgent";
    private static final String WEATHER_AGENT_URLS = System.getProperty("weatherAgentUrls", "http://localhost:8080");
    private static final String TRAVEL_AGENT_NAME = "TravelAgent";
    private static final String TRAVEL_AGENT_URLS = System.getProperty("travelAgentUrls", "http://localhost:8888");
    private static final int MAX_RETRY_TIMES = Integer.parseInt(System.getProperty("maxRetryTimes", "3"));
    
    // 测试参数
    private static final String TEST_MESSAGE = System.getProperty("testMessage");
    private static final String QPS_STR = System.getProperty("qps", "1");
    private static final String MAX_TEST_TIME_STR = System.getProperty("maxTestTime"); // 最大测试时间（秒）
    private static final String STATS_INTERVAL_STR = System.getProperty("statsInterval", "10"); // 统计打印间隔（秒）
    
    // Agent客户端映射 - 支持多副本
    private static final Map<String, List<Client>> agentClientListMap = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> agentRoundRobinIndexMap = new ConcurrentHashMap<>();
    
    // 任务信息跟踪
    private static final Map<String, TaskInfo> taskInfoMap = new ConcurrentHashMap<>();
    private static final Map<String, CompletableFuture<Void>> taskIdToFutureMap = new ConcurrentHashMap<>();
    private static final Map<String, String> taskIdToMessageIdMap = new ConcurrentHashMap<>();
    
    // 统计计数器
    private static final AtomicLong totalTriggered = new AtomicLong(0); // 总触发数
    private static final AtomicLong totalSentFailed = new AtomicLong(0); // 发送失败数
    private static final AtomicLong totalCompleted = new AtomicLong(0); // 总完成数
    
    // 控制标志
    private static volatile boolean shouldStopSending = false;
    private static volatile boolean isShuttingDown = false;
    
    // 单一session配置
    private static final String SESSION_ID = "simple_supervisor_session_" + System.currentTimeMillis();
    
    /**
     * 任务信息类
     */
    private static class TaskInfo {
        final String messageId;
        final String message;
        final String targetAgent;
        final long triggerTime;
        volatile long completeTime = -1;
        volatile boolean isCompleted = false;
        
        TaskInfo(String messageId, String message, String targetAgent, long triggerTime) {
            this.messageId = messageId;
            this.message = message;
            this.targetAgent = targetAgent;
            this.triggerTime = triggerTime;
        }
        
        long getDuration() {
            return isCompleted ? (completeTime - triggerTime) : -1;
        }
    }
    
    public static void main(String[] args) {
        if (!checkConfigParam()) {
            printSystemError("❌ 配置参数不完整，请检查参数配置");
            return;
        }
        
        int qps = parseQPS(QPS_STR);
        if (qps <= 0) {
            printSystemError("❌ QPS必须大于0，当前值: " + QPS_STR);
            return;
        }
        
        if (StringUtils.isEmpty(TEST_MESSAGE)) {
            printSystemError("❌ 请通过 -DtestMessage= 参数指定测试消息");
            return;
        }
        
        // 解析最大测试时间
        final Integer maxTestTimeSeconds = parseMaxTestTime(MAX_TEST_TIME_STR);
        if (maxTestTimeSeconds != null && maxTestTimeSeconds <= 0) {
            printSystemError("❌ 最大测试时间必须大于0，当前值: " + MAX_TEST_TIME_STR);
            return;
        }
        
        printSystemInfo("🚀 启动简化版SupervisorAgent - 批量压测模式");
        printSystemInfo("📋 测试消息: " + TEST_MESSAGE);
        printSystemInfo("📋 QPS: " + qps);
        printSystemInfo("📋 SessionId: " + SESSION_ID);
        if (maxTestTimeSeconds != null) {
            printSystemInfo("⏱️  最大测试时间: " + maxTestTimeSeconds + " 秒");
        }
        
        // 初始化Agent客户端 - 支持多副本
        initAgentClients(ACCESS_KEY, SECRET_KEY, WEATHER_AGENT_NAME, WEATHER_AGENT_URLS);
        initAgentClients(ACCESS_KEY, SECRET_KEY, TRAVEL_AGENT_NAME, TRAVEL_AGENT_URLS);
        
        printSystemInfo("📤 开始批量发送消息...");
        printSystemInfo("📊 统计信息将每 " + STATS_INTERVAL_STR + " 秒打印一次");
        
        // 使用定时器控制QPS发送消息
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        long intervalMs = 1000 / qps; // 每条消息的间隔时间（毫秒）
        
        // 消息发送任务
        final java.util.concurrent.ScheduledFuture<?> sendTask = scheduler.scheduleAtFixedRate(() -> {
            if (shouldStopSending) {
                return;
            }
            try {
                sendTestMessage(TEST_MESSAGE);
            } catch (Exception e) {
                log.error("发送消息失败", e);
            }
        }, 0, intervalMs, TimeUnit.MILLISECONDS);
        
        // 定时打印统计信息
        int statsInterval = parseStatsInterval(STATS_INTERVAL_STR);
        scheduler.scheduleAtFixedRate(() -> {
            if (!isShuttingDown) {
                printStatistics();
            }
        }, statsInterval, statsInterval, TimeUnit.SECONDS);
        
        // 最大测试时间控制
        if (maxTestTimeSeconds != null) {
            scheduler.schedule(() -> {
                printSystemInfo("⏱️  达到最大测试时间 " + maxTestTimeSeconds + " 秒，停止发送新消息...");
                shouldStopSending = true;
                sendTask.cancel(false);
                
                // 等待最多60秒让现有消息完成
                printSystemInfo("⏳ 等待最多60秒让现有消息完成...");
                long waitStartTime = System.currentTimeMillis();
                long maxWaitTime = 60 * 1000; // 60秒
                
                while (System.currentTimeMillis() - waitStartTime < maxWaitTime) {
                    long pending = totalTriggered.get() - totalSentFailed.get() - totalCompleted.get();
                    if (pending <= 0) {
                        printSystemInfo("✅ 所有消息已完成");
                        break;
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                
                isShuttingDown = true;
                printSystemInfo("👋 测试结束，打印最终统计信息...");
                printFinalStatistics();
                
                // 关闭调度器并退出
                scheduler.shutdown();
                try {
                    if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                        scheduler.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    scheduler.shutdownNow();
                    Thread.currentThread().interrupt();
                }
                System.exit(0);
            }, maxTestTimeSeconds, TimeUnit.SECONDS);
        }
        
        // 保持程序运行
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isShuttingDown = true;
            scheduler.shutdown();
            printSystemInfo("👋 程序退出，打印最终统计信息...");
            printFinalStatistics();
        }));
        
        // 主线程等待
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdown();
        }
    }
    
    /**
     * 发送测试消息
     */
    private static void sendTestMessage(String message) {
        long messageId = totalTriggered.incrementAndGet();
        String messageIdStr = String.valueOf(messageId);
        long triggerTime = System.currentTimeMillis();
        
        // 根据关键词路由到目标Agent
        String targetAgent = getTargetAgentByKeyword(message);
        if (targetAgent == null) {
            log.warn("[路由失败] ID: {}, 消息: {} - 无法识别关键词", messageIdStr, message);
            return;
        }
        
        // 创建任务信息
        TaskInfo taskInfo = new TaskInfo(messageIdStr, message, targetAgent, triggerTime);
        taskInfoMap.put(messageIdStr, taskInfo);
        
        log.debug("[任务触发] ID: {}, Agent: {}, 消息: {}", messageIdStr, targetAgent, message);
        
        // 发送消息，支持重试
        sendToAgentWithRetry(message, messageIdStr, targetAgent, 0);
    }
    
    /**
     * 带重试机制的发送消息，支持切换Client
     */
    private static void sendToAgentWithRetry(String message, String messageId, String agentName, int retryCount) {
        List<Client> clients = agentClientListMap.get(agentName);
        if (clients == null || clients.isEmpty()) {
            log.error("[发送失败] Agent客户端列表为空: {}", agentName);
            totalSentFailed.incrementAndGet();
            taskInfoMap.remove(messageId);
            return;
        }
        
        // 轮询选择Client
        AtomicLong index = agentRoundRobinIndexMap.get(agentName);
        int clientIndex = (int) (index.getAndIncrement() % clients.size());
        Client client = clients.get(clientIndex);
        
        log.debug("[发送尝试] ID: {}, Agent: {}, Client索引: {}/{}, 重试次数: {}", 
            messageId, agentName, clientIndex, clients.size() - 1, retryCount);
        
        CompletableFuture<Void> messageResponse = sendToAgent(message, messageId, agentName, client, clientIndex);
        
        // 异步处理发送结果
        messageResponse
            .orTimeout(1, TimeUnit.SECONDS)
            .whenComplete((result, throwable) -> {
                if (throwable instanceof TimeoutException) {
                    // 消息发送成功
                    log.debug("[发送成功] ID: {}, Client索引: {}", messageId, clientIndex);
                } else if (throwable != null) {
                    // 消息发送失败，尝试重试
                    log.warn("[发送失败] ID: {}, Client索引: {}, 原因: {}", 
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
                    }
                    
                    // 检查是否达到最大重试次数
                    if (retryCount < MAX_RETRY_TIMES - 1) {
                        // 尝试下一个Client
                        log.info("[准备重试] ID: {}, 尝试下一个Client, 重试次数: {}/{}", 
                            messageId, retryCount + 1, MAX_RETRY_TIMES - 1);
                        sendToAgentWithRetry(message, messageId, agentName, retryCount + 1);
                    } else {
                        // 所有Client都尝试失败
                        log.error("[最终失败] ID: {}, 所有Client均尝试失败，总重试次数: {}", 
                            messageId, retryCount);
                        totalSentFailed.incrementAndGet();
                        taskInfoMap.remove(messageId);
                    }
                }
            });
    }
    
    /**
     * 根据关键词路由到目标Agent
     */
    private static String getTargetAgentByKeyword(String message) {
        if (StringUtils.isEmpty(message)) {
            return null;
        }
        String lowerInput = message.toLowerCase();
        if (lowerInput.contains("天气")) {
            return WEATHER_AGENT_NAME;
        } else if (lowerInput.contains("行程")) {
            return TRAVEL_AGENT_NAME;
        }
        return null;
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
            
            client.sendMessage(A2A.createUserTextMessage(message, SESSION_ID, taskId));
            log.trace("[消息已发送] ID: {}, Agent: {}, Client索引: {}, TaskId: {}", 
                messageId, agentName, clientIndex, taskId);
            return messageResponse;
        } catch (Exception e) {
            log.error("发送消息到Agent失败: {}, Client索引: {}", agentName, clientIndex, e);
            CompletableFuture<Void> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(e);
            return failedFuture;
        }
    }
    
    /**
     * 初始化Agent客户端（支持多副本）
     */
    private static void initAgentClients(String accessKey, String secretKey, String agentName, String agentUrls) {
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
                printSystemSuccess("✅ 获取AgentCard成功 [" + i + "]: " + agentName + " - " + url + " - " + agentCard.description());
                
                // 创建事件消费者
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
                        if (!CollectionUtils.isEmpty(artifacts) && state == TaskState.COMPLETED) {
                            StringBuilder responseBuilder = new StringBuilder();
                            for (Artifact artifact : artifacts) {
                                responseBuilder.append(extractTextFromArtifact(artifact));
                            }
                            String response = responseBuilder.toString();
                            
                            // 获取对应的messageId
                            String messageId = taskIdToMessageIdMap.get(taskId);
                            if (messageId != null) {
                                handleTaskCompletion(messageId, response);
                                
                                // 清理映射
                                taskIdToFutureMap.remove(taskId);
                                taskIdToMessageIdMap.remove(taskId);
                            }
                        }
                    }
                });
                
                // 创建错误处理器
                Consumer<Throwable> errorHandler = (error) -> {
                    log.error("Streaming error occurred for {} [{}]", agentName, url, error);
                    printSystemError("❌ 流式错误 [" + agentName + " - " + url + "]: " + error.getMessage());
                };
                
                // 配置RocketMQ传输
                RocketMQTransportConfig config = new RocketMQTransportConfig();
                config.setNamespace(ROCKETMQ_NAMESPACE);
                config.setAccessKey(accessKey);
                config.setSecretKey(secretKey);
                config.setWorkAgentResponseGroupID(WORK_AGENT_RESPONSE_GROUP_ID);
                config.setWorkAgentResponseTopic(WORK_AGENT_RESPONSE_TOPIC);
                
                // 创建客户端
                Client client = Client.builder(agentCard)
                    .addConsumers(consumers)
                    .streamingErrorHandler(errorHandler)
                    .withTransport(RocketMQTransport.class, config)
                    .build();
                
                clients.add(client);
                printSystemSuccess("✅ Agent客户端初始化成功 [" + i + "]: " + agentName + " - " + url);
            } catch (Exception e) {
                printSystemError("❌ 初始化Agent客户端失败 [" + i + "]: " + agentName + " - " + url + " - " + e.getMessage());
                log.error("初始化Agent客户端失败", e);
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
    
    /**
     * 处理任务完成
     */
    private static void handleTaskCompletion(String messageId, String response) {
        TaskInfo taskInfo = taskInfoMap.get(messageId);
        if (taskInfo == null) {
            log.warn("[任务完成] 未找到任务信息: {}", messageId);
            return;
        }
        
        long completeTime = System.currentTimeMillis();
        taskInfo.completeTime = completeTime;
        taskInfo.isCompleted = true;
        totalCompleted.incrementAndGet();
        
        long duration = taskInfo.getDuration();
        log.debug("[任务完成] ID: {}, Agent: {}, 耗时: {}ms", messageId, taskInfo.targetAgent, duration);
    }
    
    /**
     * 从Artifact提取文本
     */
    private static String extractTextFromArtifact(Artifact artifact) {
        if (artifact == null) {
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
    
    /**
     * 打印统计信息
     */
    private static void printStatistics() {
        long triggered = totalTriggered.get();
        long failed = totalSentFailed.get();
        long completed = totalCompleted.get();
        long pending = triggered - failed - completed;
        
        double successRate = triggered > 0 ? (double) (triggered - failed) / triggered * 100 : 0;
        
        System.out.println("\n" + "=".repeat(100));
        System.out.println("\u001B[36m📊 实时统计信息\u001B[0m");
        System.out.println("=".repeat(100));
        System.out.println(String.format("总触发数: %d | 发送失败数: %d | 已完成数: %d | 待完成数: %d", 
            triggered, failed, completed, pending));
        System.out.println(String.format("触发成功率: %.2f%%", successRate));
        System.out.println("=".repeat(100) + "\n");
    }
    
    /**
     * 打印最终统计信息
     */
    private static void printFinalStatistics() {
        long triggered = totalTriggered.get();
        long failed = totalSentFailed.get();
        long completed = totalCompleted.get();
        long successfullySent = triggered - failed;
        
        double triggerSuccessRate = triggered > 0 ? (double) successfullySent / triggered * 100 : 0;
        double completionRate = successfullySent > 0 ? (double) completed / successfullySent * 100 : 0;
        
        // 收集完成任务的耗时数据
        List<Long> durations = new ArrayList<>();
        for (TaskInfo taskInfo : taskInfoMap.values()) {
            if (taskInfo.isCompleted) {
                durations.add(taskInfo.getDuration());
            }
        }
        
        System.out.println("\n" + "=".repeat(100));
        System.out.println("\u001B[36m📊 最终统计报告\u001B[0m");
        System.out.println("=".repeat(100));
        System.out.println(String.format("总触发数: %d", triggered));
        System.out.println(String.format("发送失败数: %d", failed));
        System.out.println(String.format("成功发送数: %d (触发成功率: %.2f%%)", successfullySent, triggerSuccessRate));
        System.out.println(String.format("已完成数: %d (最终完成率: %.2f%%)", completed, completionRate));
        System.out.println("-".repeat(100));
        
        if (!durations.isEmpty()) {
            Collections.sort(durations);
            long sum = durations.stream().mapToLong(Long::longValue).sum();
            double avg = (double) sum / durations.size();
            long p50 = getPercentile(durations, 50);
            long p90 = getPercentile(durations, 90);
            long p99 = getPercentile(durations, 99);
            
            System.out.println("\u001B[33m耗时统计 (ms):\u001B[0m");
            System.out.println(String.format("  样本数: %d", durations.size()));
            System.out.println(String.format("  最小值: %d ms", durations.get(0)));
            System.out.println(String.format("  最大值: %d ms", durations.get(durations.size() - 1)));
            System.out.println(String.format("  平均值: %.2f ms", avg));
            System.out.println(String.format("  P50: %d ms", p50));
            System.out.println(String.format("  P90: %d ms", p90));
            System.out.println(String.format("  P99: %d ms", p99));
        } else {
            System.out.println("\u001B[33m耗时统计: 无完成数据\u001B[0m");
        }
        
        System.out.println("=".repeat(100) + "\n");
        
        // 记录到日志
        log.info("最终统计 - 触发: {}, 失败: {}, 完成: {}, 触发成功率: {:.2f}%, 完成率: {:.2f}%", 
            triggered, failed, completed, triggerSuccessRate, completionRate);
    }
    
    /**
     * 计算百分位数
     */
    private static long getPercentile(List<Long> sortedList, double percentile) {
        if (sortedList.isEmpty()) {
            return 0;
        }
        int index = (int) Math.ceil((percentile / 100.0) * sortedList.size()) - 1;
        index = Math.max(0, Math.min(index, sortedList.size() - 1));
        return sortedList.get(index);
    }
    
    /**
     * 检查配置参数
     */
    private static boolean checkConfigParam() {
        boolean valid = true;
        if (StringUtils.isEmpty(WORK_AGENT_RESPONSE_TOPIC)) {
            System.out.println("请配置RocketMQ的轻量消息Topic: workAgentResponseTopic");
            valid = false;
        }
        if (StringUtils.isEmpty(WORK_AGENT_RESPONSE_GROUP_ID)) {
            System.out.println("请配置RocketMQ的轻量消息消费者: workAgentResponseGroupID");
            valid = false;
        }
        return valid;
    }
    
    /**
     * 解析QPS参数
     */
    private static int parseQPS(String qpsStr) {
        try {
            return Integer.parseInt(qpsStr);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
    
    /**
     * 解析最大测试时间参数
     */
    private static Integer parseMaxTestTime(String timeStr) {
        if (StringUtils.isEmpty(timeStr)) {
            return null;
        }
        try {
            return Integer.parseInt(timeStr);
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    /**
     * 解析统计间隔参数
     */
    private static int parseStatsInterval(String intervalStr) {
        try {
            return Integer.parseInt(intervalStr);
        } catch (NumberFormatException e) {
            return 10; // 默认10秒
        }
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
}
