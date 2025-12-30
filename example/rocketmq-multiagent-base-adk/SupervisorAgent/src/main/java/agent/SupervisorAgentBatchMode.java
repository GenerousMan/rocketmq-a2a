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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import com.google.adk.agents.BaseAgent;
import com.google.adk.agents.LlmAgent;
import com.google.adk.artifacts.InMemoryArtifactService;
import com.google.adk.runner.Runner;
import com.google.adk.sessions.InMemorySessionService;
import com.google.adk.sessions.Session;
import common.Mission;
import common.QWModel;
import common.QWModelRegistry;
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

public class SupervisorAgentBatchMode {
    private static final Logger log = LoggerFactory.getLogger(SupervisorAgentBatchMode.class);
    private static final String AGENT_NAME = "SupervisorAgent";
    private static final String USER_ID = "rocketmq_a2a_user";
    private static final String APP_NAME = "rocketmq_a2a";
    private static final String WEATHER_AGENT_NAME = "WeatherAgent";
    private static final String WEATHER_AGENT_URL = "http://localhost:8080";
    private static final String TRAVEL_AGENT_NAME = "TravelAgent";
    private static final String TRAVEL_AGENT_URL = "http://localhost:8888";
    private static final String WORK_AGENT_RESPONSE_TOPIC = System.getProperty("workAgentResponseTopic");
    private static final String WORK_AGENT_RESPONSE_GROUP_ID = System.getProperty("workAgentResponseGroupID");
    private static final String ROCKETMQ_NAMESPACE = System.getProperty("rocketMQNamespace");
    private static final String ACCESS_KEY = System.getProperty("rocketMQAK");
    private static final String SECRET_KEY = System.getProperty("rocketMQSK");
    private static final String API_KEY = System.getProperty("apiKey");
    
    // 批量模式参数
    private static final String TEST_MESSAGE = System.getProperty("testMessage");
    private static final String QPS_STR = System.getProperty("qps", "1");
    private static final String MAX_TEST_TIME_STR = System.getProperty("maxTestTime"); // 最大测试时间（秒）
    
    private static InMemorySessionService sessionService;
    private static final Map<String, Client> AgentClientMap = new HashMap<>();
    private static String sessionId;
    private static Runner runner;
    
    // 用于跟踪消息发送时间和任务ID
    private static final Map<String, MessageInfo> messageInfoMap = new ConcurrentHashMap<>();
    private static final ConcurrentLinkedQueue<String> pendingMessageQueue = new ConcurrentLinkedQueue<>();
    private static final AtomicLong messageCounter = new AtomicLong(0);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    
    // 统计信息
    private static final List<Long> latencyList = new CopyOnWriteArrayList<>();
    private static final AtomicLong totalSent = new AtomicLong(0);
    private static final AtomicLong totalReceived = new AtomicLong(0);
    private static final AtomicLong totalFailed = new AtomicLong(0);
    private static final String STATS_INTERVAL_STR = System.getProperty("statsInterval", "10"); // 统计打印间隔（秒）
    
    // 测试时间控制
    private static volatile boolean shouldStopSending = false;
    private static volatile boolean isShuttingDown = false;
    private static long testStartTime = 0;
    
    // 消息信息类
    private static class MessageInfo {
        final String messageId;
        final String message;
        final long sendTime;
        final String agentName;
        
        MessageInfo(String messageId, String message, long sendTime, String agentName) {
            this.messageId = messageId;
            this.message = message;
            this.sendTime = sendTime;
            this.agentName = agentName;
        }
    }
    
    public static void main(String[] args) {
        if (!checkConfigParam()) {
            System.out.println("配置参数不完整，请检查参数配置情况");
            return;
        }
        
        int qps = parseQPS(QPS_STR);
        if (qps <= 0) {
            System.out.println("QPS必须大于0，当前值: " + QPS_STR);
            return;
        }
        
        if (StringUtils.isEmpty(TEST_MESSAGE)) {
            System.out.println("请通过 -DtestMessage= 参数指定测试消息");
            return;
        }
        
        BaseAgent baseAgent = initAgent(WEATHER_AGENT_NAME, TRAVEL_AGENT_NAME);
        printSystemInfo("🚀 启动批量模式 " + AGENT_NAME + "，消息: " + TEST_MESSAGE + "，QPS: " + qps);
        printSystemInfo("📋 初始化会话...");
        
        InMemoryArtifactService artifactService = new InMemoryArtifactService();
        sessionService = new InMemorySessionService();
        runner = new Runner(baseAgent, APP_NAME, artifactService, sessionService, /* memoryService= */ null);
        Session session = runner
            .sessionService()
            .createSession(APP_NAME, USER_ID)
            .blockingGet();
        printSystemSuccess("✅ 会话创建成功: " + session.id());
        sessionId = session.id();
        
        initAgentCardInfo(ACCESS_KEY, SECRET_KEY, WEATHER_AGENT_NAME, WEATHER_AGENT_URL);
        initAgentCardInfo(ACCESS_KEY, SECRET_KEY, TRAVEL_AGENT_NAME, TRAVEL_AGENT_URL);
        
        // 解析最大测试时间
        final Integer maxTestTimeSeconds;
        if (!StringUtils.isEmpty(MAX_TEST_TIME_STR)) {
            try {
                int parsedValue = Integer.parseInt(MAX_TEST_TIME_STR);
                if (parsedValue <= 0) {
                    printSystemError("最大测试时间必须大于0，当前值: " + MAX_TEST_TIME_STR);
                    return;
                }
                maxTestTimeSeconds = parsedValue;
            } catch (NumberFormatException e) {
                printSystemError("最大测试时间格式错误: " + MAX_TEST_TIME_STR);
                return;
            }
        } else {
            maxTestTimeSeconds = null;
        }
        
        printSystemInfo("📤 开始批量发送消息...");
        printSystemInfo("📊 统计信息将每 " + STATS_INTERVAL_STR + " 秒打印一次");
        if (maxTestTimeSeconds != null) {
            printSystemInfo("⏱️  最大测试时间: " + maxTestTimeSeconds + " 秒");
        }
        
        testStartTime = System.currentTimeMillis();
        
        // 使用定时器控制QPS发送消息
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
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
                totalFailed.incrementAndGet();
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
                
                // 等待最多30秒让现有消息完成
                printSystemInfo("⏳ 等待最多30秒让现有消息完成...");
                long waitStartTime = System.currentTimeMillis();
                long maxWaitTime = 30 * 1000; // 30秒
                
                while (System.currentTimeMillis() - waitStartTime < maxWaitTime) {
                    // 检查是否还有待处理的消息
                    if (pendingMessageQueue.isEmpty() && messageInfoMap.isEmpty()) {
                        printSystemInfo("✅ 所有消息已完成");
                        break;
                    }
                    try {
                        Thread.sleep(1000); // 每秒检查一次
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                
                isShuttingDown = true;
                printSystemInfo("👋 测试结束，打印最终统计信息...");
                printStatistics();
                
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
            printStatistics();
        }));
        
        // 主线程等待
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdown();
        }
    }
    
    private static int parseQPS(String qpsStr) {
        try {
            return Integer.parseInt(qpsStr);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
    
    private static int parseStatsInterval(String intervalStr) {
        try {
            return Integer.parseInt(intervalStr);
        } catch (NumberFormatException e) {
            return 10; // 默认10秒
        }
    }
    
    private static void sendTestMessage(String message) {
        long messageId = messageCounter.incrementAndGet();
        String messageIdStr = String.valueOf(messageId);
        long sendTime = System.currentTimeMillis();
        
        // 使用关键词路由，获取目标Agent
        String targetAgent = getTargetAgentByKeyword(message);
        if (targetAgent == null) {
            log.warn("[消息路由失败] ID: {}, 消息: {}", messageIdStr, message);
            totalFailed.incrementAndGet();
            return;
        }
        
        // 保存消息信息
        MessageInfo info = new MessageInfo(messageIdStr, message, sendTime, targetAgent);
        messageInfoMap.put(messageIdStr, info);
        pendingMessageQueue.offer(messageIdStr);
        
        totalSent.incrementAndGet();
        log.debug("[消息发送] ID: {}, Agent: {}, 消息: {}", messageIdStr, targetAgent, message);
        
        // 发送消息
        routeMessageByKeyword(message, messageIdStr, targetAgent);
    }
    
    private static boolean checkConfigParam() {
        if (StringUtils.isEmpty(WORK_AGENT_RESPONSE_TOPIC) || StringUtils.isEmpty(WORK_AGENT_RESPONSE_GROUP_ID) || StringUtils.isEmpty(API_KEY)) {
            if (StringUtils.isEmpty(WORK_AGENT_RESPONSE_TOPIC)) {
                System.out.println("请配置RocketMQ 的轻量消息Topic workAgentResponseTopic");
            }
            if (StringUtils.isEmpty(WORK_AGENT_RESPONSE_GROUP_ID)) {
                System.out.println("请配置RocketMQ 的轻量消息消费者 workAgentResponseGroupID");
            }
            if (StringUtils.isEmpty(API_KEY)) {
                System.out.println("请配置SupervisorAgent qwen-plus apiKey");
            }
            return false;
        }
        return true;
    }
    
    private static void dealMissionByMessage(Mission mission, String messageId) {
        if (null == mission || StringUtils.isEmpty(mission.getAgent()) || StringUtils.isEmpty(mission.getMessageInfo())) {
            return;
        }
        try {
            String agentName = mission.getAgent().replaceAll(" ", "");
            Client client = AgentClientMap.get(agentName);
            if (client == null) {
                printSystemError("❌ Agent客户端未找到: " + agentName);
                return;
            }
            client.sendMessage(A2A.toUserMessage(mission.getMessageInfo()));
        } catch (Exception e) {
            printSystemError("❌ 转发消息失败: " + e.getMessage());
            log.error("转发消息失败", e);
        }
    }
    
    private static String getTargetAgentByKeyword(String userInput) {
        if (StringUtils.isEmpty(userInput)) {
            return null;
        }
        String lowerInput = userInput.toLowerCase();
        if (lowerInput.contains("天气")) {
            return WEATHER_AGENT_NAME;
        } else if (lowerInput.contains("行程")) {
            return TRAVEL_AGENT_NAME;
        }
        return null;
    }
    
    public static BaseAgent initAgent(String weatherAgent, String travelAgent) {
        if (StringUtils.isEmpty(weatherAgent) || StringUtils.isEmpty(travelAgent)) {
            System.out.println("initAgent 参数缺失，请补充天气助手weatherAgent、行程安排助手travelAgent");
            return null;
        }
        QWModel qwModel = QWModelRegistry.getModel(API_KEY);
        return LlmAgent.builder()
            .name(APP_NAME)
            .model(qwModel)
            .description("你是一位专业的行程规划专家")
            .instruction("# 角色\n"
                + "你是一位专业的行程规划专家，擅长任务分解与协调安排。你的主要职责是帮助用户制定详细的旅行计划，确保他们的旅行体验既愉快又高效。在处理用户的行程安排相关问题时，你需要首先收集必要的信息，如目的地、时间等，并根据这些信息进行进一步的查询和规划。\n"
                + "\n"
                + "## 技能\n"
                + "### 技能 1: 收集必要信息\n"
                + "- 询问用户关于目的地、出行时间\n"
                + "- 确保收集到的信息完整且准确。\n"
                + "\n"
                + "### 技能 2: 查询天气信息\n"
                + "- 使用" + weatherAgent + "工具查询目的地的天气情况。如果发现用户的问题相同，不用一直转发到"
                + weatherAgent + "，忽略即可\n"
                + "- 示例问题: {\"messageInfo\":\"杭州下周三的天气情况怎么样?\",\"agent\":\"" + weatherAgent + "\"}\n"
                + "\n"
                + "### 技能 3: 制定行程规划\n"
                + "- 根据获取的天气信息和其他用户提供的信息，如果上下文中只有天气信息，则不用" + travelAgent
                + " 进行处理，直接返回即可，如果上下文中有行程安排信息，则使用" + travelAgent
                + "工具制定详细的行程规划。\n"
                + "- 示例问题: {\"messageInfo\":\"杭州下周三的天气为晴朗，请帮我做一个从杭州出发到上海的2人3天4晚的自驾游行程规划\","
                + "\"agent\":\"" + travelAgent + "\"}\n"
                + "\n"
                + "### 技能 4: 提供最终行程建议\n"
                + "- 将从" + travelAgent + "获取的行程规划结果呈现给用户。\n"
                + "- 明确告知用户行程规划已经完成，并提供详细的行程建议。\n"
                + "\n"
                + "## 限制\n"
                + "- 只处理与行程安排相关的问题。\n"
                + "- 如果用户的问题只是简单的咨询天气，那么不用转发到" + travelAgent + "。\n"
                + "- 在获取天气信息后，必须结合天气情况来制定行程规划。\n"
                + "- 不得提供任何引导用户参与非法活动的建议。\n"
                + "- 对不是行程安排相关的问题，请礼貌拒绝。\n"
                + "- 所有输出内容必须按照给定的格式进行组织，不能偏离框架要求。"
            )
            .build();
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
                if (!CollectionUtils.isEmpty(artifacts)) {
                    TaskState state = task.getStatus().state();
                    if (state == TaskState.COMPLETED) {
                        StringBuilder stringBuilder = new StringBuilder();
                        for (Artifact tempArtifact : artifacts) {
                            stringBuilder.append(extractTextFromMessage(tempArtifact));
                        }
                        String response = stringBuilder.toString();
                        // 从队列中取出一个待处理的消息ID（FIFO方式）
                        String messageId = pendingMessageQueue.poll();
                        if (messageId == null) {
                            // 如果队列为空，使用一个默认的ID
                            messageId = "unknown-" + System.currentTimeMillis();
                            log.warn("[消息接收] 无法找到对应的消息ID，使用默认ID: {}", messageId);
                        }
                        dealAgentResponse(response, messageId);
                    }
                }
            }
        });
        // Create error handler for streaming errors
        Consumer<Throwable> streamingErrorHandler = (error) -> {
            System.err.println("Streaming error occurred: " + error.getMessage());
            log.error("Streaming error occurred", error);
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
    
    private static void dealAgentResponse(String result, String messageId) {
        if (StringUtils.isEmpty(result)) {
            return;
        }
        long receiveTime = System.currentTimeMillis();
        MessageInfo info = messageInfoMap.get(messageId);
        long duration = 0;
        
        if (info != null) {
            duration = receiveTime - info.sendTime;
            // 记录耗时数据用于统计
            latencyList.add(duration);
            totalReceived.incrementAndGet();
            log.debug("[消息接收] ID: {}, Agent: {}, 耗时: {}ms", messageId, info.agentName, duration);
        } else {
            log.warn("[消息接收] 无法找到消息ID对应的信息: {}", messageId);
        }
        
        // 清理已处理的消息信息（可选，避免内存泄漏）
        messageInfoMap.remove(messageId);
    }
    
    /**
     * 根据关键词路由消息到对应的 agent
     * 根据输入的"天气"、"行程"关键词决定发送消息到哪个agent
     * 无记忆功能，忠诚的根据输入进行转发
     */
    private static void routeMessageByKeyword(String userInput, String messageId, String targetAgent) {
        if (StringUtils.isEmpty(userInput) || targetAgent == null) {
            return;
        }
        
        String messageInfo = userInput;
        
        // 创建 Mission 并转发
        Mission mission = new Mission(targetAgent, messageInfo);
        dealMissionByMessage(mission, messageId);
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
    
    private static void printSystemWarning(String message) {
        System.out.println("\u001B[33m[WARNING] " + message + "\u001B[0m");
        log.warn(message);
    }
    
    /**
     * 打印统计信息
     */
    private static void printStatistics() {
        long sent = totalSent.get();
        long received = totalReceived.get();
        long failed = totalFailed.get();
        int latencyCount = latencyList.size();
        
        if (latencyCount == 0) {
            printSystemInfo("📊 统计信息 - 已发送: " + sent + ", 已接收: " + received + ", 失败: " + failed + ", 耗时数据: 0");
            return;
        }
        
        // 计算统计指标
        List<Long> sortedLatencies = new ArrayList<>(latencyList);
        Collections.sort(sortedLatencies);
        
        long min = sortedLatencies.get(0);
        long max = sortedLatencies.get(sortedLatencies.size() - 1);
        long sum = sortedLatencies.stream().mapToLong(Long::longValue).sum();
        double avg = (double) sum / latencyCount;
        
        // 计算百分位数
        long p50 = getPercentile(sortedLatencies, 50);
        long p90 = getPercentile(sortedLatencies, 90);
        long p95 = getPercentile(sortedLatencies, 95);
        long p99 = getPercentile(sortedLatencies, 99);
        long p999 = getPercentile(sortedLatencies, 99.9);
        
        // 计算成功率
        double successRate = sent > 0 ? (double) received / sent * 100 : 0;
        
        // 打印统计信息
        System.out.println("\n" + "=".repeat(80));
        System.out.println("\u001B[36m📊 统计信息报告\u001B[0m");
        System.out.println("=".repeat(80));
        System.out.println(String.format("总发送数: %d | 总接收数: %d | 失败数: %d | 成功率: %.2f%%", 
            sent, received, failed, successRate));
        System.out.println(String.format("样本数: %d", latencyCount));
        System.out.println("-".repeat(80));
        System.out.println("\u001B[33m耗时统计 (ms):\u001B[0m");
        System.out.println(String.format("  最小值: %d ms", min));
        System.out.println(String.format("  最大值: %d ms", max));
        System.out.println(String.format("  平均值: %.2f ms", avg));
        System.out.println(String.format("  P50:    %d ms", p50));
        System.out.println(String.format("  P90:    %d ms", p90));
        System.out.println(String.format("  P95:    %d ms", p95));
        System.out.println(String.format("  P99:    %d ms", p99));
        System.out.println(String.format("  P99.9:  %d ms", p999));
        System.out.println("=".repeat(80) + "\n");
        
        // 记录到日志
        log.info("统计信息 - 发送: {}, 接收: {}, 失败: {}, 成功率: {}%, 平均耗时: {}ms, P99: {}ms", 
            sent, received, failed, String.format("%.2f", successRate), String.format("%.2f", avg), p99);
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
}

