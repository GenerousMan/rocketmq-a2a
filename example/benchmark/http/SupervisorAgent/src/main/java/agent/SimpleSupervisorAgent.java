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

import com.alibaba.fastjson.JSON;
import common.Message;
import common.Response;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 简化版SupervisorAgent - HTTP协议批量压测模式
 * 功能：
 * 1. 以固定QPS触发任务
 * 2. 根据关键词路由到下游Agent
 * 3. 记录每个任务的触发时间和完成时间
 * 4. 统计触发成功率、完成率、平均耗时和百分位数
 */
@SpringBootApplication
@RestController
public class SimpleSupervisorAgent implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(SimpleSupervisorAgent.class);
    
    // Agent配置 - 支持多副本，格式: url1,url2,url3
    private static final String WEATHER_AGENT_NAME = "WeatherAgent";
    private static final String WEATHER_AGENT_URLS = System.getProperty("weatherAgentUrls", "http://localhost:8080");
    private static final String TRAVEL_AGENT_NAME = "TravelAgent";
    private static final String TRAVEL_AGENT_URLS = System.getProperty("travelAgentUrls", "http://localhost:8888");
    private static final int MAX_RETRY_TIMES = Integer.parseInt(System.getProperty("maxRetryTimes", "3"));
    
    // 测试参数
    private static final String TEST_MESSAGE = System.getProperty("testMessage");
    private static final String QPS_STR = System.getProperty("qps", "1");
    private static final String MAX_TEST_TIME_STR = System.getProperty("maxTestTime");
    private static final String STATS_INTERVAL_STR = System.getProperty("statsInterval", "10");
    private static final String SUPERVISOR_PORT = System.getProperty("server.port", "9090");
    
    // Agent客户端映射 - 支持多副本
    private static final Map<String, List<String>> agentUrlListMap = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> agentRoundRobinIndexMap = new ConcurrentHashMap<>();
    
    // 任务信息跟踪
    private static final Map<String, TaskInfo> taskInfoMap = new ConcurrentHashMap<>();
    
    // 统计计数器
    private static final AtomicLong totalTriggered = new AtomicLong(0);
    private static final AtomicLong totalSentFailed = new AtomicLong(0);
    private static final AtomicLong totalCompleted = new AtomicLong(0);
    
    // 控制标志
    private static volatile boolean shouldStopSending = false;
    private static volatile boolean isShuttingDown = false;
    
    // HTTP客户端
    private static final OkHttpClient httpClient = new OkHttpClient.Builder()
            .connectTimeout(10, TimeUnit.SECONDS)
            .writeTimeout(10, TimeUnit.SECONDS)
            .readTimeout(10, TimeUnit.SECONDS)
            .build();
    private static final MediaType JSON_MEDIA_TYPE = MediaType.parse("application/json; charset=utf-8");
    
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
        volatile String taskId;
        volatile int clientIndex = -1;
        
        TaskInfo(String messageId, String message, String targetAgent, long triggerTime) {
            this.messageId = messageId;
            this.message = message;
            this.targetAgent = targetAgent;
            this.triggerTime = triggerTime;
        }
        
        long getDuration() {
            return isCompleted ? (completeTime - triggerTime) : -1;
        }
        
        long getPendingDuration() {
            return isCompleted ? 0 : (System.currentTimeMillis() - triggerTime);
        }
    }
    
    public static void main(String[] args) {
        SpringApplication.run(SimpleSupervisorAgent.class, args);
    }
    
    @Override
    public void run(String... args) throws Exception {
        if (!checkConfigParam()) {
            printSystemError("配置参数不完整，请检查参数配置");
            System.exit(1);
        }
        
        int qps = parseQPS(QPS_STR);
        if (qps <= 0) {
            printSystemError("QPS必须大于0，当前值: " + QPS_STR);
            System.exit(1);
        }
        
        if (StringUtils.isEmpty(TEST_MESSAGE)) {
            printSystemError("请通过 -DtestMessage= 参数指定测试消息");
            System.exit(1);
        }
        
        final Integer maxTestTimeSeconds = parseMaxTestTime(MAX_TEST_TIME_STR);
        if (maxTestTimeSeconds != null && maxTestTimeSeconds <= 0) {
            printSystemError("最大测试时间必须大于0，当前值: " + MAX_TEST_TIME_STR);
            System.exit(1);
        }
        
        printSystemInfo("启动简化版SupervisorAgent - HTTP协议批量压测模式");
        printSystemInfo("测试消息: " + TEST_MESSAGE);
        printSystemInfo("QPS: " + qps);
        printSystemInfo("SessionId: " + SESSION_ID);
        printSystemInfo("回调端口: " + SUPERVISOR_PORT);
        if (maxTestTimeSeconds != null) {
            printSystemInfo("最大测试时间: " + maxTestTimeSeconds + " 秒");
        }
        
        // 初始化Agent客户端 - 支持多副本
        initAgentUrls(WEATHER_AGENT_NAME, WEATHER_AGENT_URLS);
        initAgentUrls(TRAVEL_AGENT_NAME, TRAVEL_AGENT_URLS);
        
        printSystemInfo("开始批量发送消息...");
        printSystemInfo("统计信息将每 " + STATS_INTERVAL_STR + " 秒打印一次");
        
        // 使用定时器控制QPS发送消息
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        long intervalMs = 1000 / qps;
        
        final ScheduledFuture<?> sendTask = scheduler.scheduleAtFixedRate(() -> {
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
                printSystemInfo("达到最大测试时间 " + maxTestTimeSeconds + " 秒，停止发送新消息...");
                shouldStopSending = true;
                sendTask.cancel(false);
                
                printSystemInfo("等待让现有消息完成...");
                long waitStartTime = System.currentTimeMillis();
                long maxWaitTime = 300 * 1000;
                
                while (System.currentTimeMillis() - waitStartTime < maxWaitTime) {
                    long pending = totalTriggered.get() - totalSentFailed.get() - totalCompleted.get();
                    if (pending <= 0) {
                        printSystemInfo("所有消息已完成");
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
                printSystemInfo("测试结束，打印最终统计信息...");
                printFinalStatistics();
                
                scheduler.shutdown();
                System.exit(0);
            }, maxTestTimeSeconds, TimeUnit.SECONDS);
        }
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isShuttingDown = true;
            scheduler.shutdown();
            printSystemInfo("程序退出，打印最终统计信息...");
            printFinalStatistics();
        }));
    }
    
    /**
     * 回调接口 - 接收下游Agent的响应
     */
    @PostMapping("/callback")
    public String callback(@org.springframework.web.bind.annotation.RequestBody String responseBody) {
        try {
            Response response = JSON.parseObject(responseBody, Response.class);
            handleTaskCompletion(response);
            return "OK";
        } catch (Exception e) {
            log.error("处理回调失败", e);
            return "ERROR";
        }
    }
    
    /**
     * 健康检查
     */
    @GetMapping("/health")
    public String health() {
        return "OK";
    }
    
    /**
     * 发送测试消息
     */
    private static void sendTestMessage(String message) {
        long messageId = totalTriggered.incrementAndGet();
        String messageIdStr = String.valueOf(messageId);
        long triggerTime = System.currentTimeMillis();
        
        String targetAgent = getTargetAgentByKeyword(message);
        if (targetAgent == null) {
            log.warn("[路由失败] ID: {}, 消息: {} - 无法识别关键词", messageIdStr, message);
            return;
        }
        
        TaskInfo taskInfo = new TaskInfo(messageIdStr, message, targetAgent, triggerTime);
        taskInfoMap.put(messageIdStr, taskInfo);
        
        sendToAgentWithRetry(message, messageIdStr, targetAgent, 0);
    }
    
    /**
     * 带重试机制的发送消息
     */
    private static void sendToAgentWithRetry(String message, String messageId, String agentName, int retryCount) {
        List<String> urls = agentUrlListMap.get(agentName);
        if (urls == null || urls.isEmpty()) {
            log.error("[发送失败] Agent URL列表为空: {}", agentName);
            totalSentFailed.incrementAndGet();
            taskInfoMap.remove(messageId);
            return;
        }
        
        AtomicLong index = agentRoundRobinIndexMap.get(agentName);
        int clientIndex = (int) (index.getAndIncrement() % urls.size());
        String url = urls.get(clientIndex);
        
        TaskInfo taskInfo = taskInfoMap.get(messageId);
        if (taskInfo != null) {
            taskInfo.clientIndex = clientIndex;
            taskInfo.taskId = UUID.randomUUID().toString();
        }
        
        log.debug("[发送尝试] ID: {}, Agent: {}, URL索引: {}/{}, 重试次数: {}", 
                messageId, agentName, clientIndex, urls.size() - 1, retryCount);
        
        try {
            sendToAgent(message, messageId, agentName, url, taskInfo.taskId);
        } catch (Exception e) {
            log.warn("[发送失败] ID: {}, URL索引: {}, 原因: {}", messageId, clientIndex, e.getMessage());
            
            if (retryCount < MAX_RETRY_TIMES - 1) {
                log.info("[准备重试] ID: {}, 尝试下一个URL, 重试次数: {}/{}", 
                        messageId, retryCount + 1, MAX_RETRY_TIMES - 1);
                sendToAgentWithRetry(message, messageId, agentName, retryCount + 1);
            } else {
                log.error("[最终失败] ID: {}, 所有URL均尝试失败，总重试次数: {}", messageId, retryCount);
                totalSentFailed.incrementAndGet();
                taskInfoMap.remove(messageId);
            }
        }
    }
    
    /**
     * 发送消息到Agent
     */
    private static void sendToAgent(String message, String messageId, String agentName, String baseUrl, String taskId) 
            throws IOException {
        String callbackUrl = "http://localhost:" + SUPERVISOR_PORT + "/callback";
        
        Message msg = new Message(messageId, taskId, SESSION_ID, message, callbackUrl);
        String jsonBody = JSON.toJSONString(msg);
        
        String endpoint = agentName.equals(WEATHER_AGENT_NAME) ? "/weather/query" : "/travel/plan";
        String fullUrl = baseUrl + endpoint;
        
        okhttp3.RequestBody body = okhttp3.RequestBody.create(jsonBody, JSON_MEDIA_TYPE);
        Request request = new Request.Builder()
                .url(fullUrl)
                .post(body)
                .build();
        
        try (okhttp3.Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("HTTP error: " + response.code());
            }
            log.trace("[消息已发送] ID: {}, Agent: {}, URL: {}, TaskID: {}", messageId, agentName, fullUrl, taskId);
        }
    }
    
    /**
     * 处理任务完成
     */
    private static void handleTaskCompletion(Response response) {
        String messageId = response.getMessageId();
        TaskInfo taskInfo = taskInfoMap.get(messageId);
        if (taskInfo == null) {
            log.warn("[任务完成] 未找到任务信息: {}", messageId);
            return;
        }
        
        long completeTime = System.currentTimeMillis();
        taskInfo.completeTime = completeTime;
        taskInfo.isCompleted = true;
        totalCompleted.incrementAndGet();
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
     * 初始化Agent URL列表
     */
    private static void initAgentUrls(String agentName, String agentUrls) {
        if (StringUtils.isEmpty(agentName) || StringUtils.isEmpty(agentUrls)) {
            printSystemError("Agent配置参数错误: " + agentName);
            return;
        }
        
        String[] urls = agentUrls.split(",");
        List<String> urlList = new ArrayList<>();
        
        printSystemInfo("开始初始化 " + agentName + " 的多副本URL，总数: " + urls.length);
        
        for (int i = 0; i < urls.length; i++) {
            String url = urls[i].trim();
            if (!StringUtils.isEmpty(url)) {
                urlList.add(url);
                printSystemSuccess("添加URL [" + i + "]: " + agentName + " - " + url);
            }
        }
        
        if (urlList.isEmpty()) {
            printSystemError("无法为 " + agentName + " 添加任何有效的URL");
            return;
        }
        
        agentUrlListMap.put(agentName, urlList);
        agentRoundRobinIndexMap.put(agentName, new AtomicLong(0));
        printSystemSuccess(agentName + " 总计初始化URL数: " + urlList.size());
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
        double completionRate = triggered > 0 ? (double) completed / triggered * 100 : 0;
        
        System.out.println("\n" + "=".repeat(100));
        System.out.println("\u001B[36m[STATISTIC] 实时统计信息\u001B[0m");
        System.out.println("=".repeat(100));
        System.out.println(String.format("总触发数: %d | 发送失败数: %d | 已完成数: %d | 待完成数: %d", 
                triggered, failed, completed, pending));
        System.out.println(String.format("触发成功率: %.2f%% | 任务完成率: %.2f%%", successRate, completionRate));
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
        long pending = triggered - failed - completed;
        
        double triggerSuccessRate = triggered > 0 ? (double) successfullySent / triggered * 100 : 0;
        double completionRate = successfullySent > 0 ? (double) completed / successfullySent * 100 : 0;
        
        List<Long> durations = new ArrayList<>();
        for (TaskInfo taskInfo : taskInfoMap.values()) {
            if (taskInfo.isCompleted) {
                durations.add(taskInfo.getDuration());
            }
        }
        
        System.out.println("\n" + "=".repeat(100));
        System.out.println("\u001B[36m[FINISH] 最终统计信息\u001B[0m");
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
        
        // 打印pending消息列表
        if (pending > 0) {
            printPendingTasks();
        }
        
        System.out.println("=".repeat(100) + "\n");
    }
    
    /**
     * 打印pending任务列表
     */
    private static void printPendingTasks() {
        List<TaskInfo> pendingTasks = new ArrayList<>();
        for (TaskInfo taskInfo : taskInfoMap.values()) {
            if (!taskInfo.isCompleted) {
                pendingTasks.add(taskInfo);
            }
        }
        
        if (pendingTasks.isEmpty()) {
            return;
        }
        
        System.out.println("\n\u001B[33m[PENDING] Pending状态请求列表\u001B[0m");
        System.out.println("-".repeat(100));
        
        // 按等待时间排序（最长等待时间排在前面）
        pendingTasks.sort((t1, t2) -> Long.compare(t2.getPendingDuration(), t1.getPendingDuration()));
        
        for (int i = 0; i < pendingTasks.size(); i++) {
            TaskInfo task = pendingTasks.get(i);
            long pendingDuration = task.getPendingDuration();
            String timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date(task.triggerTime));
            
            String taskIdInfo = task.taskId != null ? task.taskId : "未分配";
            String clientInfo = task.clientIndex >= 0 ? String.valueOf(task.clientIndex) : "未知";
            
            System.out.println(String.format("  [%d] MsgID: %s | TaskID: %s | Agent: %s | Client: %s | 等待时长: %d ms | 发起时间: %s",
                i + 1, task.messageId, taskIdInfo, task.targetAgent, clientInfo, pendingDuration, timestamp));
        }
        
        System.out.println("-".repeat(100));
    }
    
    private static long getPercentile(List<Long> sortedList, double percentile) {
        if (sortedList.isEmpty()) {
            return 0;
        }
        int index = (int) Math.ceil((percentile / 100.0) * sortedList.size()) - 1;
        index = Math.max(0, Math.min(index, sortedList.size() - 1));
        return sortedList.get(index);
    }
    
    private static boolean checkConfigParam() {
        return true;
    }
    
    private static int parseQPS(String qpsStr) {
        try {
            return Integer.parseInt(qpsStr);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
    
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
    
    private static int parseStatsInterval(String intervalStr) {
        try {
            return Integer.parseInt(intervalStr);
        } catch (NumberFormatException e) {
            return 10;
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
