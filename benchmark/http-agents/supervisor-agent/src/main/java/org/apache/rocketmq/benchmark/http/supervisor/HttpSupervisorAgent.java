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
package org.apache.rocketmq.benchmark.http.supervisor;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.benchmark.common.Mission;
import org.apache.rocketmq.benchmark.common.MockConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP直连版SupervisorAgent - 使用HTTP协议直接与Worker Agent通信
 * 
 * <p>模拟传统的HTTP直连架构，与RocketMQ版本形成对比：
 * <ul>
 *   <li>同步阻塞 - 调用Worker Agent时阻塞等待响应</li>
 *   <li>无消息持久化 - 请求失败需要客户端重试</li>
 *   <li>无断点续传 - 网络中断后会话丢失</li>
 * </ul>
 * 
 * <p>LLM调用被Mock为固定延迟+固定响应。
 */
public class HttpSupervisorAgent {
    
    private static final Logger log = LoggerFactory.getLogger(HttpSupervisorAgent.class);
    
    private static final String WEATHER_AGENT_URL = System.getProperty("weatherAgentUrl", "http://localhost:8080");
    private static final String TRAVEL_AGENT_URL = System.getProperty("travelAgentUrl", "http://localhost:8888");
    
    private static final AtomicLong REQUEST_COUNT = new AtomicLong(0);
    private static final AtomicLong SUCCESS_COUNT = new AtomicLong(0);
    private static final AtomicLong FAILURE_COUNT = new AtomicLong(0);
    
    private final HttpClient httpClient;
    
    public HttpSupervisorAgent() {
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    }
    
    public static void main(String[] args) {
        HttpSupervisorAgent agent = new HttpSupervisorAgent();
        
        printSystemInfo("🚀 启动 HTTP直连版 SupervisorAgent (LLM已Mock)");
        printSystemInfo("📋 Weather Agent URL: " + WEATHER_AGENT_URL);
        printSystemInfo("📋 Travel Agent URL: " + TRAVEL_AGENT_URL);
        printSystemInfo("💡 输入 'quit' 退出，输入 'help' 查看帮助");
        
        try (Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8)) {
            while (true) {
                printPrompt("You");
                String userInput = scanner.nextLine().trim();
                
                if ("quit".equalsIgnoreCase(userInput)) {
                    printSystemInfo("👋 再见！");
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
                
                agent.processUserInput(userInput);
            }
        }
    }
    
    /**
     * 处理用户输入 - Mock LLM决策 + HTTP调用Worker Agent
     */
    public void processUserInput(String userInput) {
        long requestId = REQUEST_COUNT.incrementAndGet();
        long startTime = System.currentTimeMillis();
        
        try {
            printSystemInfo("🤔 正在思考...");
            
            // Mock LLM决策延迟
            Thread.sleep(MockConfig.getMockDelayMs());
            
            // Mock LLM决定分发到哪个Agent
            String llmResponse = MockConfig.autoDetectSupervisorResponse(userInput);
            log.info("[HTTP-{}] LLM决策: {}", requestId, llmResponse);
            
            // 判断是直接响应还是需要分发
            if (llmResponse.startsWith("{")) {
                Mission mission = JSON.parseObject(llmResponse, Mission.class);
                if (mission != null && mission.getAgent() != null) {
                    printPrompt("Agent");
                    System.out.println("分发任务到 " + mission.getAgent() + ": " + mission.getMessageInfo());
                    
                    // 通过HTTP同步调用Worker Agent
                    String agentResponse = callWorkerAgent(mission);
                    
                    printPrompt("Agent");
                    System.out.println(agentResponse);
                    
                    SUCCESS_COUNT.incrementAndGet();
                } else {
                    printPrompt("Agent");
                    System.out.println(llmResponse);
                }
            } else {
                printPrompt("Agent");
                System.out.println(llmResponse);
                SUCCESS_COUNT.incrementAndGet();
            }
            
            long elapsed = System.currentTimeMillis() - startTime;
            log.info("[HTTP-{}] Total time: {}ms", requestId, elapsed);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            FAILURE_COUNT.incrementAndGet();
            printSystemInfo("❌ 请求被中断");
        } catch (Exception e) {
            FAILURE_COUNT.incrementAndGet();
            printSystemInfo("❌ 错误: " + e.getMessage());
            log.error("[HTTP-{}] Error: {}", requestId, e.getMessage(), e);
        }
    }
    
    /**
     * 通过HTTP同步调用Worker Agent - 这是HTTP直连架构的核心特点
     * 与RocketMQ版本的区别：
     * - 同步阻塞等待响应
     * - 无消息持久化
     * - 网络故障直接失败
     */
    private String callWorkerAgent(Mission mission) throws IOException, InterruptedException {
        String agentUrl = getAgentUrl(mission.getAgent());
        
        String jsonRpcRequest = buildJsonRpcRequest(mission.getMessageInfo());
        
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(agentUrl + "/a2a"))
            .header("Content-Type", "application/json")
            .timeout(Duration.ofSeconds(120))  // HTTP连接可能超时
            .POST(HttpRequest.BodyPublishers.ofString(jsonRpcRequest))
            .build();
        
        log.info("Sending HTTP request to {}", agentUrl);
        long startTime = System.currentTimeMillis();
        
        // 同步阻塞调用 - HTTP直连架构的典型特征
        HttpResponse<String> response = httpClient.send(request, 
            HttpResponse.BodyHandlers.ofString());
        
        long elapsed = System.currentTimeMillis() - startTime;
        log.info("HTTP response received in {}ms, status={}", elapsed, response.statusCode());
        
        if (response.statusCode() != 200) {
            throw new IOException("HTTP error: " + response.statusCode());
        }
        
        return parseAgentResponse(response.body());
    }
    
    /**
     * 异步版本 - 用于压测时的高并发调用
     */
    public CompletableFuture<String> callWorkerAgentAsync(Mission mission) {
        String agentUrl = getAgentUrl(mission.getAgent());
        String jsonRpcRequest = buildJsonRpcRequest(mission.getMessageInfo());
        
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(agentUrl + "/a2a"))
            .header("Content-Type", "application/json")
            .timeout(Duration.ofSeconds(120))
            .POST(HttpRequest.BodyPublishers.ofString(jsonRpcRequest))
            .build();
        
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApply(response -> {
                if (response.statusCode() != 200) {
                    throw new RuntimeException("HTTP error: " + response.statusCode());
                }
                return parseAgentResponse(response.body());
            });
    }
    
    private String getAgentUrl(String agentName) {
        return switch (agentName.toLowerCase().replace(" ", "")) {
            case "weatheragent" -> WEATHER_AGENT_URL;
            case "travelagent" -> TRAVEL_AGENT_URL;
            default -> throw new IllegalArgumentException("Unknown agent: " + agentName);
        };
    }
    
    private String buildJsonRpcRequest(String message) {
        return """
            {
                "jsonrpc": "2.0",
                "method": "message/send",
                "params": {
                    "message": {
                        "role": "user",
                        "parts": [{"type": "text", "text": "%s"}]
                    }
                },
                "id": "%s"
            }
            """.formatted(message.replace("\"", "\\\""), UUID.randomUUID());
    }
    
    private String parseAgentResponse(String responseBody) {
        try {
            JSONObject json = JSON.parseObject(responseBody);
            JSONObject result = json.getJSONObject("result");
            if (result != null && result.containsKey("artifacts")) {
                return result.getJSONArray("artifacts")
                    .getJSONObject(0)
                    .getJSONArray("parts")
                    .getJSONObject(0)
                    .getString("text");
            }
            return responseBody;
        } catch (Exception e) {
            log.warn("Failed to parse response: {}", e.getMessage());
            return responseBody;
        }
    }
    
    public static String getStats() {
        return String.format(
            "[SupervisorAgent-HTTP] requests=%d, success=%d, failure=%d",
            REQUEST_COUNT.get(), SUCCESS_COUNT.get(), FAILURE_COUNT.get()
        );
    }
    
    private static void printSystemInfo(String message) {
        System.out.println("\u001B[34m[SYSTEM] " + message + "\u001B[0m");
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

