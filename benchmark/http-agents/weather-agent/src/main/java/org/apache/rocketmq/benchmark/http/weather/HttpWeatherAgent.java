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
package org.apache.rocketmq.benchmark.http.weather;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.rocketmq.benchmark.common.MockConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP直连版天气Agent - 使用HTTP协议直接通信
 * 
 * <p>模拟传统的HTTP直连架构，所有Agent通信通过HTTP REST API完成。
 * LLM调用被Mock为固定延迟+固定响应。
 * 
 * <p>API端点：
 * <ul>
 *   <li>GET /.well-known/agent.json - 获取Agent Card</li>
 *   <li>POST /a2a - JSON-RPC请求处理</li>
 * </ul>
 */
public class HttpWeatherAgent {
    
    private static final Logger log = LoggerFactory.getLogger(HttpWeatherAgent.class);
    
    private static final int DEFAULT_PORT = 9080;
    private static final AtomicLong REQUEST_COUNT = new AtomicLong(0);
    private static final AtomicLong SUCCESS_COUNT = new AtomicLong(0);
    private static final AtomicLong FAILURE_COUNT = new AtomicLong(0);
    
    private HttpServer server;
    private final int port;
    
    public HttpWeatherAgent(int port) {
        this.port = port;
    }
    
    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        HttpWeatherAgent agent = new HttpWeatherAgent(port);
        agent.start();
        
        log.info("HTTP Weather Agent started on port {}", port);
        log.info("Mock delay: {}ms", MockConfig.getMockDelayMs());
        
        // 保持运行
        Runtime.getRuntime().addShutdownHook(new Thread(agent::stop));
    }
    
    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(
            Integer.parseInt(System.getProperty("httpThreads", "50"))
        ));
        
        // Agent Card 端点
        server.createContext("/.well-known/agent.json", this::handleAgentCard);
        
        // A2A JSON-RPC 端点
        server.createContext("/a2a", this::handleA2ARequest);
        
        // 健康检查
        server.createContext("/health", this::handleHealth);
        
        // 统计信息
        server.createContext("/stats", this::handleStats);
        
        server.start();
    }
    
    public void stop() {
        if (server != null) {
            server.stop(0);
            log.info("HTTP Weather Agent stopped");
        }
    }
    
    /**
     * 返回Agent Card信息
     */
    private void handleAgentCard(HttpExchange exchange) throws IOException {
        String agentCard = """
            {
                "name": "WeatherAgent",
                "url": "http://localhost:%d",
                "version": "1.0.0",
                "description": "天气查询助手 (HTTP直连版)",
                "capabilities": {
                    "streaming": true,
                    "pushNotifications": false,
                    "stateTransitionHistory": true
                },
                "skills": [
                    {
                        "id": "weather_query",
                        "name": "查询天气信息",
                        "tags": ["天气", "气温", "湿度", "风力"]
                    }
                ]
            }
            """.formatted(port);
        
        sendResponse(exchange, 200, agentCard, "application/json");
    }
    
    /**
     * 处理A2A JSON-RPC请求 - 这是Agent的核心处理逻辑
     */
    private void handleA2ARequest(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "Method Not Allowed", "text/plain");
            return;
        }
        
        long requestId = REQUEST_COUNT.incrementAndGet();
        long startTime = System.currentTimeMillis();
        
        try {
            // 读取请求体
            String requestBody = readRequestBody(exchange);
            log.debug("[HTTP-{}] Received: {}", requestId, 
                requestBody.length() > 100 ? requestBody.substring(0, 100) + "..." : requestBody);
            
            // 解析JSON-RPC请求
            JSONObject jsonRpc = JSON.parseObject(requestBody);
            String method = jsonRpc.getString("method");
            String id = jsonRpc.getString("id");
            
            // 模拟LLM处理延迟
            Thread.sleep(MockConfig.getMockDelayMs());
            
            // 构建响应
            String response = buildJsonRpcResponse(id, method);
            
            sendResponse(exchange, 200, response, "application/json");
            SUCCESS_COUNT.incrementAndGet();
            
            long elapsed = System.currentTimeMillis() - startTime;
            log.info("[HTTP-{}] Completed in {}ms", requestId, elapsed);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            FAILURE_COUNT.incrementAndGet();
            sendErrorResponse(exchange, requestId, "Interrupted");
        } catch (Exception e) {
            FAILURE_COUNT.incrementAndGet();
            log.error("[HTTP-{}] Error: {}", requestId, e.getMessage());
            sendErrorResponse(exchange, requestId, e.getMessage());
        }
    }
    
    /**
     * 构建JSON-RPC响应 - Mock LLM响应
     */
    private String buildJsonRpcResponse(String id, String method) {
        String taskId = UUID.randomUUID().toString();
        String response = MockConfig.getWeatherResponse();
        
        // 模拟A2A协议的任务响应格式
        return """
            {
                "jsonrpc": "2.0",
                "id": "%s",
                "result": {
                    "id": "%s",
                    "contextId": "%s",
                    "status": {
                        "state": "completed"
                    },
                    "artifacts": [
                        {
                            "parts": [
                                {
                                    "type": "text",
                                    "text": "%s"
                                }
                            ]
                        }
                    ]
                }
            }
            """.formatted(id, taskId, UUID.randomUUID().toString(), 
                response.replace("\"", "\\\"").replace("\n", "\\n"));
    }
    
    private void handleHealth(HttpExchange exchange) throws IOException {
        sendResponse(exchange, 200, "{\"status\":\"UP\"}", "application/json");
    }
    
    private void handleStats(HttpExchange exchange) throws IOException {
        String stats = """
            {
                "agent": "WeatherAgent-HTTP",
                "requests": %d,
                "success": %d,
                "failure": %d
            }
            """.formatted(REQUEST_COUNT.get(), SUCCESS_COUNT.get(), FAILURE_COUNT.get());
        sendResponse(exchange, 200, stats, "application/json");
    }
    
    private String readRequestBody(HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
    
    private void sendResponse(HttpExchange exchange, int statusCode, 
            String response, String contentType) throws IOException {
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
    
    private void sendErrorResponse(HttpExchange exchange, long requestId, 
            String message) throws IOException {
        String error = """
            {
                "jsonrpc": "2.0",
                "id": "%d",
                "error": {
                    "code": -32603,
                    "message": "%s"
                }
            }
            """.formatted(requestId, message);
        sendResponse(exchange, 500, error, "application/json");
    }
}

