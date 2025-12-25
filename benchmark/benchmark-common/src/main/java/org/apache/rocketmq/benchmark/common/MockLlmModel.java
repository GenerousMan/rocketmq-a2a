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
package org.apache.rocketmq.benchmark.common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.google.adk.models.BaseLlm;
import com.google.adk.models.BaseLlmConnection;
import com.google.adk.models.LlmRequest;
import com.google.adk.models.LlmResponse;
import com.google.common.collect.ImmutableList;
import com.google.genai.types.Blob;
import com.google.genai.types.Content;
import com.google.genai.types.Part;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mock LLM 模型 - 用于 SupervisorAgent 的压测场景
 * 将大模型调用替换为可配置的固定延迟和固定响应
 * 
 * <p>用于测试多 Agent 协作时，SupervisorAgent 的任务分发能力。
 */
public class MockLlmModel extends BaseLlm {
    
    private static final Logger log = LoggerFactory.getLogger(MockLlmModel.class);
    private static final String MODEL_NAME = "mock-llm";
    private static final String MODEL_ROLE = "model";
    
    /** 模拟延迟（毫秒） */
    private final long mockDelayMs;
    
    /** 响应模式 */
    private final MockConfig.SupervisorMode responseMode;
    
    /** 自定义响应（CUSTOM模式使用） */
    private final String customResponse;
    
    /** 请求计数器 */
    private static final AtomicLong REQUEST_COUNT = new AtomicLong(0);
    
    /**
     * 创建默认的 Mock 模型（AUTO模式）
     */
    public MockLlmModel() {
        this(MockConfig.getMockDelayMs(), MockConfig.SupervisorMode.AUTO, null);
    }
    
    /**
     * 创建指定延迟的 Mock 模型
     */
    public MockLlmModel(long mockDelayMs) {
        this(mockDelayMs, MockConfig.SupervisorMode.AUTO, null);
    }
    
    /**
     * 创建指定模式的 Mock 模型
     */
    public MockLlmModel(long mockDelayMs, MockConfig.SupervisorMode responseMode, String customResponse) {
        super(MODEL_NAME);
        this.mockDelayMs = mockDelayMs;
        this.responseMode = responseMode;
        this.customResponse = customResponse;
    }
    
    /**
     * 从系统属性创建 Mock 模型
     */
    public static MockLlmModel fromSystemProperties() {
        return new MockLlmModel(
            MockConfig.getMockDelayMs(),
            MockConfig.getSupervisorMode(),
            MockConfig.getCustomSupervisorResponse()
        );
    }
    
    public static long getRequestCount() {
        return REQUEST_COUNT.get();
    }
    
    public static void resetRequestCount() {
        REQUEST_COUNT.set(0);
    }

    @Override
    public Flowable<LlmResponse> generateContent(LlmRequest llmRequest, boolean stream) {
        long requestId = REQUEST_COUNT.incrementAndGet();
        long startTime = System.currentTimeMillis();
        
        try {
            // 模拟延迟
            if (mockDelayMs > 0) {
                Thread.sleep(mockDelayMs);
            }
            
            // 根据输入决定返回内容
            String userInput = extractUserInput(llmRequest);
            String responseText = determineResponse(userInput);
            
            LlmResponse llmResponse = buildMockResponse(responseText);
            
            long elapsed = System.currentTimeMillis() - startTime;
            log.debug("[MockLlmModel-{}] Generated response in {}ms, mode={}", 
                requestId, elapsed, responseMode);
            
            return Flowable.just(llmResponse);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("[MockLlmModel-{}] Interrupted", requestId);
            return Flowable.error(e);
        } catch (Exception e) {
            log.error("[MockLlmModel-{}] Error: {}", requestId, e.getMessage());
            return Flowable.error(e);
        }
    }
    
    private String determineResponse(String userInput) {
        switch (responseMode) {
            case WEATHER_DISPATCH:
                return MockConfig.SUPERVISOR_WEATHER_DISPATCH;
            case TRAVEL_DISPATCH:
                return MockConfig.SUPERVISOR_TRAVEL_DISPATCH;
            case DIRECT:
                return MockConfig.SUPERVISOR_DIRECT_RESPONSE;
            case CUSTOM:
                return customResponse != null ? customResponse : MockConfig.SUPERVISOR_DIRECT_RESPONSE;
            case AUTO:
            default:
                return MockConfig.autoDetectSupervisorResponse(userInput);
        }
    }

    @Override
    public BaseLlmConnection connect(LlmRequest llmRequest) {
        return new BaseLlmConnection() {
            private boolean connected = true;
            private final List<Content> conversationHistory = new ArrayList<>();
            
            @Override
            public Completable sendHistory(List<Content> history) {
                return Completable.fromAction(() -> {
                    log.debug("[MockLlmModel] sendHistory count: {}", history.size());
                    conversationHistory.clear();
                    conversationHistory.addAll(history);
                });
            }
            
            @Override
            public Completable sendContent(Content content) {
                return Completable.fromAction(() -> {
                    log.debug("[MockLlmModel] sendContent");
                    conversationHistory.add(content);
                });
            }
            
            @Override
            public Completable sendRealtime(Blob blob) {
                return Completable.fromAction(() -> {
                    log.warn("[MockLlmModel] sendRealtime not supported, ignoring");
                });
            }
            
            @Override
            public Flowable<LlmResponse> receive() {
                return Flowable.defer(() -> {
                    if (!connected) {
                        return Flowable.error(new IllegalStateException("Connection closed"));
                    }
                    if (conversationHistory.isEmpty()) {
                        log.warn("[MockLlmModel] conversationHistory is empty");
                        return Flowable.empty();
                    }
                    LlmRequest request = LlmRequest.builder()
                        .contents(new ArrayList<>(conversationHistory))
                        .build();
                    return MockLlmModel.this.generateContent(request, false);
                });
            }
            
            @Override
            public void close() {
                connected = false;
                conversationHistory.clear();
                log.debug("[MockLlmModel] Connection closed");
            }
            
            @Override
            public void close(Throwable throwable) {
                connected = false;
                conversationHistory.clear();
                log.error("[MockLlmModel] Connection closed due to error: {}", 
                    throwable.getMessage());
            }
        };
    }
    
    private String extractUserInput(LlmRequest llmRequest) {
        if (llmRequest == null || llmRequest.contents() == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Content content : llmRequest.contents()) {
            String text = content.text();
            if (text != null) {
                sb.append(text);
            }
        }
        return sb.toString();
    }
    
    private LlmResponse buildMockResponse(String text) {
        Part part = Part.builder().text(text).build();
        Content content = Content.builder()
            .role(MODEL_ROLE)
            .parts(ImmutableList.of(part))
            .build();
        return LlmResponse.builder().content(content).build();
    }
}

