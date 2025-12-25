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
package org.apache.rocketmq.benchmark.rocketmq.travel;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import io.a2a.server.agentexecution.AgentExecutor;
import io.a2a.server.agentexecution.RequestContext;
import io.a2a.server.events.EventQueue;
import io.a2a.server.tasks.TaskUpdater;
import io.a2a.spec.JSONRPCError;
import io.a2a.spec.Message;
import io.a2a.spec.Part;
import io.a2a.spec.Task;
import io.a2a.spec.TaskNotCancelableError;
import io.a2a.spec.TaskState;
import io.a2a.spec.TaskStatus;
import io.a2a.spec.TextPart;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.rocketmq.benchmark.common.MockConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 行程Agent Mock实现 - 仅Mock LLM调用，保留所有RocketMQ交互
 * 
 * <p>将大模型调用替换为可配置的固定延迟和固定响应，
 * 但所有通过RocketMQ的消息收发逻辑保持原样。
 */
@ApplicationScoped
public class MockAgentExecutorProducer {
    
    private static final Logger log = LoggerFactory.getLogger(MockAgentExecutorProducer.class);
    
    // 请求计数器（用于统计和日志）
    private static final AtomicLong REQUEST_COUNT = new AtomicLong(0);
    private static final AtomicLong SUCCESS_COUNT = new AtomicLong(0);
    private static final AtomicLong FAILURE_COUNT = new AtomicLong(0);
    
    public static String getStats() {
        return String.format(
            "[TravelAgent-Mock] requests=%d, success=%d, failure=%d",
            REQUEST_COUNT.get(), SUCCESS_COUNT.get(), FAILURE_COUNT.get()
        );
    }
    
    public static void resetStats() {
        REQUEST_COUNT.set(0);
        SUCCESS_COUNT.set(0);
        FAILURE_COUNT.set(0);
    }

    @Produces
    public AgentExecutor agentExecutor() {
        log.info("[TravelAgent] Mock mode ENABLED, delay={}ms", MockConfig.getMockDelayMs());
        
        return new AgentExecutor() {
            @Override
            public void execute(RequestContext context, EventQueue eventQueue) throws JSONRPCError {
                long requestId = REQUEST_COUNT.incrementAndGet();
                long startTime = System.currentTimeMillis();
                
                String userMessage = extractTextFromMessage(context.getMessage());
                log.info("[MOCK-{}] Received: {}", requestId, 
                    userMessage.length() > 50 ? userMessage.substring(0, 50) + "..." : userMessage);
                
                // 保留原有的Task创建逻辑
                Task task = context.getTask();
                if (task == null) {
                    task = createTask(context.getMessage());
                    eventQueue.enqueueEvent(task);
                }
                
                TaskUpdater taskUpdater = new TaskUpdater(context, eventQueue);
                
                try {
                    // 仅此处被Mock：将LLM调用替换为模拟流式输出
                    Flowable<String> mockStream = createMockStream();
                    StringBuilder fullResponse = new StringBuilder();
                    
                    for (String chunk : mockStream.blockingIterable()) {
                        fullResponse.append(chunk);
                        List<Part<?>> parts = List.of(new TextPart(chunk, null));
                        // 保留原有的TaskUpdater交互
                        taskUpdater.addArtifact(parts);
                    }
                    
                    // 保留原有的完成逻辑
                    taskUpdater.complete();
                    SUCCESS_COUNT.incrementAndGet();
                    
                    long elapsed = System.currentTimeMillis() - startTime;
                    log.info("[MOCK-{}] Completed in {}ms", requestId, elapsed);
                        
                } catch (Exception e) {
                    FAILURE_COUNT.incrementAndGet();
                    log.error("[MOCK-{}] Failed: {}", requestId, e.getMessage());
                    
                    taskUpdater.startWork(taskUpdater.newAgentMessage(
                        List.of(new TextPart("Mock error: " + e.getMessage(), null)), 
                        Map.of()
                    ));
                    taskUpdater.fail();
                }
            }

            @Override
            public void cancel(RequestContext context, EventQueue eventQueue) throws JSONRPCError {
                Task task = context.getTask();
                if (null == task || null == task.getStatus()) {
                    return;
                }
                if (task.getStatus().state() == TaskState.CANCELED 
                    || task.getStatus().state() == TaskState.COMPLETED) {
                    throw new TaskNotCancelableError();
                }
                TaskUpdater updater = new TaskUpdater(context, eventQueue);
                updater.cancel();
            }
        };
    }
    
    /**
     * 创建Mock流式输出 - 这是唯一被Mock的部分
     * 原实现调用阿里百炼API，这里替换为固定延迟+固定响应
     */
    private Flowable<String> createMockStream() {
        return Flowable.create(emitter -> {
            try {
                String response = MockConfig.getTravelResponse();
                String[] chunks = MockConfig.splitIntoChunks(response, MockConfig.getStreamChunks());
                long delayPerChunk = MockConfig.getMockDelayMs() / Math.max(MockConfig.getStreamChunks(), 1);
                
                for (String chunk : chunks) {
                    if (chunk != null && !chunk.isEmpty()) {
                        Thread.sleep(delayPerChunk);
                        emitter.onNext(chunk);
                    }
                }
                emitter.onComplete();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                emitter.onError(e);
            } catch (Exception e) {
                emitter.onError(e);
            }
        }, BackpressureStrategy.BUFFER);
    }

    private String extractTextFromMessage(Message message) {
        if (null == message) return "";
        StringBuilder textBuilder = new StringBuilder();
        if (message.getParts() != null) {
            for (Part part : message.getParts()) {
                if (part instanceof TextPart textPart) {
                    textBuilder.append(textPart.getText());
                }
            }
        }
        return textBuilder.toString();
    }

    private Task createTask(Message request) {
        String id = (request.getTaskId() != null && !request.getTaskId().isEmpty()) 
            ? request.getTaskId() : UUID.randomUUID().toString();
        String contextId = (request.getContextId() != null && !request.getContextId().isEmpty()) 
            ? request.getContextId() : UUID.randomUUID().toString();
        return new Task(id, contextId, new TaskStatus(TaskState.SUBMITTED), 
            null, List.of(request), null);
    }
}

