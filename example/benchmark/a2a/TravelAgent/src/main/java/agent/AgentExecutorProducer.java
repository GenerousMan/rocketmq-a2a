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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class AgentExecutorProducer {
    private static final Logger log = LoggerFactory.getLogger(AgentExecutorProducer.class);
    private static final String DELAY_SECONDS = System.getProperty("delaySeconds", "2");
    private static final String FIXED_RESPONSE = System.getProperty("fixedResponse", 
        "根据天气情况，为您推荐以下行程：\n" +
        "第一天：上午游览西湖，下午参观雷峰塔，晚上品尝杭州特色美食。\n" +
        "第二天：上午前往灵隐寺，下午游览西溪湿地，晚上观看印象西湖演出。\n" +
        "第三天：上午参观中国茶叶博物馆，下午自由活动，晚上返程。");

    @Produces
    public AgentExecutor agentExecutor() {
        return new AgentExecutor() {
            @Override
            public void execute(RequestContext context, EventQueue eventQueue) throws JSONRPCError {
                String userMessage = extractTextFromMessage(context.getMessage());
                log.info("TravelAgent receive userMessage: {}", userMessage);
                
                Task task = context.getTask();
                if (task == null) {
                    task = createTask(context.getMessage());
                    eventQueue.enqueueEvent(task);
                }
                
                TaskUpdater taskUpdater = new TaskUpdater(context, eventQueue);
                try {
                    // 延迟指定秒数
                    int delaySeconds = Integer.parseInt(DELAY_SECONDS);
                    log.info("TravelAgent will delay {} seconds before responding", delaySeconds);
                    TimeUnit.SECONDS.sleep(delaySeconds);
                    
                    // 返回固定内容
                    List<Part<?>> parts = List.of(new TextPart(FIXED_RESPONSE, null));
                    taskUpdater.addArtifact(parts);
                    taskUpdater.complete();
                    log.info("TravelAgent response sent: {}", FIXED_RESPONSE);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    taskUpdater.startWork(taskUpdater.newAgentMessage(
                        List.of(new TextPart("Error: Request interrupted")), Map.of()));
                    taskUpdater.fail();
                    log.error("TravelAgent execution interrupted", e);
                } catch (Exception e) {
                    taskUpdater.startWork(taskUpdater.newAgentMessage(
                        List.of(new TextPart("Error processing request: " + e.getMessage())), Map.of()));
                    taskUpdater.fail();
                    log.error("TravelAgent execution error", e);
                }
            }

            @Override
            public void cancel(RequestContext context, EventQueue eventQueue) throws JSONRPCError {
                Task task = context.getTask();
                if (null == task || null == task.getStatus()) {
                    return;
                }
                if (task.getStatus().state() == TaskState.CANCELED) {
                    throw new TaskNotCancelableError();
                }
                if (task.getStatus().state() == TaskState.COMPLETED) {
                    throw new TaskNotCancelableError();
                }
                TaskUpdater updater = new TaskUpdater(context, eventQueue);
                updater.cancel();
                log.info("TravelAgent task cancelled");
            }
        };
    }

    private String extractTextFromMessage(Message message) {
        if (null == message) {
            return "";
        }
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

    private Task createTask(io.a2a.spec.Message request) {
        String id = !StringUtils.isEmpty(request.getTaskId()) ? request.getTaskId() : UUID.randomUUID().toString();
        String contextId = !StringUtils.isEmpty(request.getContextId()) ? request.getContextId() : UUID.randomUUID().toString();
        return new Task(id, contextId, new TaskStatus(TaskState.SUBMITTED), null, List.of(request), null);
    }
}
