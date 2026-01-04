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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * WeatherAgent HTTP控制器
 * 接收消息并异步处理，通过回调返回结果
 */
@RestController
@RequestMapping("/weather")
public class WeatherAgentController {
    private static final Logger log = LoggerFactory.getLogger(WeatherAgentController.class);
    private static final ExecutorService executorService = Executors.newFixedThreadPool(100);
    private static final OkHttpClient httpClient = new OkHttpClient();
    private static final MediaType JSON_MEDIA_TYPE = MediaType.parse("application/json; charset=utf-8");
    private static final int PROCESS_TIME_SECONDS = Integer.parseInt(System.getProperty("delaySeconds", "2"));
    
    /**
     * 处理天气查询请求
     */
    @PostMapping("/query")
    public String query(@org.springframework.web.bind.annotation.RequestBody String requestBody) {
        try {
            Message message = JSON.parseObject(requestBody, Message.class);
            log.info("[接收消息] MsgID: {}, TaskID: {}, Content: {}", 
                    message.getMessageId(), message.getTaskId(), message.getContent());
            
            // 异步处理
            executorService.submit(() -> processMessage(message));
            
            // 立即返回，表示已接收
            return JSON.toJSONString(new Response(
                    message.getMessageId(), 
                    message.getTaskId(), 
                    message.getSessionId(), 
                    "Accepted", 
                    true
            ));
        } catch (Exception e) {
            log.error("处理请求失败", e);
            return JSON.toJSONString(new Response(null, null, null, null, false));
        }
    }
    
    /**
     * 异步处理消息
     */
    private void processMessage(Message message) {
        try {
            // 模拟LLM处理时间
            log.info("WeatherAgent will delay {} seconds before responding", PROCESS_TIME_SECONDS);
            Thread.sleep(PROCESS_TIME_SECONDS * 1000);
            
            // 生成响应
            String result = "根据您的问题，我为您查询了天气信息。当前天气晴朗，温度适宜。";
            
            // 通过回调返回结果
            if (message.getCallbackUrl() != null && !message.getCallbackUrl().isEmpty()) {
                sendCallback(message, result, true, null);
            }
            
            log.info("[处理完成] MsgID: {}, TaskID: {}, Result: {}", 
                    message.getMessageId(), message.getTaskId(), result);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("[处理失败] MsgID: {}, TaskID: {}", message.getMessageId(), message.getTaskId(), e);
            if (message.getCallbackUrl() != null) {
                sendCallback(message, null, false, e.getMessage());
            }
        } catch (Exception e) {
            log.error("[处理失败] MsgID: {}, TaskID: {}", message.getMessageId(), message.getTaskId(), e);
            if (message.getCallbackUrl() != null) {
                sendCallback(message, null, false, e.getMessage());
            }
        }
    }
    
    /**
     * 发送回调
     */
    private void sendCallback(Message message, String result, boolean success, String error) {
        try {
            Response response = new Response(
                    message.getMessageId(),
                    message.getTaskId(),
                    message.getSessionId(),
                    result,
                    success
            );
            response.setError(error);
            
            okhttp3.RequestBody body = okhttp3.RequestBody.create(JSON.toJSONString(response), JSON_MEDIA_TYPE);
            Request request = new Request.Builder()
                    .url(message.getCallbackUrl())
                    .post(body)
                    .build();
            
            try (okhttp3.Response httpResponse = httpClient.newCall(request).execute()) {
                if (!httpResponse.isSuccessful()) {
                    log.error("[回调失败] MsgID: {}, StatusCode: {}", message.getMessageId(), httpResponse.code());
                } else {
                    log.info("[回调成功] MsgID: {}", message.getMessageId());
                }
            }
        } catch (IOException e) {
            log.error("[回调异常] MsgID: {}", message.getMessageId(), e);
        }
    }
    
    /**
     * 健康检查
     */
    @GetMapping("/health")
    public String health() {
        return "OK";
    }
}
