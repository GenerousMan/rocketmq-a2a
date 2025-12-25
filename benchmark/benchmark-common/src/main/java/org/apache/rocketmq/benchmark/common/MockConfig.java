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

/**
 * Mock 配置类 - 集中管理所有 Mock 相关的配置参数
 * 
 * <p>通过系统属性(System Property)进行配置，支持以下参数：
 * <ul>
 *   <li>mockDelayMs - 模拟延迟时间（毫秒，默认 1000）</li>
 *   <li>mockStreamChunks - 流式输出分片数（默认 5）</li>
 *   <li>mockWeatherResponse - 天气Agent模拟响应</li>
 *   <li>mockTravelResponse - 行程Agent模拟响应</li>
 *   <li>mockSupervisorMode - Supervisor响应模式</li>
 * </ul>
 */
public class MockConfig {
    
    // ============ 延迟配置 ============
    
    /** 模拟延迟时间（毫秒） */
    private static final long MOCK_DELAY_MS = Long.parseLong(
        System.getProperty("mockDelayMs", "1000")
    );
    
    /** 流式输出的分片数量 */
    private static final int STREAM_CHUNKS = Integer.parseInt(
        System.getProperty("mockStreamChunks", "5")
    );
    
    // ============ 响应内容配置 ============
    
    /** 天气Agent默认响应 */
    public static final String DEFAULT_WEATHER_RESPONSE = 
        "杭州明天天气晴朗，气温15-25度，湿度60%，微风。非常适合户外活动，建议穿着轻便舒适的衣物。";
    
    /** 行程Agent默认响应 */
    public static final String DEFAULT_TRAVEL_RESPONSE = 
        "已为您规划好行程：\n" +
        "Day 1: 上午游览西湖，下午参观雷峰塔\n" +
        "Day 2: 上午灵隐寺参拜，下午品尝当地美食\n" +
        "Day 3: 自由活动，推荐南宋御街购物\n" +
        "祝您旅途愉快！";
    
    /** Supervisor分发到WeatherAgent的响应 */
    public static final String SUPERVISOR_WEATHER_DISPATCH = 
        "{\"messageInfo\":\"杭州明天的天气情况怎么样?\",\"agent\":\"WeatherAgent\"}";
    
    /** Supervisor分发到TravelAgent的响应 */
    public static final String SUPERVISOR_TRAVEL_DISPATCH = 
        "{\"messageInfo\":\"请帮我规划杭州到上海的2天行程\",\"agent\":\"TravelAgent\"}";
    
    /** Supervisor直接响应 */
    public static final String SUPERVISOR_DIRECT_RESPONSE = 
        "根据您的需求，我已经为您查询了相关信息。行程规划已完成。";
    
    // ============ Supervisor响应模式 ============
    
    public enum SupervisorMode {
        /** 自动根据输入内容决定响应 */
        AUTO,
        /** 总是分发到 WeatherAgent */
        WEATHER_DISPATCH,
        /** 总是分发到 TravelAgent */
        TRAVEL_DISPATCH,
        /** 直接响应，不分发 */
        DIRECT,
        /** 使用自定义响应 */
        CUSTOM
    }
    
    // ============ Getter 方法 ============
    
    public static long getMockDelayMs() {
        return MOCK_DELAY_MS;
    }
    
    public static int getStreamChunks() {
        return STREAM_CHUNKS;
    }
    
    public static String getWeatherResponse() {
        return System.getProperty("mockWeatherResponse", DEFAULT_WEATHER_RESPONSE);
    }
    
    public static String getTravelResponse() {
        return System.getProperty("mockTravelResponse", DEFAULT_TRAVEL_RESPONSE);
    }
    
    public static SupervisorMode getSupervisorMode() {
        String mode = System.getProperty("mockSupervisorMode", "AUTO");
        try {
            return SupervisorMode.valueOf(mode.toUpperCase());
        } catch (IllegalArgumentException e) {
            return SupervisorMode.AUTO;
        }
    }
    
    public static String getCustomSupervisorResponse() {
        return System.getProperty("mockSupervisorResponse", SUPERVISOR_DIRECT_RESPONSE);
    }
    
    /**
     * 将文本分割为指定数量的块（用于模拟流式输出）
     */
    public static String[] splitIntoChunks(String text, int numChunks) {
        if (numChunks <= 0) numChunks = 1;
        if (text == null || text.isEmpty()) return new String[]{""};
        
        int chunkSize = (int) Math.ceil((double) text.length() / numChunks);
        String[] chunks = new String[numChunks];
        
        for (int i = 0; i < numChunks; i++) {
            int start = i * chunkSize;
            int end = Math.min(start + chunkSize, text.length());
            chunks[i] = start < text.length() ? text.substring(start, end) : "";
        }
        return chunks;
    }
    
    /**
     * 根据输入自动决定Supervisor响应（用于AUTO模式）
     */
    public static String autoDetectSupervisorResponse(String userInput) {
        if (userInput == null) {
            return SUPERVISOR_DIRECT_RESPONSE;
        }
        String lowerInput = userInput.toLowerCase();
        
        // 检查是否是来自其他 Agent 的响应（包含天气信息）
        if (lowerInput.contains("天气") && lowerInput.contains("度")) {
            // 这是 WeatherAgent 的响应，需要继续处理或结束
            if (lowerInput.contains("行程") || lowerInput.contains("规划")) {
                return SUPERVISOR_TRAVEL_DISPATCH;
            }
            return SUPERVISOR_DIRECT_RESPONSE;
        }
        
        // 用户询问天气
        if (lowerInput.contains("天气") || lowerInput.contains("气温") 
            || lowerInput.contains("下雨") || lowerInput.contains("晴")) {
            return SUPERVISOR_WEATHER_DISPATCH;
        }
        
        // 用户询问行程
        if (lowerInput.contains("行程") || lowerInput.contains("规划") 
            || lowerInput.contains("出行") || lowerInput.contains("旅游")) {
            return SUPERVISOR_TRAVEL_DISPATCH;
        }
        
        return SUPERVISOR_DIRECT_RESPONSE;
    }
}

