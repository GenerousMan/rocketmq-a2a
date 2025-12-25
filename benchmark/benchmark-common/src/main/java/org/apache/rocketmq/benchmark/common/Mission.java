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
 * 任务分发消息结构 - 用于Supervisor向Worker Agent分发任务
 */
public class Mission {
    
    /** 消息内容 */
    private String messageInfo;
    
    /** 目标Agent名称 */
    private String agent;
    
    public Mission() {
    }
    
    public Mission(String messageInfo, String agent) {
        this.messageInfo = messageInfo;
        this.agent = agent;
    }
    
    public String getMessageInfo() {
        return messageInfo;
    }
    
    public void setMessageInfo(String messageInfo) {
        this.messageInfo = messageInfo;
    }
    
    public String getAgent() {
        return agent;
    }
    
    public void setAgent(String agent) {
        this.agent = agent;
    }
    
    @Override
    public String toString() {
        return "Mission{" +
            "messageInfo='" + messageInfo + '\'' +
            ", agent='" + agent + '\'' +
            '}';
    }
}

