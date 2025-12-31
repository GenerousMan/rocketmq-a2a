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
package common;

/**
 * HTTP协议响应封装
 */
public class Response {
    private String messageId;
    private String taskId;
    private String sessionId;
    private String result;
    private boolean success;
    private String error;
    
    public Response() {
    }
    
    public Response(String messageId, String taskId, String sessionId, String result, boolean success) {
        this.messageId = messageId;
        this.taskId = taskId;
        this.sessionId = sessionId;
        this.result = result;
        this.success = success;
    }
    
    public String getMessageId() {
        return messageId;
    }
    
    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }
    
    public String getTaskId() {
        return taskId;
    }
    
    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }
    
    public String getSessionId() {
        return sessionId;
    }
    
    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
    
    public String getResult() {
        return result;
    }
    
    public void setResult(String result) {
        this.result = result;
    }
    
    public boolean isSuccess() {
        return success;
    }
    
    public void setSuccess(boolean success) {
        this.success = success;
    }
    
    public String getError() {
        return error;
    }
    
    public void setError(String error) {
        this.error = error;
    }
}
