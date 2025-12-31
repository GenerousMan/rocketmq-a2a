# HTTP协议 Multi-Agent 实现

## 项目简介

本项目实现了基于HTTP协议的Multi-Agent场景，Supervisor Agent与Weather Agent和Travel Agent通过简单的HTTP协议进行异步交互。

## 架构说明

```
SupervisorAgent (HTTP客户端/服务端)
    |
    ├─> WeatherAgent (HTTP服务端)
    │   - 接收查询请求
    │   - 异步处理（模拟LLM处理，2秒延迟）
    │   - 通过回调返回结果
    │
    └─> TravelAgent (HTTP服务端)
        - 接收规划请求
        - 异步处理（模拟LLM处理，2秒延迟）
        - 通过回调返回结果
```

### 通信流程

1. **SupervisorAgent** 发送HTTP请求到WeatherAgent/TravelAgent
2. **WeatherAgent/TravelAgent** 立即返回"已接收"响应
3. **WeatherAgent/TravelAgent** 异步处理任务（模拟LLM调用）
4. **WeatherAgent/TravelAgent** 处理完成后，通过HTTP回调发送结果到SupervisorAgent
5. **SupervisorAgent** 接收回调，记录完成时间和统计信息

## 目录结构

```
http/
├── Common/                    # 公共类（Message、Response）
├── WeatherAgent/              # 天气查询Agent
│   ├── src/main/java/agent/
│   │   ├── WeatherAgentApplication.java
│   │   └── WeatherAgentController.java
│   └── pom.xml
├── TravelAgent/               # 行程规划Agent
│   ├── src/main/java/agent/
│   │   ├── TravelAgentApplication.java
│   │   └── TravelAgentController.java
│   └── pom.xml
├── SupervisorAgent/           # 协调Agent（批量压测）
│   ├── src/main/java/agent/
│   │   └── SimpleSupervisorAgent.java
│   └── pom.xml
└── pom.xml                    # 父POM
```

## 快速开始

### 1. 构建项目

```bash
cd /Users/juntao/rocketmq-a2a/example/benchmark/http
mvn clean package -DskipTests
```

### 2. 快速测试

使用提供的测试脚本一键启动所有Agent并进行测试：

```bash
cd /Users/juntao/rocketmq-a2a/example/benchmark/script
./test_http.sh
```

该脚本会：
- 启动WeatherAgent（端口8080）
- 启动TravelAgent（端口8888）
- 启动SupervisorAgent（端口9090）并发送测试消息
- 运行10秒后自动完成并显示统计信息

### 3. 手动启动Agent

#### 3.1 启动WeatherAgent

```bash
cd /Users/juntao/rocketmq-a2a/example/benchmark/script
./start_agent.sh http weather 8080
```

#### 3.2 启动TravelAgent

```bash
./start_agent.sh http travel 8888
```

#### 3.3 启动多副本Agent（负载均衡）

```bash
# 启动2个WeatherAgent实例
./start_agents_batch.sh http weather 8080 2

# 启动2个TravelAgent实例
./start_agents_batch.sh http travel 8888 2
```

#### 3.4 启动SupervisorAgent进行压测

```bash
cd /Users/juntao/rocketmq-a2a/example/benchmark/http/SupervisorAgent

# 单副本测试
java -Dserver.port=9090 \
     -DweatherAgentUrls="http://localhost:8080" \
     -DtravelAgentUrls="http://localhost:8888" \
     -DtestMessage="今天天气怎么样？" \
     -Dqps=5 \
     -DmaxTestTime=30 \
     -DstatsInterval=10 \
     -jar target/HttpSupervisorAgent-1.0.0-SNAPSHOT.jar

# 多副本负载均衡测试
java -Dserver.port=9090 \
     -DweatherAgentUrls="http://localhost:8080,http://localhost:8081" \
     -DtravelAgentUrls="http://localhost:8888,http://localhost:8889" \
     -DtestMessage="今天天气怎么样？" \
     -Dqps=10 \
     -DmaxTestTime=60 \
     -DstatsInterval=10 \
     -DmaxRetryTimes=3 \
     -jar target/HttpSupervisorAgent-1.0.0-SNAPSHOT.jar
```

### 4. 停止Agent

```bash
# 停止所有Agent
cd /Users/juntao/rocketmq-a2a/example/benchmark/script
./stop_agent.sh all

# 停止指定端口的Agent
./stop_agent.sh 8080
```

## SupervisorAgent参数说明

| 参数 | 说明 | 示例 | 默认值 |
|------|------|------|--------|
| `weatherAgentUrls` | WeatherAgent URL列表（逗号分隔） | `http://localhost:8080,http://localhost:8081` | `http://localhost:8080` |
| `travelAgentUrls` | TravelAgent URL列表（逗号分隔） | `http://localhost:8888,http://localhost:8889` | `http://localhost:8888` |
| `testMessage` | 测试消息内容（必需） | `今天天气怎么样？` | 无 |
| `qps` | 每秒发送消息数 | `10` | `1` |
| `maxTestTime` | 最大测试时间（秒） | `60` | 无限制 |
| `statsInterval` | 统计信息打印间隔（秒） | `10` | `10` |
| `maxRetryTimes` | 最大重试次数 | `3` | `3` |
| `server.port` | SupervisorAgent回调端口 | `9090` | `9090` |

## 主要特性

### 1. 异步通信
- Agent接收请求后立即返回，不阻塞客户端
- 通过HTTP回调机制返回处理结果
- 支持高并发场景

### 2. 负载均衡
- 支持配置多个Agent副本URL
- 轮询方式自动选择不同的Agent实例
- 提高系统吞吐量和可用性

### 3. 故障重试
- 请求失败时自动切换到下一个Agent副本
- 可配置最大重试次数
- 提高系统容错性

### 4. 性能统计
- 实时显示触发数、完成数、待完成数
- 计算触发成功率和完成率
- 统计耗时分布（平均值、P50、P90、P99）
- 定时打印统计信息和最终报告

### 5. 关键词路由
- 根据消息内容关键词自动路由到对应Agent
- "天气" -> WeatherAgent
- "行程" -> TravelAgent

## 压测场景示例

### 场景1: 突发流量测试

测试系统在高QPS下的表现：

```bash
java -Dserver.port=9090 \
     -DweatherAgentUrls="http://localhost:8080,http://localhost:8081" \
     -DtravelAgentUrls="http://localhost:8888,http://localhost:8889" \
     -DtestMessage="今天天气怎么样？" \
     -Dqps=50 \
     -DmaxTestTime=60 \
     -DstatsInterval=5 \
     -jar target/HttpSupervisorAgent-1.0.0-SNAPSHOT.jar
```

### 场景2: 故障恢复测试

1. 启动多副本Agent
2. 运行压测
3. 中途手动停止部分Agent实例
4. 观察系统自动切换到其他副本
5. 重启被停止的Agent
6. 观察系统恢复正常

### 场景3: 长时间稳定性测试

```bash
java -Dserver.port=9090 \
     -DweatherAgentUrls="http://localhost:8080,http://localhost:8081,http://localhost:8082" \
     -DtravelAgentUrls="http://localhost:8888,http://localhost:8889,http://localhost:8890" \
     -DtestMessage="今天天气怎么样？" \
     -Dqps=10 \
     -DmaxTestTime=3600 \
     -DstatsInterval=60 \
     -jar target/HttpSupervisorAgent-1.0.0-SNAPSHOT.jar
```

## 技术栈

- **Java**: 17
- **Spring Boot**: 3.2.0
- **OkHttp**: 4.12.0 (HTTP客户端)
- **Fastjson**: 1.2.83 (JSON处理)
- **Maven**: 构建工具

## 注意事项

1. **端口占用**: 确保使用的端口未被占用
2. **回调地址**: SupervisorAgent需要能够被WeatherAgent和TravelAgent访问
3. **并发控制**: WeatherAgent和TravelAgent使用固定大小的线程池（100线程），根据实际情况调整
4. **超时设置**: HTTP客户端连接超时、读写超时均为10秒
5. **日志查看**: 日志文件位于各Agent的`logs/`目录下

## 与RocketMQ-A2A协议对比

| 特性 | HTTP协议 | RocketMQ-A2A协议 |
|------|----------|------------------|
| 通信方式 | HTTP请求+回调 | 消息队列 |
| 解耦程度 | 较低（需要知道对方URL） | 高（通过Topic解耦） |
| 可靠性 | 依赖HTTP重试 | 消息持久化+重试 |
| 流量削峰 | 不支持 | 支持 |
| 实现复杂度 | 简单 | 复杂 |
| 适用场景 | 简单场景、点对点通信 | 复杂场景、高可靠性要求 |

## 常见问题

### 1. Agent启动失败

检查端口是否被占用：
```bash
lsof -i :8080
```

### 2. 回调失败

确保SupervisorAgent的端口（默认9090）可以被访问

### 3. 构建失败

确保Maven和JDK版本正确：
```bash
mvn -v  # 需要Maven 3.9+
java -version  # 需要JDK 17+
```

## 许可证

Apache License 2.0
