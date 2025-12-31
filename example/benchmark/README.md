# Benchmark

## 项目简介

本项目旨在测试 HTTP 协议、A2A 协议和 RocketMQ-A2A 协议在 Multi-Agent 场景下的性能表现。

## 测试场景

本项目主要包含以下两个测试场景：

1. **突发流量应对能力**：测试系统在高并发场景下的性能表现
2. **故障场景容错能力**：测试系统在各类故障情况下的容错和恢复能力

## 目录结构

```
benchmark/
├── a2a/                    # A2A 协议实现的 Multi-Agent 场景
│   ├── SupervisorAgent/
│   ├── TravelAgent/
│   └── WeatherAgent/
├── http/                   # HTTP 协议实现的 Multi-Agent 场景
│   ├── SupervisorAgent/
│   ├── TravelAgent/
│   └── WeatherAgent/
├── rocketmq-a2a/           # RocketMQ-A2A 协议实现的 Multi-Agent 场景
│   ├── SupervisorAgent/
│   ├── TravelAgent/
│   └── WeatherAgent/
└── script/                 # 启动脚本和故障注入脚本
```

## 架构说明

### 协议实现

`a2a/`、`http/` 和 `rocketmq-a2a/` 三个目录分别以不同协议实现了相同的 Multi-Agent 场景，便于进行性能对比测试。

### 测试架构

所有协议实现都包含相同的基本测试场景：

```
用户 <--> Supervisor Agent（双副本部署） <--> Weather Agent（双副本部署）
                                      <--> Travel Agent（双副本部署）
```

### 配置说明

- **LLM 调用模拟**：使用 `sleep` 模拟 LLM 调用，调用时间可配置
- **通信超时设置**：各 Agent 间的通信超时时间相同，设置为 `LLM 预期调用时间 + 10s`，超出即认为调用超时

## 脚本说明

`script/` 目录下包含以下脚本：

1. **启动脚本**：一键拉起 a2a/http/rocketmq-a2a 三种协议的 6 个 Agent（2 个 Supervisor + 2 个 Weather + 2 个 Travel）
2. **网络故障注入脚本**：模拟网络故障场景
   - 丢包
   - TCP 连接中断（tcpkill）
3. **Agent 故障注入脚本**：模拟 Agent 故障场景
   - Hang 机
   - Crash
   - 发布（重启）

## 使用说明

### 启动测试环境

使用 `script/` 目录下的启动脚本一键启动所有 Agent。

### 执行测试

1. 使用启动脚本启动测试环境
2. 根据测试需求执行相应的故障注入脚本
3. 观察和记录各协议在不同场景下的性能表现

## 注意事项

- 确保所有 Agent 的配置参数一致，以保证测试的公平性
- 建议在测试前检查网络环境和系统资源
- 故障注入脚本可能会影响系统稳定性，请在测试环境中使用
