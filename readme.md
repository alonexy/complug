# Complug 项目开发约束与规范 (Official Specification)

## 1. 架构核心原则
Complug 采用 **Monorepo (单仓多模块)** 架构，核心目标是实现极致的依赖隔离与能力标准化。

* **绝对解耦**：`components/`（标准层）严禁依赖 `contrib/`（实现层）。
* **零污染**：`components/` 模块禁止引入任何非 Go 标准库的第三方依赖。
* **物理隔离**：每个子目录必须拥有独立的 `go.mod` 文件，确保用户可以“按需引入”，避免引入无关的三方驱动。

---

## 2. 目录层级规范

### 2.1 /components (Domain Interfaces)
该目录定义各领域的抽象标准（插座标准）。
* **职责**：定义 `interface`、基础数据模型 `struct`、枚举常量和 Sentinel Errors。
* **约束**：
    * 必须包含独立的 `go.mod`。
    * 文件名规范：`api.go` (存放接口定义), `types.go` (存放通用模型)。
    * **禁止**包含具体实现逻辑（如连接池管理、协议解析等）。

### 2.2 /contrib (Implementation Adapters)
该目录提供针对第三方库的具体适配实现（插头）。
* **职责**：将第三方 SDK（如 Kafka, Redis, GORM）的功能适配到 `components` 定义的接口上。
* **约束**：
    * 目录结构必须与领域对应：`/contrib/{domain}/{driver}`。
    * 必须包含独立的 `go.mod`。
    * 开发阶段必须通过 `replace` 指令引用本地的 `components` 模块。

---

## 3. 代码实现规范

### 3.1 接口设计 (Interface Design)
* **上下文支持**：所有涉及 I/O、网络或耗时操作的方法，第一个参数必须是 `context.Context`。
* **错误处理**：驱动层需负责将三方 SDK 的原始错误包装或转换为 `components` 定义的标准错误。
* **类型安全**：在处理 Payload 数据或数据库 Record 时，优先使用 Go 泛型（Generics）。

### 3.2 构造函数 (Constructor)
* 每个 `contrib` 模块必须暴露一个 `New{Driver}Provider` 或 `New{Driver}Client` 函数。
* **返回类型强制**：函数必须返回 `components` 中定义的 **Interface**，而非驱动层的具体结构体指针。
    * *OK*: `func NewKafka(...) (queue.Provider, error)`
    * *Err*: `func NewKafka(...) *KafkaAdapter`

---

## 4. 依赖管理规范

* **Go 版本**：全项目 Go 版本必须对齐（建议 `1.21` 及以上）。
* **依赖声明原则**：
    * `components` 模块的依赖树深度应尽可能为 0（仅限标准库）。
    * `contrib` 模块仅能引用其对应的 `components` 模块及该驱动必要的三方 SDK。
* **模块路径命名**：
    * 核心层：`github.com/{org}/complug/components/{domain}`
    * 实现层：`github.com/{org}/complug/contrib/{domain}/{driver}`

### 4.1 版本依赖示例

```bash
# Kafka 适配
go get github.com/alonexy/complug/contrib/queue/kafka@v1.0.1

# RabbitMQ 适配
go get github.com/alonexy/complug/contrib/queue/rabbitmq@v1.0.1
```

---

## 5. AI 协作指令 (AI Prompt Guardrails)

当使用 AI 辅助开发时，请严格遵守以下逻辑：
1.  **新建组件顺序**：先在 `components/` 下定义纯接口，并初始化 `go.mod`；确认无误后，再在 `contrib/` 下开发实现。
2.  **依赖冲突检查**：若 AI 试图在 `components/` 中使用 `go get` 引入第三方包，必须立即中止并报错。
3.  **Monorepo 自动适配**：生成代码时，AI 应自动识别目录深度并正确编写 `go.mod` 中的 `replace` 路径（如 `../../components/xxx`）。

---

## 6. 模块扩展 Checklist

- [ ] 1. 在 `components/{domain}` 下定义标准接口。
- [ ] 2. 在该目录下运行 `go mod init`。
- [ ] 3. 在 `contrib/{domain}/{driver}` 下创建对应的实现目录。
- [ ] 4. 在实现目录下运行 `go mod init` 并 `replace` 接口模块。
- [ ] 5. 编写适配器逻辑并实现所有接口契约。
- [ ] 6. 编写单元测试，确保驱动完全符合接口预期。
