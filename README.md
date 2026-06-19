# debezium-demo

基于 **Spring Boot 4.1.0** + **Debezium 3.5.2.Final** 的嵌入式 CDC（Change Data Capture）演示项目，用于捕获 MySQL 数据库的 binlog 变更事件并持久化为 JSON 文件。

## 技术栈

| 组件 | 版本 |
|---|---|
| Java | 17 |
| Spring Boot | 4.1.0 |
| Debezium | 3.5.2.Final |
| Maven Wrapper | 3.3.4 |

核心依赖：
- `debezium-embedded` — 嵌入式引擎，在应用内直接运行
- `debezium-connector-mysql` — MySQL binlog CDC 连接器
- `debezium-storage-file` — 基于文件的偏移量 & Schema 历史持久化
- `hutool-all`, `tsid-creator` — JSON 处理 & ID 生成

## 快速开始

### 1. 准备 MySQL

```sql
CREATE USER canal IDENTIFIED WITH mysql_native_password BY 'canal';
GRANT SELECT, SHOW DATABASES, LOCK TABLES, RELOAD, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
FLUSH PRIVILEGES;
```

确保 MySQL 开启了 binlog（`log_bin=ON`），建议 binlog 格式为 `ROW`。

### 2. 配置

编辑 `src/main/resources/application.yaml`，按需调整：

```yaml
debezium:
  config-properties: |
    database.hostname=127.0.0.1
    database.port=34306
    database.user=canal
    database.password=canal
    database.include.list=mytest2
    table.include.list=mytest2.*
    snapshot.mode=schema_only
    topic.prefix=test
```

### 3. 构建 & 运行

```bash
# 构建
./mvnw clean package -DskipTests

# 运行
java -jar target/debezium-demo.jar
```

## 项目结构

```
src/main/java/com/dyrnq/debezium/
├── DebeziumJavaApplication.java        # 入口（非 Web 模式）
├── DebeziumEmbeddedConfig.java         # 从 YAML 读取 Debezium 配置
├── DebeziumEmbeddedRunner.java         # 启动嵌入式引擎，消费 CDC 事件
└── util/
    └── DebeziumUtil.java               # 事件持久化、偏移量解析、配置打印
```

## 工作原理

1. 应用启动 → `DebeziumEmbeddedConfig` 解析 `application.yaml` 中的 Debezium 配置
2. `DebeziumEmbeddedRunner` 创建 `DebeziumEngine`（JSON 格式输出）
3. 引擎连接到 MySQL，根据 `snapshot.mode` 执行初始快照（`schema_only` 仅捕获表结构）
4. 进入 CDC 流式阶段，实时消费 binlog 变更事件
5. 每条变更通过 `DebeziumUtil.save()` 写入 `json/dbz-{tsid}.json`

## 快照模式

| 模式 | 说明 |
|---|---|
| `schema_only` | 仅捕获表结构，不做数据快照，直接进入 CDC |
| `initial` | 先执行 SELECT 快照捕获现有数据，再进入 CDC |
| `when_needed` | 仅在无偏移量或偏移量失效时快照 |
| `never` | 跳过快照，直接 CDC（已标记弃用） |
| `no_data` | 同 `schema_only`，推荐替代 |

## 事件输出

每条 JSON 事件文件包含完整的 Debezium 变更记录：

- `"op": "r"` — 快照读取（read）
- `"op": "c"` — 插入（create）
- `"op": "u"` — 更新（update）
- `"op": "d"` — 删除（delete）

可通过 `__source.snapshot` 字段区分快照事件与 CDC 事件。

## 配置参考

参见 [Debezium MySQL Connector 配置文档](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-connector-properties)。

完整默认配置列表可通过测试方法打印：

```bash
./mvnw test -Dtest=DebeziumUtilTest#defaultMySqlConnectorConfig
```

## 参考

- [Debezium EmbeddedEngine 测试用例](https://github.com/debezium/debezium/blob/main/debezium-embedded/src/test/java/io/debezium/embedded/EmbeddedEngineTest.java)
- [Debezium 官方文档](https://debezium.io/documentation/)
