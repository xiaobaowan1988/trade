# exchange — 最小可运行骨架

设计见 [`../docs/architecture.md`](../docs/architecture.md)，实现状态与已知限制见其 §8。

```
core/      Protocol（定长编码）、MatchingEngine（确定性单线程状态机，纯 Java，无 Aeron 依赖）
cluster/   ExchangeService（ClusteredService）、ExchangeNode（单节点集群）、ExchangeClient
events/    EventBridge（IPC → 独立线程）、EventSink（Kafka / 日志）、EventDeduper
```

```bash
mvn test        # 单元 + 集群集成测试（需 JDK 21；已在 pom 中配置 --add-opens）
./run.sh        # 演示：事件打印到控制台
./run.sh -Dkafka.bootstrap=localhost:9092   # 事件写入 Kafka topic exchange.events
```
