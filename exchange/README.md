# exchange — 最小可运行骨架

设计见 [`../docs/architecture.md`](../docs/architecture.md)，实现状态与已知限制见其 §8。

```
core/      Protocol（定长编码）、MatchingEngine（确定性单线程状态机，纯 Java，无 Aeron 依赖）
cluster/   ExchangeService（ClusteredService）、ExchangeNode（单节点集群）、ExchangeClient
events/    EventBridge（IPC → 独立线程）、EventSink（Kafka / 日志）、LogReplayer（日志确定性重放补数）、GapFillingConsumer（下游参考实现）
```

```bash
mvn test        # 单元、单节点集成、3 节点故障切换、补数测试（需 JDK 21；已在 pom 中配置 --add-opens）
./run.sh        # 演示：事件打印到控制台
./run.sh -Dkafka.bootstrap=localhost:9092   # 事件写入 Kafka topic exchange.events
```
