# exchange — 最小可运行骨架

设计见 [`../docs/architecture.md`](../docs/architecture.md)，实现状态与已知限制见其 §8。

```
core/      Protocol（定长编码）、MatchingEngine（确定性单线程状态机，纯 Java，无 Aeron 依赖）
cluster/   ExchangeService（ClusteredService）、ExchangeNode（单节点集群）、ExchangeClient
archive/   对象存储归档：ObjectStore/MinioObjectStore、LogArchiver、LogTrimmer（受保护的裁剪）、ArchiveCatalog
events/    EventBridge（IPC → 独立线程）、EventSink（Kafka / 日志）、LogReplayer（日志确定性重放补数）、GapFillingConsumer（下游参考实现）
```

```bash
mvn test        # 单元、单节点集成、3 节点故障切换、补数测试（需 JDK 21；已在 pom 中配置 --add-opens）
./run.sh        # 演示：事件打印到控制台
./run.sh -Dkafka.bootstrap=localhost:9092   # 事件写入 Kafka topic exchange.events
```

## 归档测试需要 MinIO

`ArchiveTest` 用真实 MinIO 进程做 S3 归档（PATH 上的 `minio`，或环境变量 `MINIO_BIN`）；找不到可执行文件时这些测试会被**跳过**而不是失败，请确认 CI 上能找到它。
官方已不再提供二进制下载（dl.min.io 返回 410），可以用 Go 从源码编译：

```bash
GOBIN=/usr/local/bin go install github.com/minio/minio@latest   # 需要 Go，首次编译约几分钟
```

## 压测

```bash
./load.sh                      # 1 万活跃账户 × 3 个网关会话：限速看延迟、不限速看吞吐，再做全量一致性校验并投影到 MariaDB
./load.sh sessions -Dsessions=10000 -Dmax=20000   # 直接建 1 万个原始集群会话（会在几百到两千个之后失败，见设计 §5.1）
./load.sh projection -Dbatch=1000                 # 只测 事件 → MariaDB 投影吞吐
./load.sh sequencer -Dmessages=5000000 -Dmembers=1 # 空服务，只测定序器（并校验序号连续、副本一致、重放哈希一致）
XMX=8g ./load.sh correctness -Dops=1000000        # 100 万命令的正确性验证：独立参考实现差分、幂等重试、投影对账、重启重放（需要 MariaDB，约 5 分钟）
```

结果与结论见 [`../docs/architecture.md`](../docs/architecture.md) §5.1。在单机沙箱里跑，数字只用于发现问题，不是容量承诺。
