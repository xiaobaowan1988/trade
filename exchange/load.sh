#!/usr/bin/env bash
# 压测：./load.sh [-Dusers=10000 -Dgateways=3 -DpacedRate=5000 -DpacedSeconds=15 -DsaturationOps=100000 -Dproject=true]
# 另见 ./load.sh sessions（原始集群会话）与 ./load.sh projection（投影吞吐基准）。需要 test 依赖（含 MariaDB），首次运行会下载。
set -euo pipefail
cd "$(dirname "$0")"
mvn -q -B test-compile dependency:build-classpath -Dmdep.outputFile=target/cp-test.txt -Dmdep.includeScope=test
MAIN=jp.trade.exchange.load.LoadRunner
if [ "${1:-}" = "sessions" ]; then MAIN=jp.trade.exchange.load.SessionStorm; shift; fi
if [ "${1:-}" = "projection" ]; then MAIN=jp.trade.exchange.load.ProjectionBench; shift; fi
exec java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util.zip=ALL-UNNAMED \
  --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED -Xmx4g -Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8 "$@" \
  -cp "target/test-classes:target/classes:$(cat target/cp-test.txt)" $MAIN
