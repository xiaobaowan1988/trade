#!/usr/bin/env bash
# 运行演示：./run.sh [-Dkafka.bootstrap=host:9092]
set -euo pipefail
cd "$(dirname "$0")"
mvn -q -B compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
exec java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util.zip=ALL-UNNAMED \
  --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED "$@" \
  -cp "target/classes:$(cat target/cp.txt)" jp.trade.exchange.Main
