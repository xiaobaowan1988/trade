package jp.trade.exchange;

import static jp.trade.exchange.core.Protocol.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import jp.trade.exchange.archive.AeronFrames;
import jp.trade.exchange.cluster.ExchangeClient;
import jp.trade.exchange.cluster.ExchangeNode;
import jp.trade.exchange.events.LoggingEventSink;
import org.junit.jupiter.api.Test;

/**
 * 磁盘上的日志出现静默位翻转：没有校验时，重启会照常服务，但状态已经悄悄变了（这是“出错”）；
 * 启用 CRC32C 后，重放在该帧处失败，节点拒绝对外服务（这是“挂”）。
 */
class StorageIntegrityTest {
    @Test
    void aBitFlipInTheLogOnDiskMakesTheNodeRefuseToServeInsteadOfServingWrongState() throws Exception {
        final File dir = Files.createTempDirectory("crc").toFile();
        final File archiveDir;
        try (ExchangeNode node = ExchangeNode.start(dir, true, new LoggingEventSink(false));
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            c.deposit(1, ASSET_QUOTE, 1_000_000);
            for (int i = 1; i <= 50; i++) {
                c.place(1, SIDE_BUY, i, 100, 1);
            }
            c.await(51, 15_000);
            archiveDir = node.archiveDir();
        }

        // 翻转日志录制第一个 segment 里第 20 条命令负载中的一个比特
        final File seg = new File(archiveDir, "0-0.rec");
        final byte[] data = Files.readAllBytes(seg.toPath());
        final int[] target = {-1};
        final int[] n = {0};
        AeronFrames.parse(data, 0, 0, Long.MAX_VALUE, new AeronFrames.Reassembler(), (buf, off, len, pos) -> {
            if (len >= 8 + 24 + 48 && ++n[0] == 20) {
                target[0] = off + 8 + 24 + 16 + 3;
            }
        });
        assertTrue(target[0] > 0);
        try (RandomAccessFile f = new RandomAccessFile(seg, "rw")) {
            f.seek(target[0]);
            final int b = f.read();
            f.seek(target[0]);
            f.write(b ^ 0x04);
        }

        final List<String> errors = new CopyOnWriteArrayList<>();
        final LoggingEventSink sink = new LoggingEventSink(false);
        try (ExchangeNode node = ExchangeNode.start(0, 1, dir, false, sink, cfg -> cfg.errorHandler(e -> errors.add(String.valueOf(e))))) {
            assertThrows(Exception.class, () -> {
                try (ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
                    c.deposit(9, ASSET_QUOTE, 5);
                    c.await(1, 3_000);
                }
            }, "损坏的日志不能被用来对外服务");
            final long deadline = System.currentTimeMillis() + 5_000;
            while (errors.stream().noneMatch(e -> e.contains("CRC checksum mismatch")) && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertTrue(errors.stream().anyMatch(e -> e.contains("CRC checksum mismatch")), "明确报告校验和不符: " + errors);
            assertEquals(0, sink.events().size(), "没有发布任何事件");
        }
    }
}
