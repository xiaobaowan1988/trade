package jp.trade.exchange.core;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * 服务快照在 Aeron 快照流里的分块格式：int MAGIC | int total | int offset | bytes。
 * 快照录制里还夹着集群自己的开始/结束标记帧；魔数让读取方能分辨并跳过它们，
 * 因此集群服务自己加载快照和补数进程从 Archive 读取快照共用这一份解析。
 */
public final class SnapshotFraming {
    public static final int MAGIC = 0x4E58534E; // "NXSN"
    public static final int HEADER = 12;
    public static final int CHUNK = 1000;

    private SnapshotFraming() {}

    /** 把 state[off..off+n) 编码成一个分块，返回消息总长度。 */
    public static int encodeChunk(MutableDirectBuffer buf, byte[] state, int off) {
        final int n = Math.min(CHUNK, state.length - off);
        buf.putInt(0, MAGIC);
        buf.putInt(4, state.length);
        buf.putInt(8, off);
        buf.putBytes(HEADER, state, off, n);
        return HEADER + n;
    }

    public static final class Reader {
        private byte[] data;
        private int received;

        /** @return true 表示该帧是快照分块并已消费；false 表示不是（例如集群标记帧），应忽略 */
        public boolean accept(DirectBuffer buf, int offset, int length) {
            if (length < HEADER || buf.getInt(offset) != MAGIC) {
                return false;
            }
            final int total = buf.getInt(offset + 4);
            final int off = buf.getInt(offset + 8);
            if (data == null) {
                data = new byte[total];
            }
            buf.getBytes(offset + HEADER, data, off, length - HEADER);
            received += length - HEADER;
            return true;
        }

        public boolean complete() { return data != null && received >= data.length; }

        public byte[] bytes() {
            if (!complete()) {
                throw new IllegalStateException("incomplete snapshot");
            }
            return data;
        }
    }
}
