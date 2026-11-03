package jp.trade.exchange.archive;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * 解析 Aeron 原始 term 数据（即 Archive segment 文件的内容）：
 * 32 字节帧头、帧按 32 字节对齐、term 尾部是 padding 帧，分片消息需要重组。
 * 产出的是与订阅者 poll 到的完全相同的消息，因此可直接喂给线上同一套命令解码。
 */
public final class AeronFrames {
    private AeronFrames() {}

    public static final int HEADER_LENGTH = 32;
    public static final int FRAME_ALIGNMENT = 32;
    private static final int HDR_TYPE_DATA = 0x01;
    private static final int BEGIN_FLAG = 0x80;
    private static final int END_FLAG = 0x40;

    @FunctionalInterface
    public interface MessageHandler {
        /** @param position 该消息最后一帧之后的数据位置（绝对日志位置） */
        void onMessage(DirectBuffer buffer, int offset, int length, long position);
    }

    /** 跨 segment 保持的分片重组状态。 */
    public static final class Reassembler {
        private final MutableDirectBuffer buf = new ExpandableArrayBuffer(1024);
        private int length;
        private boolean inMessage;

        public boolean hasPartialMessage() { return inMessage; }
    }

    /**
     * @param data      segment 文件内容
     * @param from      起始文件偏移（必须在帧边界上）
     * @param basePos   该文件偏移 0 对应的绝对日志位置
     * @param stopAtPos 位置 >= 此值的帧不再处理（Long.MAX_VALUE 表示到数据结束）
     * @return 解析停止处的文件偏移
     */
    public static int parse(byte[] data, int from, long basePos, long stopAtPos, Reassembler re, MessageHandler handler) {
        final UnsafeBuffer buf = new UnsafeBuffer(data);
        int off = from;
        while (off + HEADER_LENGTH <= data.length) {
            if (basePos + off >= stopAtPos) {
                break;
            }
            final int frameLength = buf.getInt(off, java.nio.ByteOrder.LITTLE_ENDIAN);
            if (frameLength <= 0) {
                break; // 未写入区域
            }
            final int type = buf.getShort(off + 6, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
            final int flags = buf.getByte(off + 5) & 0xFF;
            final int next = off + align(frameLength);
            if (type == HDR_TYPE_DATA) {
                final int payloadOffset = off + HEADER_LENGTH;
                final int payloadLength = frameLength - HEADER_LENGTH;
                final long endPos = basePos + next;
                if ((flags & (BEGIN_FLAG | END_FLAG)) == (BEGIN_FLAG | END_FLAG)) {
                    handler.onMessage(buf, payloadOffset, payloadLength, endPos);
                } else {
                    if ((flags & BEGIN_FLAG) != 0) {
                        re.length = 0;
                        re.inMessage = true;
                    }
                    if (re.inMessage) {
                        re.buf.putBytes(re.length, buf, payloadOffset, payloadLength);
                        re.length += payloadLength;
                        if ((flags & END_FLAG) != 0) {
                            re.inMessage = false;
                            handler.onMessage(re.buf, 0, re.length, endPos);
                        }
                    }
                }
            } // 其他类型（padding 帧）直接跳过
            off = next;
        }
        return off;
    }

    private static int align(int length) {
        return (length + FRAME_ALIGNMENT - 1) & -FRAME_ALIGNMENT;
    }
}
