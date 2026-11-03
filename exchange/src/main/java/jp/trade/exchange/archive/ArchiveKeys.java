package jp.trade.exchange.archive;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 对象键布局与哈希工具。键格式见设计文档 §4.3。 */
public final class ArchiveKeys {
    private ArchiveKeys() {}

    private static final Pattern SEGMENT = Pattern.compile(".*/log/(\\d+)/(\\d{20})\\.rec$");
    private static final Pattern SNAPSHOT = Pattern.compile(".*/snapshots/(\\d{20})-(\\d{20})\\.bin$");

    public static String segment(String prefix, long recordingId, long basePosition) {
        return String.format("%s/log/%d/%020d.rec", prefix, recordingId, basePosition);
    }

    public static String segmentPrefix(String prefix, long recordingId) {
        return String.format("%s/log/%d/", prefix, recordingId);
    }

    public static String meta(String prefix, long recordingId) {
        return String.format("%s/log/%d/meta.json", prefix, recordingId);
    }

    public static String snapshot(String prefix, long logPosition, long lastSeq) {
        return String.format("%s/snapshots/%020d-%020d.bin", prefix, logPosition, lastSeq);
    }

    public static String snapshotPrefix(String prefix) {
        return prefix + "/snapshots/";
    }

    /** @return segment 的 basePosition，不是 segment 键则返回 -1 */
    public static long parseSegmentBase(String key) {
        final Matcher m = SEGMENT.matcher(key);
        return m.matches() ? Long.parseLong(m.group(2)) : -1;
    }

    /** @return {logPosition, lastSeq}，不是快照键则返回 null */
    public static long[] parseSnapshot(String key) {
        final Matcher m = SNAPSHOT.matcher(key);
        return m.matches() ? new long[]{Long.parseLong(m.group(1)), Long.parseLong(m.group(2))} : null;
    }

    public static String sha256(byte[] data) {
        try {
            return hex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static String sha256(java.io.File f) {
        try (java.io.InputStream in = new java.io.FileInputStream(f)) {
            final MessageDigest md = MessageDigest.getInstance("SHA-256");
            final byte[] b = new byte[1 << 20];
            int n;
            while ((n = in.read(b)) > 0) {
                md.update(b, 0, n);
            }
            return hex(md.digest());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String hex(byte[] d) {
        final StringBuilder sb = new StringBuilder();
        for (byte x : d) {
            sb.append(String.format("%02x", x));
        }
        return sb.toString();
    }

    public static byte[] json(long startPosition, long segmentFileLength) {
        return ("{\"startPosition\":" + startPosition + ",\"segmentFileLength\":" + segmentFileLength + "}")
                .getBytes(StandardCharsets.UTF_8);
    }

    /** 极简解析：meta.json 只含两个整数字段。 */
    public static long[] parseJson(byte[] json) {
        final String s = new String(json, StandardCharsets.UTF_8);
        final Matcher a = Pattern.compile("\"startPosition\":(\\d+)").matcher(s);
        final Matcher b = Pattern.compile("\"segmentFileLength\":(\\d+)").matcher(s);
        if (!a.find() || !b.find()) {
            throw new IllegalStateException("bad meta.json: " + s);
        }
        return new long[]{Long.parseLong(a.group(1)), Long.parseLong(b.group(1))};
    }
}
