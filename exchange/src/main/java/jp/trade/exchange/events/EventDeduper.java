package jp.trade.exchange.events;

/**
 * 基于状态机事件序号的去重/连续性检查。
 * 撮合集群 leader 切换或重放时，事件可能重复或出现缺口；下游以序号为准：
 * 重复丢弃，缺口告警并触发补数（由 Raft 归档日志重放，骨架暂只告警）。
 */
public final class EventDeduper {
    public enum Verdict { ACCEPT, DUPLICATE, GAP }

    private long last = -1;
    private long gaps;
    private long duplicates;

    public Verdict check(long seq) {
        if (last < 0) {
            last = seq;
            return Verdict.ACCEPT;
        }
        if (seq <= last) {
            duplicates++;
            return Verdict.DUPLICATE;
        }
        final boolean gap = seq != last + 1;
        last = seq;
        if (gap) {
            gaps++;
            return Verdict.GAP;
        }
        return Verdict.ACCEPT;
    }

    public long lastSeq() { return last; }
    public long gaps() { return gaps; }
    public long duplicates() { return duplicates; }
}
