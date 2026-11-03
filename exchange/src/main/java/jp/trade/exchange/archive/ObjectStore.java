package jp.trade.exchange.archive;

import java.io.File;
import java.util.List;
import java.util.Map;

/** 对象存储抽象（S3 / MinIO）。键是完整的对象路径。 */
public interface ObjectStore {
    record Stat(long size, Map<String, String> metadata) {}

    /** 上传文件；metadata 作为用户自定义元数据保存（键小写）。覆盖同名对象。 */
    void put(String key, File file, Map<String, String> metadata);

    void put(String key, byte[] data, Map<String, String> metadata);

    /** 对象不存在返回 null。 */
    Stat stat(String key);

    byte[] get(String key);

    void delete(String key);

    /** 列出以 prefix 开头的全部对象键，字典序。 */
    List<String> list(String prefix);
}
