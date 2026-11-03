package jp.trade.exchange.archive;

import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.Item;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** MinIO / S3 实现。生产里换成 AWS S3 只需改 endpoint 与凭证。 */
public final class MinioObjectStore implements ObjectStore {
    private final MinioClient client;
    private final String bucket;

    public MinioObjectStore(String endpoint, String accessKey, String secretKey, String bucket) {
        this.client = MinioClient.builder().endpoint(endpoint).credentials(accessKey, secretKey).build();
        this.bucket = bucket;
        try {
            if (!client.bucketExists(BucketExistsArgs.builder().bucket(bucket).build())) {
                client.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
            }
        } catch (Exception e) {
            throw new IllegalStateException("cannot prepare bucket " + bucket, e);
        }
    }

    @Override
    public void put(String key, File file, Map<String, String> metadata) {
        try (InputStream in = new FileInputStream(file)) {
            client.putObject(PutObjectArgs.builder().bucket(bucket).object(key)
                    .stream(in, file.length(), -1).userMetadata(metadata).build());
        } catch (Exception e) {
            throw new IllegalStateException("put " + key + " failed", e);
        }
    }

    @Override
    public void put(String key, byte[] data, Map<String, String> metadata) {
        try (InputStream in = new ByteArrayInputStream(data)) {
            client.putObject(PutObjectArgs.builder().bucket(bucket).object(key)
                    .stream(in, data.length, -1).userMetadata(metadata).build());
        } catch (Exception e) {
            throw new IllegalStateException("put " + key + " failed", e);
        }
    }

    @Override
    public Stat stat(String key) {
        try {
            final StatObjectResponse r = client.statObject(StatObjectArgs.builder().bucket(bucket).object(key).build());
            final Map<String, String> meta = new HashMap<>();
            r.userMetadata().forEach((k, v) -> meta.put(k.toLowerCase(), v));
            return new Stat(r.size(), Collections.unmodifiableMap(meta));
        } catch (ErrorResponseException e) {
            final String code = e.errorResponse().code();
            if ("NoSuchKey".equals(code) || "NoSuchObject".equals(code) || "NotFound".equals(code)) {
                return null;
            }
            throw new IllegalStateException("stat " + key + " failed", e);
        } catch (Exception e) {
            throw new IllegalStateException("stat " + key + " failed", e);
        }
    }

    @Override
    public byte[] get(String key) {
        try (InputStream in = client.getObject(GetObjectArgs.builder().bucket(bucket).object(key).build())) {
            return in.readAllBytes();
        } catch (Exception e) {
            throw new IllegalStateException("get " + key + " failed", e);
        }
    }

    @Override
    public void delete(String key) {
        try {
            client.removeObject(RemoveObjectArgs.builder().bucket(bucket).object(key).build());
        } catch (Exception e) {
            throw new IllegalStateException("delete " + key + " failed", e);
        }
    }

    @Override
    public List<String> list(String prefix) {
        final List<String> keys = new ArrayList<>();
        try {
            for (Result<Item> r : client.listObjects(
                    ListObjectsArgs.builder().bucket(bucket).prefix(prefix).recursive(true).build())) {
                keys.add(r.get().objectName());
            }
        } catch (Exception e) {
            throw new IllegalStateException("list " + prefix + " failed", e);
        }
        Collections.sort(keys);
        return keys;
    }
}
