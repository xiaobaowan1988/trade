package jp.trade.exchange;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import jp.trade.exchange.archive.MinioObjectStore;
import jp.trade.exchange.archive.ObjectStore;

/**
 * 整个测试 JVM 共享一个真实 MinIO 进程。需要 minio 可执行文件（PATH 上，或环境变量 MINIO_BIN）。
 * 官方已不再提供二进制下载，获取方式见 exchange/README.md。
 */
final class TestMinio {
    private static final String USER = "testkey";
    private static final String PASSWORD = "testsecret123";
    private static Process process;
    private static String endpoint;

    private TestMinio() {}

    static String binary() {
        final String env = System.getenv("MINIO_BIN");
        if (env != null && new File(env).canExecute()) {
            return env;
        }
        for (String dir : System.getenv("PATH").split(File.pathSeparator)) {
            final File f = new File(dir, "minio");
            if (f.canExecute()) {
                return f.getAbsolutePath();
            }
        }
        return null;
    }

    static boolean available() { return binary() != null; }

    static synchronized ObjectStore store(String bucket) throws Exception {
        if (process == null) {
            final File data = Files.createTempDirectory("minio-data").toFile();
            final int port = freePort();
            final int console = freePort();
            endpoint = "http://127.0.0.1:" + port;
            final ProcessBuilder pb = new ProcessBuilder(binary(), "server", data.getAbsolutePath(),
                    "--address", "127.0.0.1:" + port, "--console-address", "127.0.0.1:" + console);
            pb.environment().put("MINIO_ROOT_USER", USER);
            pb.environment().put("MINIO_ROOT_PASSWORD", PASSWORD);
            pb.redirectErrorStream(true);
            pb.redirectOutput(new File(data, "minio.log"));
            process = pb.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> process.destroyForcibly()));
            final long deadline = System.currentTimeMillis() + 30_000;
            while (!healthy()) {
                if (System.currentTimeMillis() > deadline || !process.isAlive()) {
                    throw new IllegalStateException("minio did not become healthy, see " + new File(data, "minio.log"));
                }
                Thread.sleep(100);
            }
        }
        return new MinioObjectStore(endpoint, USER, PASSWORD, bucket);
    }

    private static boolean healthy() {
        try {
            final HttpURLConnection c = (HttpURLConnection) URI.create(endpoint + "/minio/health/live").toURL().openConnection();
            c.setConnectTimeout(500);
            return c.getResponseCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    private static int freePort() throws Exception {
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    static List<String> keys(ObjectStore s, String prefix) { return s.list(prefix); }
}
