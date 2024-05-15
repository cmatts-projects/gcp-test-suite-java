package co.cmatts.gcp.storage;

import com.google.cloud.NoCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static java.util.Objects.nonNull;

public class StorageClient {
    private static Storage client;

    public static Storage getStorageClient() {
        if (client != null) {
            return client;
        }

        StorageOptions.Builder builder = StorageOptions.newBuilder();

        if (nonNull(System.getProperty("local.gcs.url"))) {
            builder = builder
                    .setHost(System.getProperty("local.gcs.url"))
                    .setProjectId("test-project")
                    .setCredentials(NoCredentials.getInstance());
        }

        client = builder.build()
                .getService();
        return client;
    }

    public static void resetStorageClient() {
        client = null;
    }

    public static boolean bucketExists(String bucketName) {
        Bucket bucket = getStorageClient().get(bucketName);
        return bucket.exists();
    }

    public static void createBucket(String bucket) {
        getStorageClient().create(BucketInfo.newBuilder(bucket).build());
    }

    public static void writeToBucket(String bucket, String key, Path path) throws IOException {
        try (WriteChannel channel = getStorageClient().writer(
                BlobInfo.newBuilder(bucket, key)
                        .setContentType("text/plain")
                        .build());
             SeekableByteChannel inputChannel = Files.newByteChannel(path, StandardOpenOption.READ)) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(1000);
            while (inputChannel.read(byteBuffer) > 0) {
                byteBuffer.flip();
                channel.write(ByteBuffer.wrap(byteBuffer.array()));
                byteBuffer.clear();
            }
        }
    }

    public static void writeToBucket(String bucket, String key, String content) throws IOException {
        try (WriteChannel channel = getStorageClient().writer(
                BlobInfo.newBuilder(bucket, key)
                        .setContentType("text/plain")
                        .build())) {
            channel.write(ByteBuffer.wrap(content.getBytes()));
        }
    }

    public static boolean fileExists(String bucketName, String key) {
        Blob blob = getStorageClient().get(bucketName, key);
        return nonNull(blob) && blob.exists();
    }

    public static InputStream readFromBucket(String bucketName, String key) {
        ReadChannel reader = getStorageClient().reader(bucketName, key);
        return Channels.newInputStream(reader);
    }
}
