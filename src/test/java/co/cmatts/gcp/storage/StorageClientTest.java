package co.cmatts.gcp.storage;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;
import uk.org.webcompere.systemstubs.properties.SystemProperties;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.nio.file.Paths;

import static co.cmatts.gcp.storage.StorageClient.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@ExtendWith(SystemStubsExtension.class)
public class StorageClientTest {
    private static final String TEST_BUCKET = "mybucket";
    private static final String TEST_CONTENT = "{ \"content\": \"some content\" }";

    @SystemStub
    private static SystemProperties systemProperties;

    @Container
    static final GenericContainer<?> localGCS = new GenericContainer<>("fsouza/fake-gcs-server")
            .withExposedPorts(4443)
            .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
                    "/bin/fake-gcs-server",
                    "-scheme", "http"
            ));

    @BeforeAll
    static void beforeAll() throws Exception {
        String localGCSUrl = "http://" + localGCS.getHost() + ":" + localGCS.getFirstMappedPort();
        configureLocalGCSUrl(localGCSUrl);

        systemProperties
                .set("local.gcs.url", localGCSUrl)
                .set("local.project", localGCSUrl);

        resetStorageClient();
        createBucket(TEST_BUCKET);
    }

    @Test
    void shouldCheckBucketExist() {
        assertThat(bucketExists(TEST_BUCKET)).isTrue();
    }

    @Test
    void shouldWriteFileToBucket() throws Exception {
        String bucket = "mybucket";
        String key = "/test/resources/MyFile.txt";
        Path localFile = Paths.get(this.getClass().getClassLoader().getResource("MyFile.txt").toURI());
        writeToBucket(bucket, key, localFile);

        assertThat(fileExists(bucket, key)).isTrue();
    }

    @Test
    void shouldWriteStringToBucket() throws Exception {
        String bucket = "mybucket";
        String key = "/test/resources/MyContent.txt";
        writeToBucket(bucket, key, TEST_CONTENT);

        assertThat(fileExists(bucket, key)).isTrue();
    }

    @Test
    void shouldReadFromBucket() throws Exception {
        String bucket = "mybucket";
        String key = "/test/resources/readFile.txt";
        writeToBucket(bucket, key, TEST_CONTENT);

        try(InputStream storageInputStream = readFromBucket(bucket, key)) {
            String actualFileContent = new String(storageInputStream.readAllBytes(), UTF_8);
            assertThat(actualFileContent).isEqualTo(TEST_CONTENT);
        }
    }

    private static void configureLocalGCSUrl(String localGCSUrl) throws Exception {
        String modifyExternalUrlRequestUri = localGCSUrl + "/_internal/config";
        String updateExternalUrlJson = "{"
                + "\"externalUrl\": \"" + localGCSUrl + "\""
                + "}";

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(modifyExternalUrlRequestUri))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(updateExternalUrlJson))
                .build();
        HttpResponse<Void> response = HttpClient.newBuilder().build()
                .send(req, HttpResponse.BodyHandlers.discarding());

        if (response.statusCode() != 200) {
            throw new RuntimeException(
                    "error updating local-gcs-server with external url, response status code " + response.statusCode());
        }
    }
}