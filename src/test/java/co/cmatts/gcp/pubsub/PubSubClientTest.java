package co.cmatts.gcp.pubsub;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;
import uk.org.webcompere.systemstubs.properties.SystemProperties;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@ExtendWith(SystemStubsExtension.class)
class PubSubClientTest {
    private static final String TEST_TOPIC = "myTopic";
    private static final String TEST_SUBSCRIPTION = "mySubscription";
    private static final String TEST_MESSAGE = "A test message";

    @SystemStub
    private static SystemProperties systemProperties;

    @Container
    private static final PubSubEmulatorContainer localPubSub = new PubSubEmulatorContainer("gcr.io/google.com/cloudsdktool/google-cloud-cli:441.0.0-emulators");

    @BeforeAll
    static void beforeAll() throws Exception {
        systemProperties
                .set("local.pubsub.url", localPubSub.getEmulatorEndpoint())
                .set("local.project", "test-project");

        PubSubClient.createTopic(TEST_TOPIC);
        PubSubClient.createSubscription(TEST_TOPIC, TEST_SUBSCRIPTION);
    }

    @AfterAll
    static void afterAll() {
        PubSubClient.shutdown();
    }

    @BeforeEach
    void purgeQueue() throws Exception {
        PubSubClient.purgeTopic(TEST_SUBSCRIPTION);
    }

    @Test
    void shouldPublishMessageAndReadMessage() throws Exception {
        PubSubClient.publishMessage(TEST_TOPIC, TEST_MESSAGE);

        List<String> receivedMessages = PubSubClient.readMessage(TEST_SUBSCRIPTION);
        assertThat(receivedMessages).hasSize(1);
        assertThat(receivedMessages.get(0)).isEqualTo(TEST_MESSAGE);
    }
}
