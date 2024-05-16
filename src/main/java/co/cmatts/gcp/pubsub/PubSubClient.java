package co.cmatts.gcp.pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.ClientSettings;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.*;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

public class PubSubClient {

    private static final int MAX_MESSAGES = 1024;
    private static ManagedChannel localChannel;
    private static CredentialsProvider localCredentials;

    public static void createTopic(String topicId) throws IOException {
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create(getTopicAdminSettings())) {
            TopicName topicName = TopicName.of(System.getProperty("local.project"), topicId);
            topicAdminClient.createTopic(topicName);
        }
    }

    public static void createSubscription(String topicId, String subscriptionId) throws IOException {
        SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(getSubscriptionAdminSettings());
        SubscriptionName subscriptionName = SubscriptionName.of(System.getProperty("local.project"), subscriptionId);
        subscriptionAdminClient.createSubscription(
                subscriptionName,
                TopicName.of(System.getProperty("local.project"), topicId),
                PushConfig.getDefaultInstance(),
                10
        );
    }

    private static TopicAdminSettings getTopicAdminSettings() throws IOException {
        TopicAdminSettings.Builder builder = TopicAdminSettings.newBuilder();
        builder = (TopicAdminSettings.Builder) configureClientSettings(builder);

        return builder.build();
    }

    private static SubscriptionAdminSettings getSubscriptionAdminSettings() throws IOException {
        SubscriptionAdminSettings.Builder builder = SubscriptionAdminSettings.newBuilder();
        builder = (SubscriptionAdminSettings.Builder) configureClientSettings(builder);

        return builder.build();
    }

    private static TransportChannelProvider getLocalTransportChannelProvider() {
        TransportChannelProvider channelProvider = FixedTransportChannelProvider.create(
                GrpcTransportChannel.create(getLocalChannel())
        );
        return channelProvider;
    }

    private static ManagedChannel getLocalChannel() {
        if (nonNull(localChannel)) {
            return localChannel;
        }
        String target = System.getProperty("local.pubsub.url");
        if (nonNull(target)) {
            localChannel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        } else {
            throw new UnsupportedOperationException("Not implemented");
        }

        return localChannel;
    }

    private static CredentialsProvider getLocalCredentialsProvider() {
        if (nonNull(localCredentials)) {
            return localCredentials;
        }
        localCredentials = NoCredentialsProvider.create();

        return localCredentials;
    }

    private static ClientSettings.Builder configureClientSettings(ClientSettings.Builder builder) {
        String target = System.getProperty("local.pubsub.url");
        if (nonNull(target)) {
            builder = builder
                    .setTransportChannelProvider(getLocalTransportChannelProvider())
                    .setCredentialsProvider(getLocalCredentialsProvider());
        }
        return builder;
    }

    private static Publisher.Builder configurePublisherSettings(Publisher.Builder builder) {
        String target = System.getProperty("local.pubsub.url");
        if (nonNull(target)) {
            builder = builder
                    .setChannelProvider(getLocalTransportChannelProvider())
                    .setCredentialsProvider(getLocalCredentialsProvider());
        }
        return builder;
    }

    private static SubscriberStubSettings.Builder configureSubscriberStubSettings(SubscriberStubSettings.Builder builder) {
        String target = System.getProperty("local.pubsub.url");
        if (nonNull(target)) {
            builder = builder
                    .setTransportChannelProvider(getLocalTransportChannelProvider())
                    .setCredentialsProvider(getLocalCredentialsProvider());
        }
        return builder;
    }

    public static void publishMessage(String topicId, String message) throws IOException {
        Publisher.Builder builder = Publisher.newBuilder(TopicName.of(System.getProperty("local.project"), topicId));
        builder = configurePublisherSettings(builder);
        Publisher publisher = builder.build();

        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(message))
                .build();
        publisher.publish(pubsubMessage);
    }

    public static List<String> readMessage(String subscriptionId) throws IOException {
        SubscriberStubSettings.Builder builder = SubscriberStubSettings.newBuilder();
        builder = configureSubscriberStubSettings(builder);
        SubscriberStubSettings subscriberStubSettings = builder.build();

        try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
            PullRequest pullRequest = PullRequest
                    .newBuilder()
                    .setMaxMessages(MAX_MESSAGES)
                    .setSubscription(ProjectSubscriptionName.format(System.getProperty("local.project"), subscriptionId))
                    .build();
            PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);

            return pullResponse.getReceivedMessagesList()
                    .stream()
                    .map(m -> m.getMessage().getData().toStringUtf8())
                    .collect(Collectors.toList());
        }
    }

    public static void purgeTopic(String subscriptionId) throws ExecutionException, InterruptedException, IOException {
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(getSubscriptionAdminSettings())) {
            SeekRequest request = SeekRequest.newBuilder()
                    .setSubscription(SubscriptionName.of(System.getProperty("local.project"), subscriptionId).toString())
                    .setTime(Timestamp.newBuilder().setSeconds(Integer.MAX_VALUE))
                    .build();
            ApiFuture<SeekResponse> future = subscriptionAdminClient.seekCallable().futureCall(request);
            future.get();
        }
    }

    public static void shutdown() {
        if (nonNull(localChannel)) {
            localChannel.shutdown();
            localChannel = null;
        }
    }
}
