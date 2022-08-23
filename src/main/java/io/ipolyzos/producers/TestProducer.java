package io.ipolyzos.producers;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class TestProducer {
    private static final Logger logger
            = LoggerFactory.getLogger(TestProducer.class);

    public static void main(String[] args) throws IOException {
        logger.info("Creating Pulsar Client ...");

        PulsarClient pulsarClient = PulsarClient
                .builder()
                .serviceUrl("pulsar://localhost:6650")
                .authentication(new AuthenticationToken(""))
                .build();

        logger.info("Creating User Producer ...");
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .producerName("test-producer")
                .topic("test-topic")
                .blockIfQueueFull(true)
                .create();

        AtomicInteger counter = new AtomicInteger();
        for (int i = 0; i < 1000000; i++ ) {
            producer
                    .newMessage()
                    .key(String.valueOf(i% 100))
                    .value("msg-" + i)
                    .eventTime(System.currentTimeMillis())
                    .sendAsync()
                    .whenComplete(callback(counter));
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Sent '{}' messages.", counter.get());
            logger.info("Closing Resources...");
            try {
                producer.close();
                pulsarClient.close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        }));
    }

    private static BiConsumer<MessageId, Throwable> callback(AtomicInteger counter) {
        return (id, exception) -> {
            if (exception != null) {
                logger.error("❌ Failed message: {}", exception.getMessage());
            } else {
                logger.info("✅ Acked message {} - Total {}", id, counter.getAndIncrement());
            }
        };
    }
}
