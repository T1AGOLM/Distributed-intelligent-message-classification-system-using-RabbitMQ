package com.distributed.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumer 2 — main entry point (task 7: support request classification with
 * Smile).
 *
 * <p>Consumes from {@code support.queue}, classifies every support request as
 * {@code access_problem}, {@code performance_problem} or
 * {@code connection_problem} with the embedded Smile model
 * ({@link SupportRequestClassifier}) and prints the result to the console.
 * Messages whose {@code type} is not a support request are flagged as a
 * routing error.</p>
 */
public final class ConsumerApp {

    private static final Logger log = LoggerFactory.getLogger(ConsumerApp.class);
    private static final String EXPECTED_TYPE = MessageType.SUPPORT_REQUEST.wireName();
    private static final long RECONNECT_DELAY_MS = 5_000;
    private static final long REPORT_INTERVAL_MS = 10_000;

    private ConsumerApp() {
    }

    public static void main(String[] args) throws InterruptedException {
        ConsumerConfig config = ConsumerConfig.fromEnv();
        ObjectMapper mapper = new ObjectMapper();
        AtomicLong received = new AtomicLong();

        // Train the embedded AI once, before consuming.
        final SupportRequestClassifier classifier;
        try {
            classifier = SupportRequestClassifier.train();
        } catch (Exception e) {
            log.error("Could not train the support classifier: {}", e.getMessage(), e);
            return;
        }

        log.info("Consumer 2 starting — queue={} dlq={} at {}:{}",
                config.queue(), config.dlq(), config.host(), config.port());

        CountDownLatch forever = new CountDownLatch(1);
        while (!forever.await(RECONNECT_DELAY_MS, TimeUnit.MILLISECONDS)) {
            try {
                consumeLoop(config, mapper, received, classifier);
            } catch (Exception e) {
                log.error("Connection/consumption failed: {} — retrying in {} ms",
                        e.getMessage(), RECONNECT_DELAY_MS);
            }
        }
    }

    private static void consumeLoop(ConsumerConfig config, ObjectMapper mapper,
                                    AtomicLong received, SupportRequestClassifier classifier) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.host());
        factory.setPort(config.port());
        factory.setUsername(config.user());
        factory.setPassword(config.password());
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(RECONNECT_DELAY_MS);

        try (Connection connection = factory.newConnection("consumer-2");
             Channel channel = connection.createChannel()) {

            // Idempotent: declares the queue with the exact same arguments used
            // in rabbitmq/definitions.json (DLX args must match or RabbitMQ
            // answers PRECONDITION_FAILED).
            Map<String, Object> args = Map.of(
                    "x-dead-letter-exchange", "messages.dlx",
                    "x-dead-letter-routing-key", config.dlq());
            channel.queueDeclare(config.queue(), true, false, false, args);

            log.info("Connected to RabbitMQ — consuming queue '{}'", config.queue());

            DeliverCallback callback = (tag, delivery) -> {
                try {
                    Message message = mapper.readValue(delivery.getBody(), Message.class);
                    handle(message, config.queue(), received, classifier);
                } catch (Exception e) {
                    log.error("Failed to process message: {}", e.getMessage());
                }
            };

            // autoAck for now — manual ACK + DLQ handling arrives in task 8.
            channel.basicConsume(config.queue(), true, callback, tag -> { });

            // Periodic throughput report; if the broker connection dies for
            // good (recovery failed), exit so the outer loop reconnects.
            long lastCount = 0;
            long lastReport = System.nanoTime();
            while (channel.isOpen() && connection.isOpen()) {
                Thread.sleep(REPORT_INTERVAL_MS);
                long count = received.get();
                double rate = (count - lastCount)
                        / ((System.nanoTime() - lastReport) / 1e9);
                log.info("→ consumed {} messages total ({} msg/s)",
                        count, String.format("%.1f", rate));
                lastCount = count;
                lastReport = System.nanoTime();
            }
        }
    }

    private static void handle(Message message, String queue, AtomicLong received,
                               SupportRequestClassifier classifier) {
        received.incrementAndGet();
        if (!EXPECTED_TYPE.equals(message.type())) {
            log.error("⚠ ROUTING ERROR — queue '{}' received unexpected type '{}': {}",
                    queue, message.type(), message.content());
            return;
        }
        String classification = classifier.classify(message.content());
        log.info("[{}] received id={} type={} classification={} content=\"{}\"",
                queue, message.id(), message.type(), classification, message.content());
    }
}
