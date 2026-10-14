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
import java.util.concurrent.locks.LockSupport;

/** Consumer 2 — main entry point (tasks 7–9: Smile support classification with manual ACK, DLQ and slower-than-production processing). */
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

        final SupportRequestClassifier classifier;
        try {
            classifier = SupportRequestClassifier.train();
        } catch (Exception e) {
            log.error("Could not train the support classifier: {}", e.getMessage(), e);
            return;
        }

        log.info("Consumer 2 configured — delay={} ms/msg", config.processingDelayMs());

        log.info("Consumer 2 starting — queue={} dlq={} at {}:{}", config.queue(), config.dlq(), config.host(), config.port());

        CountDownLatch forever = new CountDownLatch(1);
        while (!forever.await(RECONNECT_DELAY_MS, TimeUnit.MILLISECONDS)) {
            try {
                consumeLoop(config, mapper, received, classifier);
            } catch (Exception e) {
                log.error("Connection/consumption failed: {} — retrying in {} ms", e.getMessage(), RECONNECT_DELAY_MS);
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

            Map<String, Object> args = Map.of(
                    "x-dead-letter-exchange", "messages.dlx",
                    "x-dead-letter-routing-key", config.dlq());
            channel.queueDeclare(config.queue(), true, false, false, args);

            log.info("Connected to RabbitMQ — consuming queue '{}'", config.queue());

            DeliverCallback callback = (tag, delivery) -> {
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                Message message;
                try {
                    message = mapper.readValue(delivery.getBody(), Message.class);
                } catch (Exception parseError) {
                    log.error("⚠ PROCESSING ERROR — nacking to DLQ '{}': {}", config.dlq(), parseError.getMessage());
                    channel.basicNack(deliveryTag, false, false);
                    return;
                }
                try {
                    handle(message, config, received, classifier, channel, deliveryTag);
                } catch (Exception processingError) {
                    log.error("⚠ PROCESSING ERROR — nacking to DLQ '{}': {}", config.dlq(), processingError.getMessage());
                    channel.basicNack(deliveryTag, false, false);
                }
            };

            channel.basicConsume(config.queue(), false, callback, tag -> {
            });

            long lastCount = 0;
            long lastReport = System.nanoTime();
            long msgsInWindow = 0;
            while (channel.isOpen() && connection.isOpen()) {
                Thread.sleep(REPORT_INTERVAL_MS);
                long count = received.get();
                double rate = (count - lastCount) / ((System.nanoTime() - lastReport) / 1e9);
                long windowSecs = (System.nanoTime() - lastReport) / 1_000_000_000L;
                double avgProcessingMs = (msgsInWindow > 0) ? (windowSecs * 1_000_000.0 / msgsInWindow) : 0.0;
                long pending = count;
                long queueDepth = count;
                log.info("= CONSUMER 2 REPORT =");
                log.info("  - received {} messages ({} msg/s via channel)", count, String.format("%.2f", rate));
                log.info("  - consumption rate: {} msg/s (actual)", String.format("%.2f", (msgsInWindow > 0) ? msgsInWindow : 0.0));
                log.info("  - average processing time per message: {} ms", String.format("%.2f", avgProcessingMs));
                log.info("  - estimated queue depth: {} messages", pending);
                lastCount = count;
                lastReport = System.nanoTime();
                msgsInWindow = 0;
            }
        }
    }

    private static void handle(Message message, ConsumerConfig config, AtomicLong received,
                               SupportRequestClassifier classifier, Channel channel,
                               long deliveryTag) throws Exception {
        if (!EXPECTED_TYPE.equals(message.type())) {
            throw new IllegalArgumentException("unexpected message type: " + message.type());
        }

        try {
            SupportRequestClassifier.ClassificationResult result =
                    classifier.classify(message.content());

            if (config.processingDelayMs() > 0) {
                long sleepMs = config.processingDelayMs();
                long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(sleepMs);
                while (System.nanoTime() < deadlineNs) {
                    LockSupport.parkNanos(100_000_000L);
                }
            }

            log.info("[{}] received id={} ts={} type={} classification={} confidence={:.3f} content=\"{}\"",
                    config.queue(), message.id(), message.timestamp(), message.type(),
                    result.label(), result.confidence(), message.content());
        } catch (Exception processingError) {
            log.error("⚠ PROCESSING ERROR — nacking to DLQ '{}': {}", config.dlq(), processingError.getMessage());
            channel.basicNack(deliveryTag, false, false);
            return;
        }
        channel.basicAck(deliveryTag, false);
    }
}
