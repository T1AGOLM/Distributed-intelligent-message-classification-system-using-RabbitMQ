package com.distributed.messagegen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.AMQP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Message Generator — main entry point (assignment task 4).
 *
 * <p>Publishes JSON messages to the RabbitMQ Topic Exchange at a constant rate
 * of {@code RABBITMQ_MESSAGE_RATE} messages per second (default 5, i.e. one
 * message every 200 ms). Message types alternate between product reviews
 * (routing key {@code review.product}) and support requests
 * (routing key {@code support.request}).</p>
 *
 * <p>The connection is (re)established with a retry loop so the service
 * survives broker restarts and works with {@code docker compose up} regardless
 * of container start-up order.</p>
 */
public final class GeneratorApp {

    private static final Logger log = LoggerFactory.getLogger(GeneratorApp.class);
    private static final long RECONNECT_DELAY_MS = 5_000;

    private GeneratorApp() {
    }

    public static void main(String[] args) throws InterruptedException {
        GeneratorConfig config = GeneratorConfig.fromEnv();
        MessageGenerator generator = new MessageGenerator();
        ObjectMapper mapper = new ObjectMapper();

        log.info("Message Generator starting — {}:{} exchange={} rate={} msg/s",
                config.host(), config.port(), config.exchange(), config.messagesPerSecond());

        // Retry forever: the broker may start after this container.
        while (!Thread.currentThread().isInterrupted()) {
            try {
                publishLoop(config, generator, mapper);
            } catch (Exception e) {
                log.error("Connection/publishing failed: {} — retrying in {} ms",
                        e.getMessage(), RECONNECT_DELAY_MS);
                Thread.sleep(RECONNECT_DELAY_MS);
            }
        }
    }

    private static void publishLoop(GeneratorConfig config, MessageGenerator generator,
                                    ObjectMapper mapper) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.host());
        factory.setPort(config.port());
        factory.setUsername(config.user());
        factory.setPassword(config.password());
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(5_000);

        try (Connection connection = factory.newConnection("message-generator");
             Channel channel = connection.createChannel()) {

            // Idempotent: matches the topology loaded from rabbitmq/definitions.json.
            channel.exchangeDeclare(config.exchange(), "topic", true);
            log.info("Connected to RabbitMQ at {}:{}", config.host(), config.port());

            final long intervalNanos = TimeUnit.SECONDS.toNanos(1) / config.messagesPerSecond();
            long nextDeadline = System.nanoTime();
            long windowStart = nextDeadline;
            long windowCount = 0;
            long total = 0;

            while (!Thread.currentThread().isInterrupted()) {
                Message message = generator.next();
                String routingKey = (message.type().equals(MessageType.PRODUCT_REVIEW.wireName()))
                        ? config.reviewRoutingKey()
                        : config.supportRoutingKey();

                AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                        .contentType("application/json")
                        .deliveryMode(2) // persistent
                        .build();

                channel.basicPublish(config.exchange(), routingKey, properties,
                        mapper.writeValueAsBytes(message));

                windowCount++;
                total++;
                log.info("[{}] published id={} type={} content=\"{}\"",
                        routingKey, message.id(), message.type(), message.content());

                // Constant rate, drift-free pacing.
                nextDeadline += intervalNanos;
                long sleepNanos = nextDeadline - System.nanoTime();
                if (sleepNanos > 0) {
                    LockSupport.parkNanos(sleepNanos);
                } else {
                    // We fell behind (slow publish); resynchronize the schedule.
                    nextDeadline = System.nanoTime();
                }

                // Throughput report every 10 seconds.
                long now = System.nanoTime();
                if (now - windowStart >= TimeUnit.SECONDS.toNanos(10)) {
                    double rate = windowCount / ((now - windowStart) / 1e9);
                    log.info("→ rate: {} msg/s ({} messages total)", String.format("%.1f", rate), total);
                    windowStart = now;
                    windowCount = 0;
                }
            }
        }
    }
}
