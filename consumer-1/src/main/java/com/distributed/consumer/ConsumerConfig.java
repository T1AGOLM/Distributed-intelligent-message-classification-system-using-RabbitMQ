package com.distributed.consumer;

/**
 * Configuration read from environment variables (see docker-compose.yml).
 *
 * <p>All values have sensible defaults so the service also runs outside
 * Docker, e.g. directly with {@code java -jar app.jar}.</p>
 */
public record ConsumerConfig(
        String host,
        int port,
        String user,
        String password,
        String queue,
        String dlq) {

    /** Builds the configuration from the process environment. */
    public static ConsumerConfig fromEnv() {
        return new ConsumerConfig(
                env("RABBITMQ_HOST", "localhost"),
                intEnv("RABBITMQ_PORT", 5672),
                env("RABBITMQ_USER", "guest"),
                env("RABBITMQ_PASSWORD", "guest"),
                env("RABBITMQ_QUEUE", "review.queue"),
                env("RABBITMQ_DLQ", "review.dlq"));
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }

    private static int intEnv(String key, int fallback) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
