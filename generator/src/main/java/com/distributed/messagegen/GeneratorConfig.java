package com.distributed.messagegen;

/**
 * Configuration read from environment variables (see docker-compose.yml).
 *
 * <p>All values have sensible defaults so the service also runs outside
 * Docker, e.g. directly with {@code java -jar app.jar}.</p>
 */
public record GeneratorConfig(
        String host,
        int port,
        String user,
        String password,
        String exchange,
        String reviewRoutingKey,
        String supportRoutingKey,
        int messagesPerSecond) {

    /** Builds the configuration from the process environment. */
    public static GeneratorConfig fromEnv() {
        return new GeneratorConfig(
                env("RABBITMQ_HOST", "localhost"),
                intEnv("RABBITMQ_PORT", 5672),
                env("RABBITMQ_USER", "guest"),
                env("RABBITMQ_PASSWORD", "guest"),
                env("RABBITMQ_EXCHANGE", "messages.topic"),
                env("RABBITMQ_ROUTING_KEY_REVIEW", "review.product"),
                env("RABBITMQ_ROUTING_KEY_SUPPORT", "support.request"),
                Math.max(1, intEnv("RABBITMQ_MESSAGE_RATE", 5)));
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
