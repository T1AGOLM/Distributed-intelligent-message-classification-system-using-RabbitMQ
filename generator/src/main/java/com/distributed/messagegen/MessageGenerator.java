package com.distributed.messagegen;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Creates the messages published by the generator.
 *
 * <p>The two types are produced alternately so both queues receive roughly the
 * same traffic. Content is randomly picked from curated pools that cover every
 * class the consumers will later predict:</p>
 *
 * <ul>
 *   <li>reviews: positive, negative and neutral sentiments;</li>
 *   <li>support requests: access, performance and connection problems.</li>
 * </ul>
 */
public final class MessageGenerator {

    private static final List<String> REVIEWS = List.of(
            "Absolutely loved this product, best purchase I made all year!",
            "The battery life is amazing and the build quality feels premium.",
            "Decent product for the price, nothing spectacular but it works.",
            "Arrived on time and works exactly as described in the manual.",
            "Terrible quality — the screen broke after two days of normal use.",
            "Very disappointed, the item stopped working within a week.",
            "The worst customer experience I have ever had with an online store.",
            "It is okay, does the basic job but the design could be better.",
            "Shipping was slow and the packaging was damaged, not impressed.",
            "Excellent value for money, I would definitely recommend it to friends."
    );

    private static final List<String> SUPPORT_REQUESTS = List.of(
            "I cannot log into my account, it always says invalid credentials.",
            "The login page keeps rejecting my password even after a reset.",
            "Getting an access denied error when trying to open the dashboard.",
            "The application takes over a minute to start and the page loads very slowly.",
            "Performance is terrible today, every screen takes ages to respond.",
            "The system froze completely and CPU usage is at 100 percent.",
            "I cannot connect to the VPN, the connection drops as soon as it is established.",
            "Connection lost every few minutes, the network icon shows no internet access.",
            "Wi-Fi keeps disconnecting and I cannot reach the server at all.",
            "The app times out with a connection timeout error when submitting forms."
    );

    private int cursor = 0;

    /**
     * Returns the next message. Types alternate (review, support, review, ...)
     * so both routing keys see the same message volume.
     */
    public synchronized Message next() {
        MessageType type = (cursor++ % 2 == 0) ? MessageType.PRODUCT_REVIEW : MessageType.SUPPORT_REQUEST;
        List<String> pool = (type == MessageType.PRODUCT_REVIEW) ? REVIEWS : SUPPORT_REQUESTS;
        String content = pool.get(ThreadLocalRandom.current().nextInt(pool.size()));
        return Message.of(type, content);
    }
}
