package com.distributed.messagegen;

/**
 * The two message types produced by the generator.
 *
 * <p>Each type maps to a routing key on the Topic Exchange (the keys are read
 * from environment variables so they stay in sync with the broker topology).</p>
 */
public enum MessageType {

    /** Free-text product review → routed with key {@code review.product}. */
    PRODUCT_REVIEW("product_review"),

    /** Technical support request → routed with key {@code support.request}. */
    SUPPORT_REQUEST("support_request");

    private final String wireName;

    MessageType(String wireName) {
        this.wireName = wireName;
    }

    /** Value serialized in the JSON {@code type} field. */
    public String wireName() {
        return wireName;
    }
}
