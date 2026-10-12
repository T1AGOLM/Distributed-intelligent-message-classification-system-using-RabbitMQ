package com.distributed.consumer;

/**
 * The two message types produced by the generator (same wire format as the
 * generator service — see generator/src/.../Message.java).
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
