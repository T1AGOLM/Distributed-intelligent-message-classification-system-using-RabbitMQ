package com.distributed.messagegen;

import java.time.Instant;
import java.util.UUID;

/**
 * Message emitted by the generator.
 *
 * <p>Required fields (assignment task 4):</p>
 * <ul>
 *   <li>{@code id}        — unique identifier (UUID);</li>
 *   <li>{@code timestamp} — ISO-8601 generation timestamp;</li>
 *   <li>{@code type}      — {@code product_review} or {@code support_request};</li>
 *   <li>{@code content}   — the free text of the message.</li>
 * </ul>
 *
 * <p>Serialized to JSON before being published to the Topic Exchange.</p>
 */
public record Message(String id, String timestamp, String type, String content) {

    /** Builds a new message with a fresh UUID and the current instant. */
    public static Message of(MessageType type, String content) {
        return new Message(
                UUID.randomUUID().toString(),
                Instant.now().toString(),
                type.wireName(),
                content);
    }
}
